# Homelab — Kubernetes & Observability on Talos

A single-host homelab: **Proxmox → 3× Talos VMs → Kubernetes (Cilium) → Flux GitOps → full observability stack**, accessible over Tailscale. Everything is declarative and in git; after bootstrap, changes are `git push` → Flux syncs.

This README is a **manual rebuild runbook** — you can re-create the whole thing from scratch following it, no AI required. The detailed step-by-step plans live in [`docs/superpowers/plans/`](docs/superpowers/plans/).

---

## 1. Architecture

```
                 Beelink GTR7 (Proxmox VE 9.1.1)  192.168.10.100
                 ┌───────────────────────────────────────────────┐
                 │  talos-cp-01 .101  (6GB/4cpu)  control-plane    │
                 │  talos-w-01  .102  (8GB/4cpu)  worker           │
                 │  talos-w-02  .103  (8GB/4cpu)  worker           │
                 └───────────────────────────────────────────────┘
   CNI: Cilium 1.17 (eBPF, kube-proxy replaced) + Hubble
   GitOps: Flux v2  ── watches main ──►  kubernetes/
   Secrets: SOPS + age
   Access: Cilium LoadBalancer (LB-IPAM + L2) on the LAN —
           Grafana http://192.168.10.240, Hubble http://192.168.10.241
           (reachable off-LAN via the Proxmox Tailscale subnet router)

   Observability:
     Grafana ── Prometheus (metrics) ── node-exporter / kube-state-metrics
            └── Loki (logs)        ── Vector (DaemonSet)
            └── Tempo (traces)     ── OpenTelemetry Collector
            Alertmanager ──► Telegram bot
```

### Component versions (pinned)
| Component | Version | Notes |
|---|---|---|
| Talos | v1.13.5 | match your installed `talosctl` |
| Kubernetes | v1.34.1 | |
| Cilium | 1.17.4 | inline manifest, `kubeProxyReplacement=true`, KubePrism :7445 |
| cert-manager | v1.20.3 | |
| kube-prometheus-stack | 87.x | |
| Loki | 6.x | SingleBinary, filesystem |
| Vector | 0.56.x | Agent DaemonSet |
| Tempo | 1.x | single binary; query API on **:3200** |
| OpenTelemetry Collector | 0.159.x | `-contrib` image |
| Tailscale operator | 1.x | |
| local-path-provisioner | 0.0.28 | default StorageClass |

> Versions move fast — verify with `helm search repo <chart> --versions` and the Talos releases page before pinning. cert-manager / kube-prometheus-stack / otel especially drift.

---

## 2. Prerequisites

**Hardware:** Beelink GTR7 (AMD 7840HS, 16 threads), 32 GB RAM (**~27.13 GiB usable** — iGPU reserves ~5 GB), 512 GB SSD. Proxmox VE 9.1.1 + Tailscale already installed on the host.

**Network:** flat LAN `192.168.10.0/24`, gateway `.1`. Proxmox host `.100`, your workstation on the same subnet, VM static IPs `.101/.102/.103`.

**CLIs on your workstation** (installed to `~/.local/bin`):
```
terraform  talhelper  talosctl  kubectl  helm  flux  age  sops
```
Install from official releases (Talos/Cilium/Flux/sops/age GitHub releases, terraform from releases.hashicorp.com, talhelper from budimanjojo/talhelper).

**Accounts/tokens you must create yourself (kept out of git):**
- Proxmox API token (`terraform@pve!provider`)
- GitHub PAT (`repo` scope) for Flux bootstrap
- Tailscale OAuth client (Trust credentials page) + ACL tag setup
- Telegram bot (via @BotFather) + your chat id

---

## 3. Repository layout

```
infrastructure/
  terraform/proxmox/      # VM provisioning (bpg/proxmox). terraform.tfvars is gitignored.
  talos/
    talconfig.yaml        # talhelper cluster def (static IPs, CNI=none, kube-proxy off)
    patches/              # all-nodes.yaml (proxy off), controlplane.yaml (Cilium inline manifest)
    talsecret.sops.yaml   # SOPS-encrypted Talos secrets
    clusterconfig/        # gitignored — generated machine configs
kubernetes/
  flux/
    bootstrap/            # Flux install + flux-system/platform/monitoring Kustomizations
    repositories/helm/    # HelmRepository sources
  platform/               # storage, cert-manager, tailscale   (Flux Kustomization "platform")
  monitoring/             # kube-prometheus-stack, loki, vector, tempo, otel-collector ("monitoring")
docs/superpowers/
  specs/                  # design spec
  plans/                  # 2026-06-27-homelab-phase1.md, 2026-06-25-homelab-phase2-5.md
.sops.yaml                # SOPS creation rules (public key) — safe to commit
homelab.agekey            # gitignored, NEVER commit — BACK THIS UP
```

---

## 4. Rebuild procedure

> Full detail + exact YAML is in the two plan docs. This is the spine.

### Phase 0 — Secrets foundation (do first; everything depends on it)
```bash
# age keypair (BACK UP homelab.agekey securely; gitignored via *.agekey)
age-keygen -o homelab.agekey
# .sops.yaml: put the PUBLIC key (age1...) under creation_rules (see the file)
export SOPS_AGE_KEY_FILE="$PWD/homelab.agekey"
```

### Phase 1 — Provision VMs + bootstrap Talos  (`docs/.../phase1.md`)
1. **Proxmox API token** (on the host):
   ```
   pveum user add terraform@pve
   pveum aclmod / -user terraform@pve -role Administrator
   pveum user token add terraform@pve provider --privsep=0
   ```
2. **Talos Factory schematic** (adds qemu-guest-agent so Proxmox sees VM IPs):
   ```
   curl -X POST --data-binary @- https://factory.talos.dev/schematics <<'EOF'
   customization:
     systemExtensions:
       officialExtensions:
         - siderolabs/qemu-guest-agent
   EOF
   ```
3. **Terraform** — fill `infrastructure/terraform/proxmox/terraform.tfvars` (endpoint, token, node, datastores, schematic id), then:
   ```
   cd infrastructure/terraform/proxmox && terraform init && terraform apply
   terraform output maintenance_ips      # the DHCP IPs the VMs booted with
   ```
4. **Talos secrets + config**:
   ```
   cd ../../talos
   talhelper gensecret > talsecret.sops.yaml
   sops --encrypt --in-place talsecret.sops.yaml   # run from repo root so .sops.yaml path_regex matches
   talhelper genconfig                              # renders clusterconfig/ (gitignored)
   ```
5. **Apply + bootstrap** (use the *maintenance* IPs for the insecure apply; nodes reboot to static .101/.102/.103):
   ```
   talosctl apply-config --insecure -n <cp-maint>  -f clusterconfig/homelab-talos-cp-01.yaml
   talosctl apply-config --insecure -n <w1-maint>  -f clusterconfig/homelab-talos-w-01.yaml
   talosctl apply-config --insecure -n <w2-maint>  -f clusterconfig/homelab-talos-w-02.yaml
   talosctl config merge clusterconfig/talosconfig
   talosctl config endpoint 192.168.10.101 && talosctl config node 192.168.10.101
   talosctl -n 192.168.10.101 bootstrap            # ONCE, control plane only
   talosctl -n 192.168.10.101 kubeconfig ~/.kube/config
   kubectl get nodes -o wide                        # all Ready; Cilium install Job completes
   ```

### Phase 2 — Flux + platform + observability  (`docs/.../phase2-5.md`, start at Task 3)
6. **Bootstrap Flux** (needs `GITHUB_TOKEN` PAT):
   ```
   flux bootstrap github --owner=<you> --repository=homelab --branch=main \
     --path=kubernetes/flux/bootstrap --personal --token-auth
   git pull   # pull the files Flux pushed
   ```
7. **SOPS decryption for Flux** (one-time secret, not committed):
   ```
   kubectl create secret generic sops-age -n flux-system --from-file=age.agekey=homelab.agekey
   ```
   The `flux-system`/`platform`/`monitoring` Kustomizations already carry the `decryption: { provider: sops, secretRef: sops-age }` block.
8. **Everything else is declarative** — Flux applies `kubernetes/platform` then `kubernetes/monitoring` in dependency order:
   `flux-system → platform (storage → cert-manager → tailscale) → monitoring (kube-prometheus-stack → loki/tempo → vector/otel)`.
   For SOPS-encrypted app secrets (Tailscale OAuth, Telegram), recreate them with
   `kubectl create secret ... --dry-run=client -o yaml | sops --encrypt /dev/stdin > <path>.sops.yaml`.

### Phase 3 — Access + alerting
9. **Remote access — Cilium LoadBalancer (NOT the Tailscale operator).** The
   Tailscale operator's ingress proxy needs kernel iptables-NAT, which Talos
   + Cilium (kube-proxy-replacement) does not provide, and userspace mode
   rejects `TS_DEST_IP` — so operator-based `*.ts.net` exposure does not work
   here (see Gotchas). Instead, expose via Cilium LB-IPAM + L2 on the LAN
   (`kubernetes/platform/cilium/`): a `CiliumLoadBalancerIPPool` (`.240-.245`),
   a `CiliumL2AnnouncementPolicy` (NIC `ens18`), a Lease RBAC Role, and
   `type: LoadBalancer` Services for Grafana (`.240`) and Hubble (`.241`).
   Requires `enable-l2-announcements=true` in Cilium (set in the Talos
   cilium-install patch). The Proxmox host's Tailscale **subnet router**
   (192.168.10.0/24) makes these LAN IPs reachable from any Tailscale device
   that runs `tailscale up --accept-routes`.
10. **Telegram** — @BotFather bot token + your chat id (`https://api.telegram.org/bot<TOKEN>/getUpdates`).

---

## 5. Gotchas (the things that cost time — read before rebuilding)

- **Talos enforces PodSecurity `baseline`** on all non-`kube-system` namespaces. Any namespace running hostPath/hostNetwork/hostPort pods must be labeled privileged, or pods/PVC provisioning are rejected:
  ```yaml
  pod-security.kubernetes.io/enforce: privileged   # (+ audit, warn)
  ```
  Applied to `local-path-storage` (helper pods), `monitoring` (node-exporter, Vector), `tailscale` (proxies).
- **local-path ignores `fsGroup`** (it's hostPath-backed). A non-root pod mounting its data dir via **subPath** crashes with "permission denied" (kubelet makes the subPath dir root:root). Fix = a root `chown` initContainer. Prometheus needed one; Loki/Tempo did not (they mount the whole 0777 PV).
- **Control-plane RAM:** 4 GB is too small once the observability load (Prometheus watches/scrapes the apiserver) kicks in — it OOM'd. Use **6 GB** for the CP.
- **Set resource requests/limits on every workload.** Two 8 GB workers running a 30-day Prometheus + Loki + Tempo will OOM a node without limits. Prometheus also needs `retentionSize` as a hard cap.
- **terraform stalls on a dead guest agent:** if a VM is unhealthy, `terraform plan/apply` hangs querying its qemu-agent IP. Emergency changes (e.g. RAM bump) can be done via the Proxmox API directly, then reconcile state with `terraform apply -refresh-only`.
- **talhelper 3.x + Talos 1.13 render multi-doc machine configs** → use **strategic-merge** patches, NOT JSON6902 (`op/path/value`).
- **Vector chart** runs Helm `tpl()` over `customConfig`, mangling Vector's VRL `{{ field }}` templates → use a pre-rendered ConfigMap via `existingConfigMaps`.
- **Tempo query API is on :3200** (not :3100; that's Loki). Grafana's Tempo datasource URL uses :3200.
- **cert-manager `servicemonitor.enabled` must be false** until kube-prometheus-stack installs the ServiceMonitor CRD, or the HelmRelease fails.
- **Tailscale operator ingress does NOT work on Talos+Cilium.** The operator's expose proxy runs in kernel mode and needs iptables-NAT (DNAT); Talos with Cilium kube-proxy-replacement doesn't load the nat modules, so the proxy registers + answers tailnet ping but TCP times out (`nat: Table does not exist`). Userspace mode is incompatible (`TS_DEST_IP is not supported with TS_USERSPACE`). → We expose via **Cilium LoadBalancer (LB-IPAM + L2 announcements)** on the LAN instead, reached remotely through the Proxmox Tailscale subnet router. (If you ever DO want the operator: still note it mints a self-key tagged `tag:k8s-operator`, so that tag must be **self-owned** in `tagOwners` and the OAuth client — under **Trust credentials** — needs **Auth Keys: Write** + the tag.)
- **Cilium L2 announcements need Lease RBAC + the enable flag.** Enabling `enable-l2-announcements` alone isn't enough if Cilium was installed without it: the cilium ServiceAccount lacks `coordination.k8s.io/leases` perms → "cannot get resource leases ... forbidden" and LB IPs never get ARP-announced. Add a Lease Role/RoleBinding (see `kubernetes/platform/cilium/l2-leases-rbac.yaml`), or install Cilium with `l2announcements.enabled=true` so the chart adds it. Note: ICMP ping to a LoadBalancer VIP isn't answered — test with `curl`, not `ping`.
- **Alertmanager + kube-prometheus-stack:** when you override `alertmanager.config`, Helm *replaces* the `receivers` list but *merges* `route` — so the chart's default `Watchdog → "null"` route can survive while the `null` receiver is dropped → "undefined receiver null". Always define a `null` receiver (and route Watchdog to it to silence it).
- **Secrets:** never commit `homelab.agekey`, `terraform.tfvars`, or `clusterconfig/` (all gitignored). All `*.sops.yaml` must show `ENC[...]`. `.sops.yaml` itself is plaintext config (public key) — that's correct.

---

## 6. Day-2 operations

```bash
# cluster
talosctl -n 192.168.10.101 health
kubectl get nodes -o wide

# GitOps — make a change: edit kubernetes/**, then
git add -A && git commit -m "..." && git push        # Flux applies within the sync interval
flux get kustomizations ; flux get hr -A             # check sync state
flux reconcile kustomization monitoring --with-source   # force a sync

# edit an encrypted secret in place
sops kubernetes/<path>/<name>.sops.yaml              # decrypts in $EDITOR, re-encrypts on save

# access — Cilium LoadBalancer IPs on the LAN (http, not https)
http://192.168.10.240    # Grafana  (admin / secret: kube-prometheus-stack-grafana)
http://192.168.10.241    # Hubble UI
#   - on the LAN: works directly.
#   - remote Tailscale device: enable `tailscale up --accept-routes` so the
#     Proxmox subnet router's 192.168.10.0/24 route is used.
# admin password:
kubectl -n monitoring get secret kube-prometheus-stack-grafana -o jsonpath='{.data.admin-password}' | base64 -d; echo
# or just port-forward:
kubectl -n monitoring port-forward svc/kube-prometheus-stack-grafana 3000:80
```

---

## 7. Known follow-ups (optional, non-blocking)
- Pin `cilium-cli-ci:latest` (in the Talos controlplane patch) to a tagged release.
- Vendor the `local-path-provisioner` chart (currently from the third-party `charts.containeroo.ch`).
- Add `install/upgrade.remediation.retries` to HelmReleases for self-healing.
- Widen the Grafana `traceID` derived-field regex if apps emit `traceId`/`trace_id`.

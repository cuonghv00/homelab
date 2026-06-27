# Homelab Kubernetes & Observability — Design Spec

**Date:** 2026-06-25  
**Status:** Approved  
**Approach:** Incremental Layered Build (GitOps-first, phase-by-phase)

---

## 1. Goals

- Build a scalable homelab on a single physical machine for **learning and experimentation**
- Run real services (observability stack, future workloads) with production-like practices
- Everything as code: infrastructure, cluster config, and Kubernetes manifests all in git
- Accessible from anywhere via Tailscale, zero public port exposure

---

## 2. Hardware & Existing Infrastructure

| Item | Spec |
|---|---|
| Machine | Beelink GTR7 (AMD 7840HS, 16 threads / 8 cores) |
| RAM | 32 GB physical — **27.13 GiB available to Proxmox** (~5 GB reserved by AMD Radeon 780M iGPU) |
| Storage | 512 GB SSD (Proxmox root uses ~94 GiB; ~418 GiB available for VM disks) |
| Hypervisor | Proxmox VE 9.1.1, kernel 6.17.2-1-pve, EFI boot |
| VPN | Tailscale (already installed on Proxmox host) |

---

## 3. Infrastructure Layer

### 3.1 VM Layout

| Node | Role | vCPU | RAM | Disk |
|---|---|---|---|---|
| `talos-cp-01` | Control Plane | 4 | 4 GB | 50 GB |
| `talos-w-01` | Worker 1 | 4 | 8 GB | 150 GB |
| `talos-w-02` | Worker 2 | 4 | 8 GB | 150 GB |
| *Proxmox host* | Hypervisor | — | ~7 GiB reserved | ~418 GiB free |

Total allocated: 20 GB RAM from 27.13 GiB available — leaves ~7 GiB for Proxmox host OS and overhead. Disk: ~350 GB for VMs from ~418 GiB available after Proxmox root (~94 GiB).

**Note:** Available RAM is 27.13 GiB (not 32 GB) because the AMD Radeon 780M iGPU reserves ~5 GB from system RAM. CP reduced to 4 GB (sufficient: etcd + control plane components use ~1.5 GB). Workers at 8 GB each support the full observability stack (~4–5 GB) with buffer.

### 3.2 VM Provisioning: Terraform

Provider: `bpg/proxmox` (maintained, supports cloud-init and Talos ISO upload).

Workflow:
1. Download Talos ISO → upload to Proxmox storage once
2. Terraform references ISO to create VMs
3. Terraform outputs IP addresses → manually copied into `talhelper.yaml` for Talos bootstrap (one-time step)

```
infrastructure/
  terraform/
    proxmox/
      main.tf          # VM definitions
      variables.tf
      outputs.tf       # IP addresses → Talos bootstrap input
```

### 3.3 Networking

Tailscale is already installed on the Proxmox host as a **subnet router** — all cluster VMs are reachable over the Tailscale mesh without any router port-forwarding. No public IP required.

---

## 4. Kubernetes Layer

### 4.1 Distribution: Talos Linux

Talos Linux is an immutable, API-driven OS purpose-built for Kubernetes. There is no SSH; all configuration is declarative YAML applied via `talosctl`.

**talhelper** manages Talos machine configs in a DRY, versioned way. Secrets are encrypted with **age + SOPS** and safe to commit to git.

```
infrastructure/
  talos/
    talhelper.yaml        # cluster definition (single source of truth)
    talsecret.sops.yaml   # encrypted cluster secrets (age + SOPS)
    clusterconfig/        # gitignored — generated machine configs
```

Bootstrap sequence:
```
talhelper genconfig → talosctl apply-config → talosctl bootstrap → kubeconfig
```

### 4.2 CNI: Cilium

Cilium replaces kube-proxy using eBPF. Chosen for:
- **Hubble UI**: real-time visualization of pod-to-pod network flows
- Native `CiliumNetworkPolicy` for advanced network segmentation
- **Cilium Gateway API** as the ingress controller (modern replacement for Ingress)

Cilium is deployed via Talos inline manifests so the cluster has a CNI before Flux is bootstrapped.

### 4.3 Storage: local-path-provisioner

Provides a default `StorageClass` that provisions `hostPath` volumes on the local node disk. Zero replication overhead — appropriate for this setup because:
- Single physical host means node HA is not achievable regardless of replication strategy
- All PVC consumers (observability stack) tolerate data loss on rebuild
- Eliminates ~2x disk overhead and ~1.5 GB RAM overhead of Longhorn

Longhorn can be added later as a dedicated learning exercise for distributed storage concepts.

---

## 5. GitOps Layer: Flux

### 5.1 Repository Structure

```
homelab/
  infrastructure/
    terraform/           # Proxmox VMs
    talos/               # talhelper configs + SOPS secrets
  kubernetes/
    flux/
      bootstrap/         # Flux install manifests
      repositories/      # HelmRepository sources
    platform/
      cilium/
      cert-manager/
      storage/           # local-path-provisioner
      tailscale/         # Tailscale Kubernetes Operator
    monitoring/
      kube-prometheus-stack/
      loki/
      tempo/
      vector/
    apps/
      base/              # raw manifests / Helm values
      production/        # kustomization overlays
  docs/
    superpowers/
      specs/
```

### 5.2 Bootstrap Flow

```
flux bootstrap github → Flux watches repo → applies kubernetes/ directory
```

All subsequent changes: `git push` → Flux syncs automatically. No manual `kubectl apply`.

### 5.3 Dependency Order (dependsOn)

```
Cilium CNI
  └─ cert-manager
       └─ platform (storage, tailscale)
            └─ monitoring stack
                 └─ user workloads
```

### 5.4 Secret Management

**Phase 1 — SOPS + age:**
- Encrypt secrets (API keys, passwords, Talos secrets) with age keypair before committing
- Flux has native SOPS support: decrypts automatically on apply
- age private key stored as a Kubernetes Secret (`flux-system/sops-age`) during bootstrap — one-time manual step before Flux is installed; after that Flux decrypts automatically
- No external runtime dependency

**Phase 2 — Vault OSS + External Secrets Operator (future):**
- Add Vault for dynamic secrets (short-lived DB credentials, etc.)
- External Secrets Operator syncs Vault secrets → Kubernetes Secrets
- Sealed Secrets excluded: cluster-coupled encryption key is a liability when rebuilding homelabs

---

## 6. Observability Stack

### 6.1 Architecture

```
                    ┌─────────────────────────────────┐
                    │         Grafana (UI)             │
                    └────┬──────────┬──────────┬───────┘
                         │          │          │
                    Prometheus    Loki       Tempo
                         │          │          │
                    ┌────┴────┐  ┌──┴───┐  ┌──┴──────────┐
                    │Exporters│  │Vector│  │OTel Collector│
                    │node/kube│  │      │  │              │
                    └─────────┘  └──────┘  └─────────────┘
```

### 6.2 Components

| Component | Role | Deploy method |
|---|---|---|
| kube-prometheus-stack | Prometheus + Alertmanager + Grafana + node-exporter + kube-state-metrics | Helm via Flux |
| Loki (monolithic) | Log aggregation, filesystem storage via PVC | Helm via Flux |
| Vector | DaemonSet log collector: `/var/log/pods/` → Loki | Helm via Flux |
| Tempo | Distributed tracing backend, OTLP ingest | Helm via Flux |
| OpenTelemetry Collector | Trace gateway: batch, retry, route to Tempo | Helm via Flux |

**Loki mode:** monolithic (single binary), local-path PVC — no MinIO required.  
**Vector role:** replaces Promtail/Alloy as log collector. Enriches logs with Kubernetes metadata (namespace, pod, container labels) before forwarding to Loki via HTTP.

### 6.3 Storage (PVCs via local-path)

| Component | PVC size | Retention |
|---|---|---|
| Prometheus | 30 GB | 30 days |
| Loki | 20 GB | 14 days |
| Tempo | 10 GB | 7 days |
| Grafana | 1 GB | — |

Total: ~61 GB across worker nodes.

### 6.4 Alerting

Alertmanager routes alerts to a **Telegram bot**. Configuration:
- Telegram Bot Token + Chat ID stored as SOPS-encrypted secret
- Alert rules from kube-prometheus-stack defaults cover: node down, pod crash-looping, high memory/CPU, PVC near full

---

## 7. Networking & Access

### 7.1 Tailscale Kubernetes Operator

Deployed into the cluster. Exposes internal services as Tailscale hostnames — no Ingress controller or LoadBalancer IP needed for personal access.

| Service | Tailscale hostname |
|---|---|
| Grafana | `grafana.homelab.ts.net` |
| Hubble UI | `hubble.homelab.ts.net` |
| Proxmox UI | existing Tailscale access |

### 7.2 Cilium Gateway API

Used for any service that needs HTTP routing within the cluster (inter-service, not user-facing). Replaces the traditional Ingress resource.

### 7.3 TLS

Tailscale-exposed services use Tailscale's built-in HTTPS certificates — requires **MagicDNS** and **HTTPS** to be enabled in the Tailscale admin console (free, one-time setting). cert-manager is installed for any non-Tailscale TLS needs (internal CA or Let's Encrypt via DNS challenge).

---

## 8. Implementation Phases

| Phase | Deliverable | Key tools |
|---|---|---|
| 1 | Proxmox VMs provisioned, Talos bootstrapped, kubeconfig working | Terraform, talhelper, talosctl |
| 2 | Cilium CNI running, Flux bootstrapped, repo structure in place | Flux, cilium CLI |
| 3 | local-path storage, Tailscale Operator, SOPS secrets wired up | Flux HelmReleases, age, SOPS |
| 4 | Full observability stack deployed and accessible via Tailscale | Helm (kube-prometheus-stack, Loki, Tempo, Vector) |
| 5 | Alertmanager → Telegram, dashboards verified, Hubble UI accessible | Alertmanager config, Grafana dashboards |

Each phase produces a working, usable cluster state before the next phase begins.

---

## 9. Future Extensions (out of scope for initial implementation)

- **Vault OSS + External Secrets Operator** — dynamic secrets management
- **Longhorn** — distributed storage learning exercise
- **Crossplane** — cloud provider abstraction layer
- **Tekton / Gitea** — CI/CD pipeline practice
- **Workloads** — self-hosted apps (Nextcloud, Vaultwarden, etc.)

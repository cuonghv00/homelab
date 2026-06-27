# Homelab Phase 1 — Provision VMs + Bootstrap Talos (with Cilium) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Provision 3 Talos VMs on Proxmox via Terraform (`bpg/proxmox`), then bootstrap a single-control-plane Kubernetes cluster with Cilium CNI (CNI=none + kube-proxy disabled + Cilium inline manifest), producing a working `kubeconfig` and `talosconfig`.

**Architecture:** Terraform downloads a Talos Image Factory ISO (with the `qemu-guest-agent` extension so Proxmox can report VM IPs) and creates 3 VMs that boot into Talos maintenance mode over DHCP. talhelper renders machine configs that assign **static IPs** (101=cp, 102/103=workers), disable the default CNI and kube-proxy, and inject a Cilium install Job as a Talos inline manifest. We apply configs to the maintenance-mode nodes with `--insecure`, bootstrap etcd on the control plane, and fetch the kubeconfig.

**Tech Stack:** Proxmox VE 9.1.1, Terraform + `bpg/proxmox` provider, Talos Linux (Image Factory), talhelper, SOPS + age, Cilium (inline manifest, Hubble enabled), talosctl, kubectl.

## Global Constraints

- Host: Beelink GTR7, Proxmox VE 9.1.1. Dev machine (where talosctl/kubectl/terraform run): `192.168.10.30/24`, same L2 subnet as the cluster.
- Node IPs (static, final): `talos-cp-01` = `192.168.10.101`, `talos-w-01` = `192.168.10.102`, `talos-w-02` = `192.168.10.103`. Gateway `192.168.10.1` (confirm in tfvars). Cluster endpoint: `https://192.168.10.101:6443`.
- VM sizing (from approved spec, 27.13 GiB usable RAM): cp = 4 vCPU / 4 GB / 50 GB; each worker = 4 vCPU / 8 GB / 150 GB. `allowSchedulingOnControlPlanes: false`.
- **Versions pinned to match installed `talosctl v1.13.5`** — verify latest stable before running: Talos `v1.13.5`, Kubernetes `v1.34.1`, Cilium `1.17.4`. (Check https://github.com/siderolabs/talos/releases and `helm/cilium` for current.)
- Auth: dedicated Proxmox API token `terraform@pve!provider`. Talos image: Image Factory ISO + `siderolabs/qemu-guest-agent`. Static IPs declared in Talos machine config.
- **Secrets never committed in plaintext.** age private key (`homelab.agekey`) and `terraform.tfvars` are gitignored. `infrastructure/talos/talsecret.sops.yaml` is SOPS-encrypted before commit. Generated `infrastructure/talos/clusterconfig/` is gitignored.
- This plan supersedes Task 2 of the `phase2-5` plan (Talos/Cilium config). After Phase 1 completes, **resume the phase2-5 plan at Task 3 (Flux bootstrap)** — its Task 1 (skeleton) and Task 2 (Talos+Cilium) are then both done. The age key + `.sops.yaml` created in Phase 1 Task 3 also satisfy phase2-5 Task 5 Steps 1–2.

---

## Task 1: Terraform provider scaffolding + Proxmox API token

**Files:**
- Create: `infrastructure/terraform/proxmox/versions.tf`
- Create: `infrastructure/terraform/proxmox/provider.tf`
- Create: `infrastructure/terraform/proxmox/variables.tf`
- Create: `infrastructure/terraform/proxmox/terraform.tfvars.example`

**Interfaces:**
- Produces: an initialized Terraform working directory with the `bpg/proxmox` provider configured from variables; `terraform.tfvars` (gitignored) holds endpoint + token + network facts consumed by Tasks 2.

**Prerequisites:** `terraform` (or `tofu`) installed. Proxmox web/SSH access to create an API token.

- [ ] **Step 1: Create the Proxmox API token (on the Proxmox host, via SSH or the web shell)**

```bash
# Dedicated user + token for Terraform (privsep=0 so the token inherits the user's perms)
pveum user add terraform@pve
pveum aclmod / -user terraform@pve -role Administrator
pveum user token add terraform@pve provider --privsep=0
```

The last command prints a table containing a `value` (UUID) **once** — copy it. The full token id used by Terraform is `terraform@pve!provider=<that-uuid>`.

- [ ] **Step 2: Write `versions.tf`**

```hcl
# infrastructure/terraform/proxmox/versions.tf
terraform {
  required_version = ">= 1.6.0"
  required_providers {
    proxmox = {
      source  = "bpg/proxmox"
      version = ">= 0.66.0"
    }
  }
}
```

- [ ] **Step 3: Write `provider.tf`**

```hcl
# infrastructure/terraform/proxmox/provider.tf
provider "proxmox" {
  endpoint  = var.proxmox_endpoint   # e.g. https://192.168.10.X:8006/
  api_token = var.proxmox_api_token  # terraform@pve!provider=<uuid>
  insecure  = true                   # Proxmox uses a self-signed cert by default
}
```

- [ ] **Step 4: Write `variables.tf`**

```hcl
# infrastructure/terraform/proxmox/variables.tf
variable "proxmox_endpoint" {
  type        = string
  description = "Proxmox API endpoint, e.g. https://192.168.10.X:8006/"
}

variable "proxmox_api_token" {
  type        = string
  sensitive   = true
  description = "API token: terraform@pve!provider=<uuid>"
}

variable "proxmox_node" {
  type        = string
  description = "Proxmox node name (see: pvesh get /nodes)"
  default     = "pve"
}

variable "vm_datastore" {
  type        = string
  description = "Datastore for VM disks (e.g. local-lvm)"
  default     = "local-lvm"
}

variable "iso_datastore" {
  type        = string
  description = "Datastore that supports ISO images / snippets (e.g. local)"
  default     = "local"
}

variable "network_bridge" {
  type        = string
  description = "Proxmox bridge for VM NICs"
  default     = "vmbr0"
}

variable "talos_schematic_id" {
  type        = string
  description = "Talos Image Factory schematic id (created in Task 2 Step 1)"
}

variable "talos_version" {
  type        = string
  default     = "v1.13.5"
}
```

- [ ] **Step 5: Write `terraform.tfvars.example` (committed; real `terraform.tfvars` is gitignored)**

```hcl
# infrastructure/terraform/proxmox/terraform.tfvars.example
# Copy to terraform.tfvars and fill in. terraform.tfvars is gitignored.
proxmox_endpoint   = "https://192.168.10.X:8006/"
proxmox_api_token  = "terraform@pve!provider=00000000-0000-0000-0000-000000000000"
proxmox_node       = "pve"
vm_datastore       = "local-lvm"
iso_datastore      = "local"
network_bridge     = "vmbr0"
talos_schematic_id = "REPLACE_WITH_SCHEMATIC_ID_FROM_TASK2"
talos_version      = "v1.13.5"
```

- [ ] **Step 6: Confirm `.gitignore` already covers tfvars and init**

`.gitignore` (created in phase2-5 Task 1) already ignores `infrastructure/terraform/**/.terraform/`, `terraform.tfstate*`, and `*.tfvars` (with `!*.tfvars.example`). Verify, then init:

```bash
cd infrastructure/terraform/proxmox
terraform init
```

Expected: `Terraform has been successfully initialized!` and `bpg/proxmox` downloaded.

- [ ] **Step 7: Commit**

```bash
git add infrastructure/terraform/proxmox/versions.tf \
        infrastructure/terraform/proxmox/provider.tf \
        infrastructure/terraform/proxmox/variables.tf \
        infrastructure/terraform/proxmox/terraform.tfvars.example
git commit -m "feat(terraform): scaffold bpg/proxmox provider config and variables"
```

---

## Task 2: Talos Factory ISO + VM definitions + apply

**Files:**
- Create: `infrastructure/terraform/proxmox/main.tf`
- Create: `infrastructure/terraform/proxmox/outputs.tf`

**Interfaces:**
- Consumes: variables from Task 1, `terraform.tfvars` filled in.
- Produces: 3 running VMs in Talos maintenance mode; `terraform output` exposes each VM's DHCP (maintenance) IP via the qemu guest agent — consumed by Task 5.

**Prerequisites:** Task 1 complete, `terraform.tfvars` filled with real endpoint/token/node/datastores.

- [ ] **Step 1: Create the Talos Image Factory schematic (qemu-guest-agent)**

```bash
curl -X POST --data-binary @- https://factory.talos.dev/schematics <<'EOF'
customization:
  systemExtensions:
    officialExtensions:
      - siderolabs/qemu-guest-agent
EOF
```

Expected: JSON `{"id":"<64-hex-schematic-id>"}`. Put that id into `terraform.tfvars` as `talos_schematic_id`.

- [ ] **Step 2: Write `main.tf` (ISO download + 3 VMs)**

```hcl
# infrastructure/terraform/proxmox/main.tf
locals {
  iso_url = "https://factory.talos.dev/image/${var.talos_schematic_id}/${var.talos_version}/metal-amd64.iso"

  nodes = {
    "talos-cp-01" = { vmid = 101, cores = 4, memory = 4096,  disk = 50,  ip = "192.168.10.101" }
    "talos-w-01"  = { vmid = 102, cores = 4, memory = 8192,  disk = 150, ip = "192.168.10.102" }
    "talos-w-02"  = { vmid = 103, cores = 4, memory = 8192,  disk = 150, ip = "192.168.10.103" }
  }
}

resource "proxmox_virtual_environment_download_file" "talos_iso" {
  content_type = "iso"
  datastore_id = var.iso_datastore
  node_name    = var.proxmox_node
  file_name    = "talos-${var.talos_version}-${substr(var.talos_schematic_id, 0, 8)}-amd64.iso"
  url          = local.iso_url
  overwrite    = false
}

resource "proxmox_virtual_environment_vm" "talos" {
  for_each = local.nodes

  name      = each.key
  vm_id     = each.value.vmid
  node_name = var.proxmox_node

  # Talos has no cloud-init; boot the ISO into maintenance mode, install to disk.
  agent {
    enabled = true   # qemu-guest-agent extension reports the IP to Proxmox
  }

  cpu {
    cores = each.value.cores
    type  = "host"
  }

  memory {
    dedicated = each.value.memory
  }

  scsi_hardware = "virtio-scsi-single"

  disk {
    datastore_id = var.vm_datastore
    interface    = "scsi0"
    size         = each.value.disk
    file_format  = "raw"
    iothread     = true
    discard      = "on"
  }

  cdrom {
    interface = "ide3"   # pin so boot_order's "ide3" is always correct
    file_id   = proxmox_virtual_environment_download_file.talos_iso.id
  }

  network_device {
    bridge = var.network_bridge
    model  = "virtio"
  }

  operating_system {
    type = "l26"
  }

  # Boot from disk first (after install); empty disk falls through to the ISO (maintenance mode).
  boot_order = ["scsi0", "ide3"]

  # Talos reboots itself on config apply; don't let Terraform fight the agent timeout on first boot.
  timeout_create = 600

  lifecycle {
    ignore_changes = [cdrom] # keep ISO attached; don't churn after install
  }
}
```

- [ ] **Step 3: Write `outputs.tf` (maintenance IPs from the guest agent)**

```hcl
# infrastructure/terraform/proxmox/outputs.tf
output "maintenance_ips" {
  description = "DHCP IP each VM received in maintenance mode (target for talosctl apply-config --insecure)"
  value = {
    for name, vm in proxmox_virtual_environment_vm.talos :
    name => try(
      [for ip in flatten(vm.ipv4_addresses) : ip if ip != "127.0.0.1"][0],
      "pending-agent"
    )
  }
}

output "planned_static_ips" {
  description = "Final static IPs assigned by the Talos machine config"
  value       = { for name, n in local.nodes : name => n.ip }
}
```

- [ ] **Step 4: Plan and apply**

```bash
cd infrastructure/terraform/proxmox
terraform validate          # Expected: Success! The configuration is valid.
terraform plan              # Expected: 1 download_file + 3 vm resources to add
terraform apply             # type yes; takes a few minutes to download ISO + boot VMs
```

- [ ] **Step 5: Verify VMs are up and report maintenance IPs**

```bash
terraform output maintenance_ips
# Expected: each of talos-cp-01/w-01/w-02 shows a 192.168.10.x DHCP IP (not "pending-agent").
# If "pending-agent": wait ~60s for the guest agent, then: terraform refresh && terraform output maintenance_ips
```

Confirm maintenance-mode access to one node (insecure, no certs yet):

```bash
talosctl -n <cp-maintenance-ip> disks --insecure
# Expected: a table listing /dev/sda (the 50GB SCSI disk)
```

- [ ] **Step 6: Commit**

```bash
git add infrastructure/terraform/proxmox/main.tf infrastructure/terraform/proxmox/outputs.tf
git commit -m "feat(terraform): download Talos factory ISO and provision 3 VMs"
```

---

## Task 3: age key + SOPS config + talhelper secrets

**Files:**
- Create: `homelab.agekey` (repo root, **gitignored** — never committed)
- Create: `.sops.yaml` (repo root, committed)
- Create: `infrastructure/talos/talsecret.sops.yaml` (SOPS-encrypted, committed)

**Interfaces:**
- Produces: an age keypair; `.sops.yaml` creation rules; an encrypted Talos cluster secret bundle consumed by talhelper in Task 4. The same age key + `.sops.yaml` satisfy phase2-5 Task 5 Steps 1–2.

**Prerequisites:** `age`, `sops`, `talhelper` installed (`brew install age sops siderolabs/tap/talhelper`, or distro equivalents). Verify: `talhelper --version`.

- [ ] **Step 1: Generate the age keypair**

```bash
cd "$(git rev-parse --show-toplevel)"
age-keygen -o homelab.agekey
grep '# public key:' homelab.agekey
```

Copy the public key (starts with `age1...`). Store `homelab.agekey` securely outside the repo too (password manager). It is gitignored by the existing `*.agekey` rule.

- [ ] **Step 2: Write `.sops.yaml`** (replace `<AGE_PUBLIC_KEY>`)

```yaml
# .sops.yaml
creation_rules:
  - path_regex: kubernetes/.*\.sops\.yaml
    encrypted_regex: ^(data|stringData)$
    age: <AGE_PUBLIC_KEY>
  - path_regex: infrastructure/talos/talsecret\.sops\.yaml
    age: <AGE_PUBLIC_KEY>
```

- [ ] **Step 3: Generate and encrypt the Talos secret bundle**

```bash
cd "$(git rev-parse --show-toplevel)"
export SOPS_AGE_KEY_FILE="$(pwd)/homelab.agekey"
mkdir -p infrastructure/talos
talhelper gensecret > infrastructure/talos/talsecret.sops.yaml   # plaintext at this point
# Run sops from the REPO ROOT with the full repo-relative path so the
# .sops.yaml path_regex (infrastructure/talos/talsecret\.sops\.yaml) matches.
# If sops errors "no matching creation rules", the path/regex don't agree — do not proceed.
sops --encrypt --in-place infrastructure/talos/talsecret.sops.yaml
```

- [ ] **Step 4: Verify encryption (must show ENC[...] / no plaintext keys)**

```bash
grep -q 'ENC\[' infrastructure/talos/talsecret.sops.yaml && echo "ENCRYPTED OK" || echo "FAIL: not encrypted"
# Expected: ENCRYPTED OK
grep -RIl 'AGE-SECRET-KEY' infrastructure/ || echo "no plaintext age keys committed — good"
```

- [ ] **Step 5: Commit `.sops.yaml` and the encrypted secret (NOT homelab.agekey)**

```bash
cd "$(git rev-parse --show-toplevel)"
git status --short   # confirm homelab.agekey is NOT listed (gitignored)
git add .sops.yaml infrastructure/talos/talsecret.sops.yaml
git commit -m "feat(sops): add age key rules and SOPS-encrypted Talos secret bundle"
```

---

## Task 4: talhelper config + patches + genconfig

**Files:**
- Create: `infrastructure/talos/talconfig.yaml`
- Create: `infrastructure/talos/patches/all-nodes.yaml`
- Create: `infrastructure/talos/patches/controlplane.yaml`

**Interfaces:**
- Consumes: `talsecret.sops.yaml` (Task 3), node static IPs (Global Constraints).
- Produces: `infrastructure/talos/clusterconfig/` (gitignored) containing `homelab-talos-cp-01.yaml`, `homelab-talos-w-01.yaml`, `homelab-talos-w-02.yaml`, and `talosconfig`. Machine configs declare static IPs, CNI=none, kube-proxy disabled, and a Cilium inline manifest.

**Note:** talhelper's default config filename is `talconfig.yaml` (not `talhelper.yaml`). The earlier phase2-5 plan said `talhelper.yaml`; use `talconfig.yaml` here.

- [ ] **Step 1: Write `talconfig.yaml`**

```yaml
# infrastructure/talos/talconfig.yaml
clusterName: homelab
talosVersion: v1.13.5
kubernetesVersion: v1.34.1
endpoint: https://192.168.10.101:6443
allowSchedulingOnControlPlanes: false

cniConfig:
  name: none

patches:
  - "@./patches/all-nodes.yaml"

nodes:
  - hostname: talos-cp-01
    ipAddress: 192.168.10.101
    installDisk: /dev/sda
    controlPlane: true
    nameservers:
      - 1.1.1.1
      - 8.8.8.8
    networkInterfaces:
      - deviceSelector:
          physical: true
        dhcp: false
        addresses:
          - 192.168.10.101/24
        routes:
          - network: 0.0.0.0/0
            gateway: 192.168.10.1
    patches:
      - "@./patches/controlplane.yaml"

  - hostname: talos-w-01
    ipAddress: 192.168.10.102
    installDisk: /dev/sda
    controlPlane: false
    nameservers:
      - 1.1.1.1
      - 8.8.8.8
    networkInterfaces:
      - deviceSelector:
          physical: true
        dhcp: false
        addresses:
          - 192.168.10.102/24
        routes:
          - network: 0.0.0.0/0
            gateway: 192.168.10.1

  - hostname: talos-w-02
    ipAddress: 192.168.10.103
    installDisk: /dev/sda
    controlPlane: false
    nameservers:
      - 1.1.1.1
      - 8.8.8.8
    networkInterfaces:
      - deviceSelector:
          physical: true
        dhcp: false
        addresses:
          - 192.168.10.103/24
        routes:
          - network: 0.0.0.0/0
            gateway: 192.168.10.1
```

- [ ] **Step 2: Write `patches/all-nodes.yaml` (disable kube-proxy; harden)**

```yaml
# infrastructure/talos/patches/all-nodes.yaml
# Strategic-merge patch (NOT JSON6902): talhelper 3.x + Talos 1.13 render
# multi-document machine configs, which reject RFC6902 op/path/value patches.
machine:
  sysctls:
    net.core.bpf_jit_harden: "1"
cluster:
  proxy:
    disabled: true
```

(CNI is set to `none` via `cniConfig` in talconfig.yaml, so no patch needed for it.)

- [ ] **Step 3: Write `patches/controlplane.yaml` (Cilium inline manifest with Hubble)**

```yaml
# infrastructure/talos/patches/controlplane.yaml
# Strategic-merge patch (NOT JSON6902) — see all-nodes.yaml note.
cluster:
  inlineManifests:
    - name: cilium-install
      contents: |
        ---
        apiVersion: rbac.authorization.k8s.io/v1
        kind: ClusterRoleBinding
        metadata:
          name: cilium-install
        roleRef:
          apiGroup: rbac.authorization.k8s.io
          kind: ClusterRole
          name: cluster-admin
        subjects:
          - kind: ServiceAccount
            name: cilium-install
            namespace: kube-system
        ---
        apiVersion: v1
        kind: ServiceAccount
        metadata:
          name: cilium-install
          namespace: kube-system
        ---
        apiVersion: batch/v1
        kind: Job
        metadata:
          name: cilium-install
          namespace: kube-system
        spec:
          backoffLimit: 10
          template:
            metadata:
              labels:
                app: cilium-install
            spec:
              restartPolicy: OnFailure
              tolerations:
                - operator: Exists
              serviceAccount: cilium-install
              serviceAccountName: cilium-install
              hostNetwork: true
              containers:
                - name: cilium-install
                  image: quay.io/cilium/cilium-cli-ci:latest
                  env:
                    - name: KUBERNETES_SERVICE_HOST
                      valueFrom:
                        fieldRef:
                          apiVersion: v1
                          fieldPath: status.podIP
                    - name: KUBERNETES_SERVICE_PORT
                      value: "6443"
                  command:
                    - cilium
                    - install
                    - --version=1.17.4
                    - --set=ipam.mode=kubernetes
                    - --set=kubeProxyReplacement=true
                    - --set=securityContext.capabilities.ciliumAgent={CHOWN,KILL,NET_ADMIN,NET_RAW,IPC_LOCK,SYS_ADMIN,SYS_RESOURCE,DAC_OVERRIDE,FOWNER,SETGID,SETUID}
                    - --set=securityContext.capabilities.cleanCiliumState={NET_ADMIN,SYS_ADMIN,SYS_RESOURCE}
                    - --set=cgroup.autoMount.enabled=false
                    - --set=cgroup.hostRoot=/sys/fs/cgroup
                    - --set=hubble.enabled=true
                    - --set=hubble.relay.enabled=true
                    - --set=hubble.ui.enabled=true
                    - --set=k8sServiceHost=localhost
                    - --set=k8sServicePort=7445
```

- [ ] **Step 4: Generate machine configs**

```bash
export SOPS_AGE_KEY_FILE="$(git rev-parse --show-toplevel)/homelab.agekey"
cd infrastructure/talos
talhelper genconfig
ls clusterconfig/
# Expected: homelab-talos-cp-01.yaml, homelab-talos-w-01.yaml, homelab-talos-w-02.yaml, talosconfig
```

- [ ] **Step 5: Confirm clusterconfig is gitignored, then commit only the source config**

```bash
cd "$(git rev-parse --show-toplevel)"
git status --short   # clusterconfig/ must NOT appear (gitignored)
git add infrastructure/talos/talconfig.yaml infrastructure/talos/patches/
git commit -m "feat(talos): talhelper config — static IPs, CNI none, Cilium inline manifest"
```

---

## Task 5: Apply config, bootstrap, fetch kubeconfig, verify cluster

**Files:** none (operational task; consumes generated `clusterconfig/`).

**Interfaces:**
- Consumes: maintenance IPs (Task 2 output), generated machine configs + talosconfig (Task 4).
- Produces: a running 3-node cluster; merged `~/.talos/config` and `~/.kube/config`; Cilium + Hubble running.

**Prerequisites:** Tasks 2 and 4 complete. Have the maintenance IPs ready: `cd infrastructure/terraform/proxmox && terraform output maintenance_ips`.

- [ ] **Step 1: Apply config to each node at its maintenance IP (`--insecure`)**

Substitute `<cp-maint>`, `<w1-maint>`, `<w2-maint>` with the Task 2 maintenance IPs.

```bash
cd infrastructure/talos
talosctl apply-config --insecure -n <cp-maint> -f clusterconfig/homelab-talos-cp-01.yaml
talosctl apply-config --insecure -n <w1-maint> -f clusterconfig/homelab-talos-w-01.yaml
talosctl apply-config --insecure -n <w2-maint> -f clusterconfig/homelab-talos-w-02.yaml
```

Each node installs Talos to `/dev/sda`, reboots, and comes up at its **static** IP (101/102/103). Wait ~2–3 minutes.

- [ ] **Step 2: Point talosctl at the static IPs**

```bash
# Use the generated talosconfig (has the cluster CA + admin cert)
talosctl --talosconfig infrastructure/talos/clusterconfig/talosconfig \
  config endpoint 192.168.10.101
talosctl --talosconfig infrastructure/talos/clusterconfig/talosconfig \
  config node 192.168.10.101

# Merge into ~/.talos/config for convenience
talosctl config merge infrastructure/talos/clusterconfig/talosconfig
talosctl config endpoint 192.168.10.101
talosctl config node 192.168.10.101
```

Verify the control plane is reachable at its static IP (TLS now, no --insecure):

```bash
talosctl -n 192.168.10.101 version
# Expected: Server section reports Talos v1.13.5
```

- [ ] **Step 3: Bootstrap etcd (control plane only, run ONCE)**

```bash
talosctl -n 192.168.10.101 bootstrap
# Expected: no error. If "already bootstrapped" appears, it's fine — continue.
```

- [ ] **Step 4: Fetch kubeconfig**

```bash
talosctl -n 192.168.10.101 kubeconfig ~/.kube/config
kubectl config current-context   # Expected: admin@homelab
```

- [ ] **Step 5: Verify nodes become Ready and Cilium is running**

```bash
# Cilium install Job (inline manifest) should complete
kubectl -n kube-system get job cilium-install
# Expected: COMPLETIONS 1/1

kubectl get nodes -o wide
# Expected (after a few minutes): all three Ready, with INTERNAL-IP 192.168.10.101/102/103
# talos-cp-01  Ready  control-plane
# talos-w-01   Ready  <none>
# talos-w-02   Ready  <none>

kubectl -n kube-system get pods -l k8s-app=cilium
# Expected: 3 cilium agent pods Running

kubectl -n kube-system get pods -l k8s-app=hubble-relay
# Expected: hubble-relay Running
```

- [ ] **Step 6: Confirm clean git state (no secrets/configs leaked)**

```bash
cd "$(git rev-parse --show-toplevel)"
git status --short
# Expected: clean, or only untracked gitignored files (homelab.agekey, clusterconfig/, terraform state/tfvars). None of those should be staged.
grep -RIl 'AGE-SECRET-KEY' --include='*.yaml' kubernetes/ infrastructure/ 2>/dev/null || echo "no plaintext age secrets — good"
```

---

## Final Verification Checklist (Phase 1)

- [ ] `terraform output maintenance_ips` showed real DHCP IPs for all 3 VMs
- [ ] All 3 nodes `STATUS=Ready` at static IPs 101/102/103: `kubectl get nodes -o wide`
- [ ] `cilium-install` Job `COMPLETIONS 1/1`; 3 cilium agents + hubble-relay Running
- [ ] kube-proxy is NOT running (replaced by Cilium): `kubectl -n kube-system get ds kube-proxy` → `NotFound`
- [ ] `~/.kube/config` works: `kubectl get --raw='/healthz'` → `ok`
- [ ] No plaintext secrets committed: `git grep -I 'AGE-SECRET-KEY'` returns nothing; `homelab.agekey`, `terraform.tfvars`, `clusterconfig/` all untracked/gitignored
- [ ] **Next:** resume the `phase2-5` plan at **Task 3 (Bootstrap Flux)**

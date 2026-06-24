# Homelab Kubernetes & Observability — Implementation Plan (Phase 2–5)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bootstrap Flux GitOps on an existing Talos cluster, wire up CNI + storage + secrets, then deploy a full observability stack (Prometheus + Loki + Tempo + Grafana) accessible via Tailscale.

**Architecture:** Incremental layered build — each task produces a working cluster state. Cilium is deployed as a Talos inline manifest before Flux; all subsequent components are Flux HelmReleases. SOPS+age encrypts all secrets in git.

**Tech Stack:** Talos Linux, Cilium 1.16.x, Flux v2, SOPS + age, local-path-provisioner, Tailscale Kubernetes Operator, kube-prometheus-stack, Loki (monolithic), Vector, Tempo, OpenTelemetry Collector, cert-manager.

## Global Constraints

- GitHub username: `cuonghv00`, repo: `homelab`, branch: `main`
- Flux bootstrap path: `kubernetes/flux/bootstrap`
- All secrets: SOPS-encrypted with age before committing; never commit plaintext secrets
- All k8s resources after bootstrap: managed via Flux HelmRelease or Kustomization — no manual `kubectl apply`
- Talos: no SSH; all config changes via `talosctl apply-config`
- Tailscale MagicDNS + HTTPS must be enabled in Tailscale admin console before Task 8
- Storage class: `local-path` (default) for all PVCs
- Namespaces: `flux-system`, `kube-system` (Cilium), `cert-manager`, `tailscale`, `monitoring`
- Flux sync interval: `10m` for platform components, `5m` for monitoring

---

## Phase 2 — Cilium CNI + Flux Bootstrap

### Task 1: Add .gitignore and repo skeleton

**Files:**
- Create: `.gitignore`
- Create: `kubernetes/flux/bootstrap/.gitkeep`
- Create: `kubernetes/flux/repositories/.gitkeep`
- Create: `kubernetes/platform/.gitkeep`
- Create: `kubernetes/monitoring/.gitkeep`
- Create: `infrastructure/talos/.gitkeep`
- Create: `infrastructure/terraform/.gitkeep`

**Interfaces:**
- Produces: directory structure that all subsequent tasks populate

- [ ] **Step 1: Create .gitignore**

```
# infrastructure/talos/clusterconfig/ — generated Talos machine configs contain secrets
infrastructure/talos/clusterconfig/

# Terraform state and local overrides
infrastructure/terraform/**/.terraform/
infrastructure/terraform/**/terraform.tfstate
infrastructure/terraform/**/terraform.tfstate.backup
infrastructure/terraform/**/*.tfvars
!infrastructure/terraform/**/*.tfvars.example

# age private key — never commit
*.agekey
```

- [ ] **Step 2: Create directory skeleton**

```bash
mkdir -p kubernetes/flux/bootstrap
mkdir -p kubernetes/flux/repositories/helm
mkdir -p kubernetes/platform/cilium
mkdir -p kubernetes/platform/cert-manager
mkdir -p kubernetes/platform/storage
mkdir -p kubernetes/platform/tailscale
mkdir -p kubernetes/monitoring/kube-prometheus-stack
mkdir -p kubernetes/monitoring/loki
mkdir -p kubernetes/monitoring/vector
mkdir -p kubernetes/monitoring/tempo
mkdir -p infrastructure/talos
mkdir -p infrastructure/terraform
touch kubernetes/flux/bootstrap/.gitkeep
touch kubernetes/flux/repositories/.gitkeep
```

- [ ] **Step 3: Commit**

```bash
git add .gitignore kubernetes/ infrastructure/
git commit -m "chore: scaffold repo directory structure"
```

---

### Task 2: Configure Talos for Cilium (inline manifest)

**Files:**
- Create: `infrastructure/talos/talhelper.yaml`
- Create: `infrastructure/talos/patches/all-nodes.yaml`
- Create: `infrastructure/talos/patches/controlplane.yaml`

**Interfaces:**
- Consumes: Talos node IPs from Phase 1 Terraform outputs (3 IPs needed)
- Produces: `infrastructure/talos/clusterconfig/` (gitignored) — machine configs ready to apply

**Prerequisites:** `talhelper` installed (`brew install siderolabs/tap/talhelper` or `go install`), `talosctl` installed.

- [ ] **Step 1: Write talhelper.yaml**

Replace `<CP_IP>`, `<W1_IP>`, `<W2_IP>` with actual IPs from Phase 1 Terraform outputs.

```yaml
# infrastructure/talos/talhelper.yaml
clusterName: homelab
talosVersion: v1.9.0
kubernetesVersion: v1.32.0
endpoint: https://<CP_IP>:6443
allowSchedulingOnControlPlanes: false

patches:
  - "@./patches/all-nodes.yaml"

nodes:
  - hostname: talos-cp-01
    ipAddress: <CP_IP>
    installDisk: /dev/sda
    controlPlane: true
    patches:
      - "@./patches/controlplane.yaml"

  - hostname: talos-w-01
    ipAddress: <W1_IP>
    installDisk: /dev/sda
    controlPlane: false

  - hostname: talos-w-02
    ipAddress: <W2_IP>
    installDisk: /dev/sda
    controlPlane: false
```

- [ ] **Step 2: Write all-nodes patch (disable default CNI + kube-proxy)**

```yaml
# infrastructure/talos/patches/all-nodes.yaml
- op: add
  path: /machine/sysctls
  value:
    net.core.bpf_jit_harden: "1"
- op: replace
  path: /cluster/network/cni
  value:
    name: none
- op: replace
  path: /cluster/proxy/disabled
  value: true
```

- [ ] **Step 3: Write controlplane patch (Cilium inline manifest Job)**

```yaml
# infrastructure/talos/patches/controlplane.yaml
- op: add
  path: /cluster/inlineManifests
  value:
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
                    - --version=1.16.5
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
cd infrastructure/talos
talhelper genconfig
```

Expected: `clusterconfig/` directory created with `homelab-talos-cp-01.yaml`, `homelab-talos-w-01.yaml`, `homelab-talos-w-02.yaml`, `talosconfig`.

- [ ] **Step 5: Apply config to all nodes**

```bash
# Apply to control plane
talosctl apply-config \
  --nodes <CP_IP> \
  --file clusterconfig/homelab-talos-cp-01.yaml

# Apply to workers
talosctl apply-config \
  --nodes <W1_IP> \
  --file clusterconfig/homelab-talos-w-01.yaml

talosctl apply-config \
  --nodes <W2_IP> \
  --file clusterconfig/homelab-talos-w-02.yaml
```

Nodes will reboot and apply the new config. Wait ~2 minutes.

- [ ] **Step 6: Verify Cilium Job runs and nodes become Ready**

```bash
# Watch cilium-install Job
kubectl -n kube-system get job cilium-install -w
# Expected: COMPLETIONS 1/1

# Check all nodes Ready
kubectl get nodes
# Expected:
# NAME           STATUS   ROLES           AGE   VERSION
# talos-cp-01    Ready    control-plane   Xm    v1.32.x
# talos-w-01     Ready    <none>          Xm    v1.32.x
# talos-w-02     Ready    <none>          Xm    v1.32.x

# Check Cilium pods
kubectl -n kube-system get pods -l app.kubernetes.io/name=cilium-agent
# Expected: 3 pods Running

# Run Cilium connectivity check
cilium status
# Expected: all green
```

- [ ] **Step 7: Commit talhelper config (not clusterconfig — it's gitignored)**

```bash
git add infrastructure/talos/talhelper.yaml infrastructure/talos/patches/
git commit -m "feat(talos): configure Cilium inline manifest with Hubble"
```

---

### Task 3: Bootstrap Flux

**Files:**
- Auto-created by Flux in `kubernetes/flux/bootstrap/` (flux-system namespace manifests)

**Interfaces:**
- Consumes: GitHub PAT with `repo` scope, working kubeconfig
- Produces: Flux controllers running in `flux-system`, GitRepository watching `homelab` repo

**Prerequisites:** `flux` CLI installed (`brew install fluxcd/tap/flux`), GitHub Personal Access Token with `repo` scope created at github.com/settings/tokens.

- [ ] **Step 1: Verify Flux prerequisites**

```bash
flux check --pre
# Expected: all checks passing
```

- [ ] **Step 2: Bootstrap Flux**

```bash
export GITHUB_TOKEN=<your-github-pat>

flux bootstrap github \
  --owner=cuonghv00 \
  --repository=homelab \
  --branch=main \
  --path=kubernetes/flux/bootstrap \
  --personal \
  --token-auth
```

Expected output ends with:
```
✔ all components are healthy
✔ configured deploy key "flux-system-main-kubernetes-flux-bootstrap" with read-only access
```

- [ ] **Step 3: Pull the changes Flux pushed to the repo**

```bash
git pull origin main
```

Flux will have created files under `kubernetes/flux/bootstrap/`:
- `flux-system/gotk-components.yaml` — Flux controllers
- `flux-system/gotk-sync.yaml` — GitRepository + Kustomization pointing at `kubernetes/flux/bootstrap`
- `flux-system/kustomization.yaml`

- [ ] **Step 4: Verify Flux is running**

```bash
flux get all -n flux-system
# Expected:
# NAME                    READY   STATUS
# gitrepository/flux-system True  Fetched revision: main@sha1:...
# kustomization/flux-system True  Applied revision: main@sha1:...

kubectl -n flux-system get pods
# Expected: 4 pods Running (source-controller, kustomize-controller, helm-controller, notification-controller)
```

---

### Task 4: Flux Kustomization tree — platform + monitoring

This wires Flux to watch the `kubernetes/platform/` and `kubernetes/monitoring/` directories.

**Files:**
- Create: `kubernetes/flux/bootstrap/platform.yaml`
- Create: `kubernetes/flux/bootstrap/monitoring.yaml`
- Create: `kubernetes/flux/repositories/helm/kustomization.yaml`
- Create: `kubernetes/flux/repositories/helm/grafana.yaml`
- Create: `kubernetes/flux/repositories/helm/prometheus-community.yaml`
- Create: `kubernetes/flux/repositories/helm/grafana-loki.yaml`
- Create: `kubernetes/flux/repositories/helm/vector.yaml`
- Create: `kubernetes/flux/repositories/helm/tempo.yaml`
- Create: `kubernetes/flux/repositories/helm/cert-manager.yaml`
- Create: `kubernetes/flux/repositories/helm/tailscale.yaml`
- Modify: `kubernetes/flux/bootstrap/flux-system/kustomization.yaml`

**Interfaces:**
- Produces: Flux Kustomization CRDs that watch platform/ and monitoring/ paths in repo

- [ ] **Step 1: Write HelmRepository sources**

```yaml
# kubernetes/flux/repositories/helm/grafana.yaml
apiVersion: source.toolkit.fluxcd.io/v1
kind: HelmRepository
metadata:
  name: grafana
  namespace: flux-system
spec:
  interval: 1h
  url: https://grafana.github.io/helm-charts
```

```yaml
# kubernetes/flux/repositories/helm/prometheus-community.yaml
apiVersion: source.toolkit.fluxcd.io/v1
kind: HelmRepository
metadata:
  name: prometheus-community
  namespace: flux-system
spec:
  interval: 1h
  url: https://prometheus-community.github.io/helm-charts
```

```yaml
# kubernetes/flux/repositories/helm/vector.yaml
apiVersion: source.toolkit.fluxcd.io/v1
kind: HelmRepository
metadata:
  name: vector
  namespace: flux-system
spec:
  interval: 1h
  url: https://helm.vector.dev
```

Note: Tempo charts are hosted on the same URL as the `grafana` HelmRepository — no separate entry needed.

```yaml
# kubernetes/flux/repositories/helm/cert-manager.yaml
apiVersion: source.toolkit.fluxcd.io/v1
kind: HelmRepository
metadata:
  name: cert-manager
  namespace: flux-system
spec:
  interval: 1h
  url: https://charts.jetstack.io
```

```yaml
# kubernetes/flux/repositories/helm/tailscale.yaml
apiVersion: source.toolkit.fluxcd.io/v1
kind: HelmRepository
metadata:
  name: tailscale
  namespace: flux-system
spec:
  interval: 1h
  url: https://pkgs.tailscale.com/helmcharts
```

- [ ] **Step 2: Write kustomization.yaml for repositories**

```yaml
# kubernetes/flux/repositories/helm/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - grafana.yaml
  - prometheus-community.yaml
  - vector.yaml
  - cert-manager.yaml
  - tailscale.yaml
```

```yaml
# kubernetes/flux/repositories/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - helm
```

- [ ] **Step 3: Write Flux Kustomization CRDs for platform and monitoring**

```yaml
# kubernetes/flux/bootstrap/platform.yaml
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: platform
  namespace: flux-system
spec:
  interval: 10m0s
  path: ./kubernetes/platform
  prune: true
  wait: true
  timeout: 5m0s
  sourceRef:
    kind: GitRepository
    name: flux-system
  dependsOn:
    - name: flux-system
```

```yaml
# kubernetes/flux/bootstrap/monitoring.yaml
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: monitoring
  namespace: flux-system
spec:
  interval: 5m0s
  path: ./kubernetes/monitoring
  prune: true
  wait: true
  timeout: 10m0s
  sourceRef:
    kind: GitRepository
    name: flux-system
  dependsOn:
    - name: platform
```

- [ ] **Step 4: Add repositories + new Kustomizations to the bootstrap kustomization**

Edit `kubernetes/flux/bootstrap/flux-system/kustomization.yaml` — add references to the new files:

```yaml
# kubernetes/flux/bootstrap/flux-system/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - gotk-components.yaml
  - gotk-sync.yaml
  - ../repositories
  - ../platform.yaml
  - ../monitoring.yaml
```

- [ ] **Step 5: Create stub kustomization.yaml files so Flux doesn't error on empty dirs**

```yaml
# kubernetes/platform/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources: []
```

```yaml
# kubernetes/monitoring/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources: []
```

- [ ] **Step 6: Commit and push**

```bash
git add kubernetes/flux/
git commit -m "feat(flux): wire HelmRepositories, platform and monitoring Kustomizations"
git push origin main
```

- [ ] **Step 7: Verify Flux picks up the new Kustomizations**

```bash
flux get kustomizations
# Expected:
# NAME          READY   STATUS
# flux-system   True    Applied revision: main@sha1:...
# platform      True    Applied revision: main@sha1:...
# monitoring    True    Applied revision: main@sha1:...

flux get sources helm -n flux-system
# Expected: 5 HelmRepositories, all Ready True
```

---

## Phase 3 — Platform: Storage + SOPS + Tailscale

### Task 5: SOPS + age secret encryption setup

**Files:**
- Create: `.sops.yaml` (root of repo)
- Create: `kubernetes/flux/bootstrap/flux-system/sops-secret.yaml` (gitignored — contains private key, applied once manually)

**Interfaces:**
- Produces: age keypair; Flux configured to auto-decrypt SOPS secrets on apply

**Prerequisites:** `age` installed (`brew install age`), `sops` installed (`brew install sops`).

- [ ] **Step 1: Generate age keypair**

```bash
age-keygen -o homelab.agekey
```

Output example:
```
# created: 2026-06-25T10:00:00+07:00
# public key: age1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
AGE-SECRET-KEY-1XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
```

Save the **public key** value (starts with `age1`). Store `homelab.agekey` securely outside the repo (e.g., a password manager).

- [ ] **Step 2: Write .sops.yaml**

Replace `<AGE_PUBLIC_KEY>` with the public key from Step 1.

```yaml
# .sops.yaml
creation_rules:
  - path_regex: kubernetes/.*\.sops\.yaml
    encrypted_regex: ^(data|stringData)$
    age: <AGE_PUBLIC_KEY>
  - path_regex: infrastructure/talos/talsecret\.sops\.yaml
    age: <AGE_PUBLIC_KEY>
```

- [ ] **Step 3: Create the Flux SOPS secret (NOT committed — applied manually once)**

```bash
cat homelab.agekey | kubectl create secret generic sops-age \
  --namespace=flux-system \
  --from-file=age.agekey=/dev/stdin \
  --dry-run=client -o yaml > /tmp/sops-age-secret.yaml

kubectl apply -f /tmp/sops-age-secret.yaml
rm /tmp/sops-age-secret.yaml
```

- [ ] **Step 4: Patch the flux-system Kustomization to enable SOPS decryption**

Edit `kubernetes/flux/bootstrap/flux-system/gotk-sync.yaml` — add `decryption` block to the `flux-system` Kustomization CR:

```yaml
# Find the Kustomization named flux-system and add:
spec:
  decryption:
    provider: sops
    secretRef:
      name: sops-age
```

The full `gotk-sync.yaml` Kustomization block should look like:

```yaml
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: flux-system
  namespace: flux-system
spec:
  interval: 10m0s
  path: ./kubernetes/flux/bootstrap
  prune: true
  sourceRef:
    kind: GitRepository
    name: flux-system
  decryption:
    provider: sops
    secretRef:
      name: sops-age
```

Apply the same `decryption` block to the `platform` and `monitoring` Kustomization CRDs in `kubernetes/flux/bootstrap/platform.yaml` and `monitoring.yaml`.

- [ ] **Step 5: Commit .sops.yaml and updated Kustomization CRDs**

```bash
git add .sops.yaml kubernetes/flux/bootstrap/flux-system/gotk-sync.yaml \
        kubernetes/flux/bootstrap/platform.yaml \
        kubernetes/flux/bootstrap/monitoring.yaml
git commit -m "feat(sops): configure age encryption for Flux secret decryption"
git push origin main
```

- [ ] **Step 6: Verify SOPS decryption works**

Create a test secret, encrypt it, commit, push, and verify Flux applies it:

```bash
# Create test secret
kubectl create secret generic sops-test \
  --namespace=flux-system \
  --from-literal=key=hello \
  --dry-run=client -o yaml > /tmp/sops-test.yaml

# Encrypt it
sops --encrypt /tmp/sops-test.yaml > kubernetes/flux/bootstrap/sops-test.sops.yaml

# Add to flux-system kustomization resources temporarily, push, check
# Then remove it after verification
```

Verify: `kubectl -n flux-system get secret sops-test -o jsonpath='{.data.key}' | base64 -d` → `hello`.

Remove the test secret file and commit the removal.

---

### Task 6: local-path-provisioner via Flux

**Files:**
- Create: `kubernetes/platform/storage/namespace.yaml`
- Create: `kubernetes/platform/storage/helmrelease.yaml`
- Create: `kubernetes/platform/storage/kustomization.yaml`
- Modify: `kubernetes/platform/kustomization.yaml`

**Interfaces:**
- Produces: `StorageClass` named `local-path` set as cluster default

- [ ] **Step 1: Write storage manifests**

```yaml
# kubernetes/platform/storage/namespace.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: local-path-storage
```

```yaml
# kubernetes/platform/storage/helmrelease.yaml
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: local-path-provisioner
  namespace: local-path-storage
spec:
  interval: 1h
  chart:
    spec:
      chart: local-path-provisioner
      version: "0.0.28"
      sourceRef:
        kind: HelmRepository
        name: local-path-provisioner
        namespace: flux-system
  values:
    storageClass:
      create: true
      name: local-path
      defaultClass: true
    nodePathMap:
      - node: DEFAULT_PATH_FOR_NON_LISTED_NODES
        paths:
          - /var/local-path-provisioner
```

Add the HelmRepository for local-path-provisioner:

```yaml
# kubernetes/flux/repositories/helm/local-path.yaml
apiVersion: source.toolkit.fluxcd.io/v1
kind: HelmRepository
metadata:
  name: local-path-provisioner
  namespace: flux-system
spec:
  interval: 1h
  url: https://charts.containeroo.ch
```

```yaml
# kubernetes/platform/storage/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - namespace.yaml
  - helmrelease.yaml
```

- [ ] **Step 2: Update platform kustomization.yaml to include storage**

```yaml
# kubernetes/platform/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - storage
```

- [ ] **Step 3: Add local-path to flux repositories kustomization**

Edit `kubernetes/flux/repositories/helm/kustomization.yaml`:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - grafana.yaml
  - prometheus-community.yaml
  - vector.yaml
  - cert-manager.yaml
  - tailscale.yaml
  - local-path.yaml
```

- [ ] **Step 4: Commit and push**

```bash
git add kubernetes/platform/ kubernetes/flux/repositories/
git commit -m "feat(storage): add local-path-provisioner as default StorageClass"
git push origin main
```

- [ ] **Step 5: Verify**

```bash
flux reconcile kustomization platform --with-source
kubectl get storageclass
# Expected:
# NAME                   PROVISIONER             RECLAIMPOLICY   VOLUMEBINDINGMODE      DEFAULT
# local-path (default)   rancher.io/local-path   Delete          WaitForFirstConsumer   true
```

---

### Task 7: cert-manager via Flux

**Files:**
- Create: `kubernetes/platform/cert-manager/namespace.yaml`
- Create: `kubernetes/platform/cert-manager/helmrelease.yaml`
- Create: `kubernetes/platform/cert-manager/kustomization.yaml`
- Modify: `kubernetes/platform/kustomization.yaml`

**Interfaces:**
- Produces: `cert-manager` namespace with controller, `ClusterIssuer` available for TLS

- [ ] **Step 1: Write cert-manager manifests**

```yaml
# kubernetes/platform/cert-manager/namespace.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: cert-manager
```

```yaml
# kubernetes/platform/cert-manager/helmrelease.yaml
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: cert-manager
  namespace: cert-manager
spec:
  interval: 1h
  chart:
    spec:
      chart: cert-manager
      version: "v1.16.x"
      sourceRef:
        kind: HelmRepository
        name: cert-manager
        namespace: flux-system
  values:
    crds:
      enabled: true
    prometheus:
      enabled: true
      servicemonitor:
        enabled: true
```

```yaml
# kubernetes/platform/cert-manager/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - namespace.yaml
  - helmrelease.yaml
```

- [ ] **Step 2: Update platform kustomization.yaml**

```yaml
# kubernetes/platform/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - storage
  - cert-manager
```

- [ ] **Step 3: Commit and push**

```bash
git add kubernetes/platform/cert-manager/ kubernetes/platform/kustomization.yaml
git commit -m "feat(cert-manager): deploy via Flux HelmRelease"
git push origin main
```

- [ ] **Step 4: Verify**

```bash
flux reconcile kustomization platform --with-source
kubectl -n cert-manager get pods
# Expected: cert-manager, cert-manager-cainjector, cert-manager-webhook pods Running

kubectl get crds | grep cert-manager.io
# Expected: certificates.cert-manager.io, clusterissuers.cert-manager.io, ...
```

---

### Task 8: Tailscale Kubernetes Operator

**Files:**
- Create: `kubernetes/platform/tailscale/namespace.yaml`
- Create: `kubernetes/platform/tailscale/helmrelease.yaml`
- Create: `kubernetes/platform/tailscale/oauth-secret.sops.yaml`
- Create: `kubernetes/platform/tailscale/kustomization.yaml`
- Modify: `kubernetes/platform/kustomization.yaml`

**Interfaces:**
- Consumes: Tailscale OAuth client credentials (from Tailscale admin console → Settings → OAuth clients)
- Produces: Tailscale operator running; Services annotated with `tailscale.com/expose=true` get a `<name>.homelab.ts.net` hostname

**Prerequisites:** Create a Tailscale OAuth client at https://login.tailscale.com/admin/settings/oauth with scopes: `devices:core:write`. Copy the client ID and secret.

- [ ] **Step 1: Create encrypted OAuth secret**

```bash
kubectl create secret generic operator-oauth \
  --namespace=tailscale \
  --from-literal=client_id=<TAILSCALE_OAUTH_CLIENT_ID> \
  --from-literal=client_secret=<TAILSCALE_OAUTH_CLIENT_SECRET> \
  --dry-run=client -o yaml > /tmp/tailscale-oauth.yaml

sops --encrypt /tmp/tailscale-oauth.yaml > kubernetes/platform/tailscale/oauth-secret.sops.yaml
rm /tmp/tailscale-oauth.yaml
```

- [ ] **Step 2: Write tailscale manifests**

```yaml
# kubernetes/platform/tailscale/namespace.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: tailscale
```

```yaml
# kubernetes/platform/tailscale/helmrelease.yaml
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: tailscale-operator
  namespace: tailscale
spec:
  interval: 1h
  chart:
    spec:
      chart: tailscale-operator
      version: ">=1.76.0 <2.0.0"  # run: helm search repo tailscale/tailscale-operator
      sourceRef:
        kind: HelmRepository
        name: tailscale
        namespace: flux-system
  values:
    operatorConfig:
      hostname: homelab-operator
      # Operator reads client_id and client_secret keys from this Secret
      secret:
        name: operator-oauth
    # Verify exact values structure with: helm show values tailscale/tailscale-operator
```

```yaml
# kubernetes/platform/tailscale/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - namespace.yaml
  - oauth-secret.sops.yaml
  - helmrelease.yaml
```

- [ ] **Step 3: Update platform kustomization.yaml**

```yaml
# kubernetes/platform/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - storage
  - cert-manager
  - tailscale
```

- [ ] **Step 4: Commit and push**

```bash
git add kubernetes/platform/tailscale/ kubernetes/platform/kustomization.yaml
git commit -m "feat(tailscale): deploy Kubernetes Operator with SOPS-encrypted OAuth secret"
git push origin main
```

- [ ] **Step 5: Verify**

```bash
flux reconcile kustomization platform --with-source
kubectl -n tailscale get pods
# Expected: tailscale-operator pod Running

# Check Tailscale admin console → Machines: homelab-operator should appear
```

---

## Phase 4 — Observability Stack

### Task 9: kube-prometheus-stack (Prometheus + Alertmanager + Grafana)

**Files:**
- Create: `kubernetes/monitoring/kube-prometheus-stack/namespace.yaml`
- Create: `kubernetes/monitoring/kube-prometheus-stack/helmrelease.yaml`
- Create: `kubernetes/monitoring/kube-prometheus-stack/kustomization.yaml`
- Modify: `kubernetes/monitoring/kustomization.yaml`

**Interfaces:**
- Produces: `monitoring` namespace with Prometheus (port 9090), Grafana (port 3000), Alertmanager (port 9093); `ServiceMonitor` CRD available for all monitoring tasks

- [ ] **Step 1: Write namespace**

```yaml
# kubernetes/monitoring/kube-prometheus-stack/namespace.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: monitoring
```

- [ ] **Step 2: Write HelmRelease with customized values**

```yaml
# kubernetes/monitoring/kube-prometheus-stack/helmrelease.yaml
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: kube-prometheus-stack
  namespace: monitoring
spec:
  interval: 1h
  chart:
    spec:
      chart: kube-prometheus-stack
      version: ">=67.0.0 <68.0.0"  # run: helm search repo prometheus-community/kube-prometheus-stack
      sourceRef:
        kind: HelmRepository
        name: prometheus-community
        namespace: flux-system
  values:
    fullnameOverride: prometheus

    prometheus:
      prometheusSpec:
        retention: 30d
        storageSpec:
          volumeClaimTemplate:
            spec:
              storageClassName: local-path
              accessModes: [ReadWriteOnce]
              resources:
                requests:
                  storage: 30Gi
        ruleSelectorNilUsesHelmValues: false
        serviceMonitorSelectorNilUsesHelmValues: false
        podMonitorSelectorNilUsesHelmValues: false

    grafana:
      defaultDashboardsTimezone: Asia/Ho_Chi_Minh
      persistence:
        enabled: true
        storageClassName: local-path
        size: 1Gi
      # Datasources added in Task 12
      additionalDataSources: []
      sidecar:
        dashboards:
          enabled: true
          searchNamespace: ALL

    alertmanager:
      alertmanagerSpec:
        storage:
          volumeClaimTemplate:
            spec:
              storageClassName: local-path
              accessModes: [ReadWriteOnce]
              resources:
                requests:
                  storage: 2Gi

    # Talos-specific: disable components that conflict with Talos
    kubeControllerManager:
      enabled: false
    kubeScheduler:
      enabled: false
    kubeEtcd:
      enabled: false
    kubeProxy:
      enabled: false
```

- [ ] **Step 3: Write kustomization.yaml**

```yaml
# kubernetes/monitoring/kube-prometheus-stack/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - namespace.yaml
  - helmrelease.yaml
```

- [ ] **Step 4: Update monitoring kustomization.yaml**

```yaml
# kubernetes/monitoring/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - kube-prometheus-stack
```

- [ ] **Step 5: Commit and push**

```bash
git add kubernetes/monitoring/
git commit -m "feat(monitoring): deploy kube-prometheus-stack"
git push origin main
```

- [ ] **Step 6: Verify (stack takes ~3-5 minutes to fully start)**

```bash
flux reconcile kustomization monitoring --with-source
kubectl -n monitoring get pods -w
# Expected: prometheus-*, alertmanager-*, grafana-*, kube-state-metrics-*, prometheus-node-exporter-* all Running

kubectl -n monitoring get pvc
# Expected: 3 PVCs Bound (prometheus, alertmanager, grafana) via local-path
```

---

### Task 10: Loki (log aggregation) + Vector (log collector)

**Files:**
- Create: `kubernetes/monitoring/loki/helmrelease.yaml`
- Create: `kubernetes/monitoring/loki/kustomization.yaml`
- Create: `kubernetes/monitoring/vector/helmrelease.yaml`
- Create: `kubernetes/monitoring/vector/kustomization.yaml`
- Modify: `kubernetes/monitoring/kustomization.yaml`

**Interfaces:**
- Consumes: `monitoring` namespace (created in Task 9), `local-path` StorageClass
- Produces: Loki HTTP endpoint `http://loki.monitoring.svc.cluster.local:3100`; Vector DaemonSet collecting all pod logs and forwarding to Loki

- [ ] **Step 1: Write Loki HelmRelease (monolithic mode)**

```yaml
# kubernetes/monitoring/loki/helmrelease.yaml
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: loki
  namespace: monitoring
spec:
  interval: 1h
  dependsOn:
    - name: kube-prometheus-stack
      namespace: monitoring
  chart:
    spec:
      chart: loki
      version: ">=6.0.0 <7.0.0"  # run: helm search repo grafana/loki
      sourceRef:
        kind: HelmRepository
        name: grafana
        namespace: flux-system
  values:
    deploymentMode: SingleBinary
    loki:
      commonConfig:
        replication_factor: 1
      storage:
        type: filesystem
      auth_enabled: false
      limits_config:
        retention_period: 336h   # 14 days
      compactor:
        retention_enabled: true
    singleBinary:
      replicas: 1
      persistence:
        enabled: true
        storageClass: local-path
        size: 20Gi
    # Disable distributed components (monolithic mode)
    read:
      replicas: 0
    write:
      replicas: 0
    backend:
      replicas: 0
    # Disable Loki's built-in gateway/nginx
    gateway:
      enabled: false
    # ServiceMonitor for Prometheus scraping
    monitoring:
      serviceMonitor:
        enabled: true
        labels:
          release: prometheus
```

```yaml
# kubernetes/monitoring/loki/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - helmrelease.yaml
```

- [ ] **Step 2: Write Vector HelmRelease (DaemonSet log collector)**

```yaml
# kubernetes/monitoring/vector/helmrelease.yaml
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: vector
  namespace: monitoring
spec:
  interval: 1h
  dependsOn:
    - name: loki
      namespace: monitoring
  chart:
    spec:
      chart: vector
      version: ">=0.36.0 <1.0.0"  # run: helm search repo vector/vector
      sourceRef:
        kind: HelmRepository
        name: vector
        namespace: flux-system
  values:
    role: Agent
    customConfig:
      data_dir: /vector-data-dir
      api:
        enabled: true
        address: 127.0.0.1:8686
        playground: false
      sources:
        kubernetes_logs:
          type: kubernetes_logs
          self_node_name: "${VECTOR_SELF_NODE_NAME}"
      transforms:
        add_cluster_metadata:
          type: remap
          inputs: [kubernetes_logs]
          source: |
            .cluster = "homelab"
      sinks:
        loki:
          type: loki
          inputs: [add_cluster_metadata]
          endpoint: http://loki.monitoring.svc.cluster.local:3100
          encoding:
            codec: json
          labels:
            cluster: "{{ cluster }}"
            namespace: "{{ kubernetes.pod_namespace }}"
            pod: "{{ kubernetes.pod_name }}"
            container: "{{ kubernetes.container_name }}"
            stream: "{{ stream }}"
```

```yaml
# kubernetes/monitoring/vector/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - helmrelease.yaml
```

- [ ] **Step 3: Update monitoring kustomization.yaml**

```yaml
# kubernetes/monitoring/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - kube-prometheus-stack
  - loki
  - vector
```

- [ ] **Step 4: Commit and push**

```bash
git add kubernetes/monitoring/loki/ kubernetes/monitoring/vector/ kubernetes/monitoring/kustomization.yaml
git commit -m "feat(monitoring): add Loki (monolithic) and Vector log pipeline"
git push origin main
```

- [ ] **Step 5: Verify**

```bash
flux reconcile kustomization monitoring --with-source

kubectl -n monitoring get pods -l app.kubernetes.io/name=loki
# Expected: 1 pod Running

kubectl -n monitoring get pods -l app.kubernetes.io/name=vector
# Expected: 3 pods Running (one per node — DaemonSet)

# Verify Vector is shipping logs to Loki
kubectl -n monitoring logs -l app.kubernetes.io/name=vector --tail=20
# Expected: no errors, "Sending batch" log lines

# Test Loki query directly
kubectl -n monitoring port-forward svc/loki 3100:3100 &
curl -s 'http://localhost:3100/loki/api/v1/query?query={cluster="homelab"}' | jq '.data.result | length'
# Expected: number > 0 (logs are arriving)
kill %1
```

---

### Task 11: Tempo (distributed tracing) + OpenTelemetry Collector

**Files:**
- Create: `kubernetes/monitoring/tempo/helmrelease.yaml`
- Create: `kubernetes/monitoring/tempo/kustomization.yaml`
- Create: `kubernetes/monitoring/otel-collector/helmrelease.yaml`
- Create: `kubernetes/monitoring/otel-collector/kustomization.yaml`
- Modify: `kubernetes/monitoring/kustomization.yaml`

**Interfaces:**
- Produces: Tempo OTLP endpoint `http://tempo.monitoring.svc.cluster.local:4317` (gRPC); OTel Collector endpoint `http://otel-collector.monitoring.svc.cluster.local:4317` as the public trace ingest gateway

- [ ] **Step 1: Add OTel Collector HelmRepository**

```yaml
# kubernetes/flux/repositories/helm/opentelemetry.yaml
apiVersion: source.toolkit.fluxcd.io/v1
kind: HelmRepository
metadata:
  name: opentelemetry
  namespace: flux-system
spec:
  interval: 1h
  url: https://open-telemetry.github.io/opentelemetry-helm-charts
```

Add `opentelemetry.yaml` to `kubernetes/flux/repositories/helm/kustomization.yaml` resources list.

- [ ] **Step 2: Write Tempo HelmRelease**

```yaml
# kubernetes/monitoring/tempo/helmrelease.yaml
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: tempo
  namespace: monitoring
spec:
  interval: 1h
  dependsOn:
    - name: kube-prometheus-stack
      namespace: monitoring
  chart:
    spec:
      chart: tempo
      version: ">=1.0.0 <2.0.0"  # run: helm search repo grafana/tempo
      sourceRef:
        kind: HelmRepository
        name: grafana
        namespace: flux-system
  values:
    tempo:
      retention: 168h   # 7 days
      storage:
        trace:
          backend: local
          local:
            path: /var/tempo/traces
    persistence:
      enabled: true
      storageClassName: local-path
      size: 10Gi
    serviceMonitor:
      enabled: true
      additionalLabels:
        release: prometheus
```

```yaml
# kubernetes/monitoring/tempo/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - helmrelease.yaml
```

- [ ] **Step 3: Write OpenTelemetry Collector HelmRelease**

```yaml
# kubernetes/monitoring/otel-collector/helmrelease.yaml
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: otel-collector
  namespace: monitoring
spec:
  interval: 1h
  dependsOn:
    - name: tempo
      namespace: monitoring
  chart:
    spec:
      chart: opentelemetry-collector
      version: ">=0.108.0 <1.0.0"  # run: helm search repo opentelemetry/opentelemetry-collector
      sourceRef:
        kind: HelmRepository
        name: opentelemetry
        namespace: flux-system
  values:
    mode: deployment
    config:
      receivers:
        otlp:
          protocols:
            grpc:
              endpoint: 0.0.0.0:4317
            http:
              endpoint: 0.0.0.0:4318
      processors:
        batch:
          timeout: 1s
          send_batch_size: 1024
        memory_limiter:
          check_interval: 1s
          limit_mib: 400
      exporters:
        otlp/tempo:
          endpoint: http://tempo.monitoring.svc.cluster.local:4317
          tls:
            insecure: true
      service:
        pipelines:
          traces:
            receivers: [otlp]
            processors: [memory_limiter, batch]
            exporters: [otlp/tempo]
```

```yaml
# kubernetes/monitoring/otel-collector/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - helmrelease.yaml
```

- [ ] **Step 4: Update monitoring kustomization.yaml**

```yaml
# kubernetes/monitoring/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - kube-prometheus-stack
  - loki
  - vector
  - tempo
  - otel-collector
```

- [ ] **Step 5: Commit and push**

```bash
git add kubernetes/monitoring/tempo/ kubernetes/monitoring/otel-collector/ \
        kubernetes/monitoring/kustomization.yaml \
        kubernetes/flux/repositories/helm/opentelemetry.yaml \
        kubernetes/flux/repositories/helm/kustomization.yaml
git commit -m "feat(monitoring): add Tempo tracing backend and OTel Collector gateway"
git push origin main
```

- [ ] **Step 6: Verify**

```bash
flux reconcile kustomization monitoring --with-source

kubectl -n monitoring get pods -l app.kubernetes.io/name=tempo
# Expected: 1 pod Running

kubectl -n monitoring get pods -l app.kubernetes.io/name=opentelemetry-collector
# Expected: 1 pod Running

kubectl -n monitoring get pvc
# Expected: prometheus (30Gi), loki (20Gi), tempo (10Gi), grafana (1Gi) all Bound
```

---

### Task 12: Grafana datasources (Prometheus + Loki + Tempo)

Wire all three backends as Grafana datasources so Grafana becomes the single pane of glass.

**Files:**
- Create: `kubernetes/monitoring/kube-prometheus-stack/grafana-datasources.yaml`
- Modify: `kubernetes/monitoring/kube-prometheus-stack/helmrelease.yaml` (add datasources values)
- Modify: `kubernetes/monitoring/kube-prometheus-stack/kustomization.yaml`

**Interfaces:**
- Consumes: Loki service `loki.monitoring.svc.cluster.local:3100`, Tempo service `tempo.monitoring.svc.cluster.local:3100`
- Produces: Grafana with 3 datasources configured; Loki and Tempo linked (trace-to-log correlation)

- [ ] **Step 1: Add datasources to kube-prometheus-stack HelmRelease values**

In `kubernetes/monitoring/kube-prometheus-stack/helmrelease.yaml`, update the `grafana.additionalDataSources` field:

```yaml
    grafana:
      defaultDashboardsTimezone: Asia/Ho_Chi_Minh
      # ... existing values ...
      additionalDataSources:
        - name: Loki
          type: loki
          uid: loki
          url: http://loki.monitoring.svc.cluster.local:3100
          access: proxy
          isDefault: false
          jsonData:
            derivedFields:
              - datasourceUid: Tempo
                matcherRegex: '"traceID":"(\w+)"'
                name: TraceID
                url: "${__value.raw}"
        - name: Tempo
          type: tempo
          url: http://tempo.monitoring.svc.cluster.local:3100
          access: proxy
          isDefault: false
          uid: Tempo
          jsonData:
            httpMethod: GET
            serviceMap:
              datasourceUid: prometheus
            nodeGraph:
              enabled: true
            lokiSearch:
              datasourceUid: loki
```

- [ ] **Step 2: Commit and push**

```bash
git add kubernetes/monitoring/kube-prometheus-stack/helmrelease.yaml
git commit -m "feat(grafana): wire Loki and Tempo datasources with trace-log correlation"
git push origin main
```

- [ ] **Step 3: Verify Grafana datasources**

```bash
flux reconcile kustomization monitoring --with-source

# Port-forward Grafana temporarily
kubectl -n monitoring port-forward svc/prometheus-grafana 3000:80 &
# Open http://localhost:3000 — admin/prom-operator (default)
# Go to Configuration → Data Sources
# Expected: Prometheus (default), Loki, Tempo all green

kill %1
```

---

## Phase 5 — Alerting + Access

### Task 13: Alertmanager → Telegram bot

**Files:**
- Create: `kubernetes/monitoring/kube-prometheus-stack/alertmanager-secret.sops.yaml`
- Modify: `kubernetes/monitoring/kube-prometheus-stack/helmrelease.yaml` (Alertmanager config)
- Modify: `kubernetes/monitoring/kube-prometheus-stack/kustomization.yaml`

**Prerequisites:**
1. Create a Telegram bot via @BotFather → get `BOT_TOKEN`
2. Send any message to your bot, then call `https://api.telegram.org/bot<BOT_TOKEN>/getUpdates` to get your `CHAT_ID`

- [ ] **Step 1: Create and encrypt the Telegram secret**

```bash
kubectl create secret generic alertmanager-telegram \
  --namespace=monitoring \
  --from-literal=telegram_bot_token=<BOT_TOKEN> \
  --from-literal=telegram_chat_id=<CHAT_ID> \
  --dry-run=client -o yaml > /tmp/telegram-secret.yaml

sops --encrypt /tmp/telegram-secret.yaml > \
  kubernetes/monitoring/kube-prometheus-stack/alertmanager-secret.sops.yaml
rm /tmp/telegram-secret.yaml
```

- [ ] **Step 2: Add Alertmanager config to kube-prometheus-stack HelmRelease**

In `kubernetes/monitoring/kube-prometheus-stack/helmrelease.yaml`, add under `alertmanager`:

```yaml
    alertmanager:
      alertmanagerSpec:
        storage:
          volumeClaimTemplate:
            spec:
              storageClassName: local-path
              accessModes: [ReadWriteOnce]
              resources:
                requests:
                  storage: 2Gi
      config:
        global:
          resolve_timeout: 5m
        route:
          group_by: [alertname, job]
          group_wait: 30s
          group_interval: 5m
          repeat_interval: 12h
          receiver: telegram
          routes:
            - receiver: telegram
              matchers:
                - alertname =~ ".*"
        receivers:
          - name: telegram
            telegram_configs:
              - bot_token_file: /etc/alertmanager/secrets/alertmanager-telegram/telegram_bot_token
                chat_id_file: /etc/alertmanager/secrets/alertmanager-telegram/telegram_chat_id
                parse_mode: HTML
                message: |
                  {{ range .Alerts }}
                  <b>{{ .Status | toUpper }} {{ .Labels.alertname }}</b>
                  Severity: {{ .Labels.severity }}
                  {{ .Annotations.summary }}
                  {{ .Annotations.description }}
                  {{ end }}
        templates: []
      alertmanagerSpec:
        secrets:
          - alertmanager-telegram
```

- [ ] **Step 3: Update kustomization.yaml to include the secret**

```yaml
# kubernetes/monitoring/kube-prometheus-stack/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - namespace.yaml
  - alertmanager-secret.sops.yaml
  - helmrelease.yaml
```

- [ ] **Step 4: Commit and push**

```bash
git add kubernetes/monitoring/kube-prometheus-stack/
git commit -m "feat(alerting): configure Alertmanager → Telegram bot with SOPS-encrypted credentials"
git push origin main
```

- [ ] **Step 5: Verify alert delivery**

```bash
flux reconcile kustomization monitoring --with-source

# Check Alertmanager config loaded
kubectl -n monitoring port-forward svc/prometheus-alertmanager 9093:9093 &
# Open http://localhost:9093 → Status → should show telegram receiver
kill %1

# Fire a test alert (will resolve automatically after 1 min)
kubectl -n monitoring port-forward svc/prometheus-alertmanager 9093:9093 &
curl -XPOST http://localhost:9093/api/v2/alerts \
  -H 'Content-Type: application/json' \
  -d '[{"labels":{"alertname":"TestAlert","severity":"warning"},"annotations":{"summary":"Test from homelab"}}]'
# Expected: Telegram message received within 30 seconds
kill %1
```

---

### Task 14: Expose Grafana + Hubble UI via Tailscale

**Files:**
- Create: `kubernetes/monitoring/kube-prometheus-stack/grafana-tailscale.yaml`
- Create: `kubernetes/platform/cilium/hubble-tailscale.yaml`
- Create: `kubernetes/platform/cilium/kustomization.yaml`
- Modify: `kubernetes/monitoring/kube-prometheus-stack/kustomization.yaml`
- Modify: `kubernetes/platform/kustomization.yaml`

**Interfaces:**
- Consumes: Tailscale Operator running (Task 8), MagicDNS + HTTPS enabled in Tailscale admin console
- Produces: `grafana.homelab.ts.net` and `hubble.homelab.ts.net` accessible from any Tailscale device

- [ ] **Step 1: Create Grafana Tailscale Service**

The Tailscale Operator exposes a Service when annotated with `tailscale.com/expose: "true"`:

```yaml
# kubernetes/monitoring/kube-prometheus-stack/grafana-tailscale.yaml
apiVersion: v1
kind: Service
metadata:
  name: grafana-tailscale
  namespace: monitoring
  annotations:
    tailscale.com/expose: "true"
    tailscale.com/hostname: grafana
spec:
  selector:
    app.kubernetes.io/name: grafana
  ports:
    - name: http
      port: 80
      targetPort: 3000
  type: ClusterIP
```

- [ ] **Step 2: Create Hubble UI Tailscale Service**

```yaml
# kubernetes/platform/cilium/hubble-tailscale.yaml
apiVersion: v1
kind: Service
metadata:
  name: hubble-ui-tailscale
  namespace: kube-system
  annotations:
    tailscale.com/expose: "true"
    tailscale.com/hostname: hubble
spec:
  selector:
    k8s-app: hubble-ui
  ports:
    - name: http
      port: 80
      targetPort: 8081
  type: ClusterIP
```

```yaml
# kubernetes/platform/cilium/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - hubble-tailscale.yaml
```

- [ ] **Step 3: Update monitoring and platform kustomization.yaml**

```yaml
# kubernetes/monitoring/kube-prometheus-stack/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - namespace.yaml
  - alertmanager-secret.sops.yaml
  - helmrelease.yaml
  - grafana-tailscale.yaml
```

```yaml
# kubernetes/platform/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - storage
  - cert-manager
  - tailscale
  - cilium
```

- [ ] **Step 4: Commit and push**

```bash
git add kubernetes/monitoring/kube-prometheus-stack/grafana-tailscale.yaml \
        kubernetes/monitoring/kube-prometheus-stack/kustomization.yaml \
        kubernetes/platform/cilium/ \
        kubernetes/platform/kustomization.yaml
git commit -m "feat(access): expose Grafana and Hubble UI via Tailscale hostnames"
git push origin main
```

- [ ] **Step 5: Verify**

```bash
flux reconcile kustomization platform monitoring --with-source

# Check Tailscale admin console → Machines
# Expected: grafana and hubble machines appear

# From any device on your Tailscale network:
curl -sk https://grafana.homelab.ts.net/api/health | jq .
# Expected: {"commit":"...","database":"ok","version":"..."}

# Open https://hubble.homelab.ts.net in browser
# Expected: Hubble network flow UI loads showing pod-to-pod traffic
```

---

## Final Verification Checklist

After all tasks complete:

- [ ] All 3 Talos nodes `STATUS=Ready`: `kubectl get nodes`
- [ ] All Flux Kustomizations `READY=True`: `flux get kustomizations`
- [ ] All HelmReleases `READY=True`: `flux get helmreleases -A`
- [ ] Prometheus scraping cluster metrics: `curl -s http://localhost:9090/api/v1/targets` (via port-forward) shows targets `UP`
- [ ] Loki receiving logs from all namespaces: LogQL `{cluster="homelab"} |= ""` returns results in Grafana
- [ ] Tempo receiving traces (send a test trace via OTel Collector)
- [ ] Alertmanager → Telegram: test alert delivered
- [ ] Grafana accessible at `https://grafana.homelab.ts.net`
- [ ] Hubble UI accessible at `https://hubble.homelab.ts.net`
- [ ] All secrets in git are SOPS-encrypted: `grep -r "AGE-SECRET" kubernetes/` returns nothing

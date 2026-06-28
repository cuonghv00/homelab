# Pattern: Flux CD Deployment — Homelab Reference

> Self-contained reference trích từ repo `homelab` (Flux v2.8.8, Talos+Cilium, 3 nodes).
> Dùng làm input cho session thiết kế, không cần truy cập codebase.

---

## 1. Directory Layout

```
kubernetes/
├── flux/
│   ├── bootstrap/
│   │   ├── flux-system/
│   │   │   ├── gotk-components.yaml   # Flux CRDs + controllers (generated, DO NOT EDIT)
│   │   │   ├── gotk-sync.yaml         # GitRepository + bootstrap Kustomization
│   │   │   └── kustomization.yaml     # Kustomize config gom bootstrap resources
│   │   ├── platform.yaml              # Kustomization CRD → ./kubernetes/platform
│   │   ├── monitoring.yaml            # Kustomization CRD → ./kubernetes/monitoring
│   │   └── kustomization.yaml         # (không dùng — bootstrap Ks trỏ trực tiếp)
│   └── repositories/
│       └── helm/
│           ├── cert-manager.yaml      # HelmRepository
│           ├── prometheus-community.yaml
│           ├── grafana.yaml
│           ├── vector.yaml
│           ├── opentelemetry.yaml
│           ├── local-path.yaml
│           └── kustomization.yaml     # Kustomize config gom HelmRepositories
├── platform/                          # Layer 1: infra platform
│   ├── storage/
│   │   ├── namespace.yaml
│   │   ├── helmrelease.yaml           # local-path-provisioner
│   │   └── kustomization.yaml
│   ├── cert-manager/
│   │   ├── namespace.yaml
│   │   ├── helmrelease.yaml
│   │   └── kustomization.yaml
│   ├── cilium/                        # Chỉ CRDs (L2, LB-IPAM) — Cilium cài qua Talos
│   │   ├── l2-policy.yaml
│   │   ├── lb-pool.yaml
│   │   ├── l2-leases-rbac.yaml
│   │   ├── hubble-lb.yaml
│   │   └── kustomization.yaml
│   └── kustomization.yaml             # Kustomize config gom storage/cert-manager/cilium
└── monitoring/                        # Layer 2: observability
    ├── kube-prometheus-stack/
    │   ├── namespace.yaml
    │   ├── helmrelease.yaml
    │   ├── alertmanager-secret.sops.yaml
    │   ├── grafana-lb.yaml
    │   └── kustomization.yaml
    ├── loki/
    ├── tempo/
    ├── vector/
    │   ├── configmap.yaml             # Pre-rendered — tránh Helm tpl() mangling {{ }}
    │   └── helmrelease.yaml
    ├── otel-collector/
    └── kustomization.yaml
```

---

## 2. Bootstrap Pattern

**`kubernetes/flux/bootstrap/flux-system/gotk-sync.yaml`** — điểm khởi đầu duy nhất:

```yaml
apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata:
  name: flux-system
  namespace: flux-system
spec:
  interval: 1m0s
  ref:
    branch: main
  secretRef:
    name: flux-system          # Secret chứa GitHub deploy token/SSH key
  url: https://github.com/cuonghv00/homelab.git
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: flux-system
  namespace: flux-system
spec:
  interval: 10m0s
  path: ./kubernetes/flux/bootstrap  # Fan-out point
  prune: true
  sourceRef:
    kind: GitRepository
    name: flux-system
  decryption:
    provider: sops
    secretRef:
      name: sops-age             # SOPS decryption key (age)
```

**`kubernetes/flux/bootstrap/flux-system/kustomization.yaml`** — bootstrap fan-out:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - gotk-components.yaml         # Flux controllers
  - gotk-sync.yaml               # GitRepository + bootstrap Ks
  - ../../repositories           # Tất cả HelmRepository
  - ../platform.yaml             # Kustomization CRD cho layer platform
  - ../monitoring.yaml           # Kustomization CRD cho layer monitoring
```

> WHY fan-out qua `resources` thay vì app-of-apps Kustomization: đơn giản hơn, ít indirection,
> dễ debug hơn khi mới bắt đầu. Bootstrap Kustomization tự gom tất cả vào 1 lần apply.

---

## 3. Layer Kustomizations (Flux CRD)

Mỗi layer là 1 `Kustomization` CRD (toolkit CRD, không phải kustomize config) trong `flux-system`:

**`kubernetes/flux/bootstrap/platform.yaml`**:
```yaml
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
    - name: flux-system          # Đợi HelmRepository sẵn sàng
  decryption:
    provider: sops
    secretRef:
      name: sops-age
```

**`kubernetes/flux/bootstrap/monitoring.yaml`**:
```yaml
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: monitoring
  namespace: flux-system
spec:
  interval: 5m0s                 # Shorter: monitoring hay thay đổi hơn platform
  path: ./kubernetes/monitoring
  prune: true
  wait: true
  timeout: 10m0s                 # Longer timeout: stack lớn hơn
  sourceRef:
    kind: GitRepository
    name: flux-system
  dependsOn:
    - name: platform             # Đợi storage + cert-manager trước
  decryption:
    provider: sops
    secretRef:
      name: sops-age
```

---

## 4. Dependency Graph

```
GitRepository (flux-system, 1m poll)
          │
          ▼
Kustomization: flux-system (10m)
  ├── gotk-components (Flux CRDs + controllers)
  ├── HelmRepository × 6 (flux-system ns, 1h each)
  ├── Kustomization: platform (10m) ─── dependsOn: flux-system
  │     ├── storage/        local-path-provisioner
  │     ├── cert-manager/   cert-manager v1.20.3
  │     └── cilium/         L2 + LB-IPAM CRDs
  └── Kustomization: monitoring (5m) ── dependsOn: platform
        ├── kube-prometheus-stack   (HelmRelease, no dependsOn)
        ├── loki                    (HelmRelease, dependsOn: kube-prometheus-stack)
        ├── tempo                   (HelmRelease, dependsOn: kube-prometheus-stack)
        ├── vector                  (HelmRelease, dependsOn: loki)
        └── otel-collector          (HelmRelease, dependsOn: tempo)
```

---

## 5. HelmRepository Pattern

Tất cả HelmRepository trong `flux-system` namespace. HelmRelease ở app namespace reference qua `sourceRef.namespace: flux-system`.

```yaml
apiVersion: source.toolkit.fluxcd.io/v1
kind: HelmRepository
metadata:
  name: cert-manager
  namespace: flux-system
spec:
  interval: 1h
  url: https://charts.jetstack.io
```

---

## 6. HelmRelease Patterns

### 6a. Simple (pinned version, no deps)

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
      version: "v1.20.3"          # Pinned — platform infra ổn định
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
        enabled: false            # Disabled until kube-prometheus-stack cài ServiceMonitor CRD
```

### 6b. Version range + dependsOn

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
      version: ">=1.0.0 <2.0.0"  # Range: nhận patch/minor, chặn major breaking
      sourceRef:
        kind: HelmRepository
        name: grafana
        namespace: flux-system
  values:
    tempo:
      retention: 168h
      resources:
        requests:
          cpu: 50m
          memory: 256Mi
        limits:
          memory: 512Mi
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
        release: prometheus        # Match Prometheus selector label
```

### 6c. existingConfigMaps (tránh Helm tpl() mangling)

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
      version: ">=0.56.0 <0.57.0"  # pre-1.0: minor bumps break config
      sourceRef:
        kind: HelmRepository
        name: vector
        namespace: flux-system
  values:
    role: Agent
    existingConfigMaps:
      - vector-config              # Pre-rendered ConfigMap; tránh {{ }} bị Helm tpl() mangle
    service:
      enabled: false               # Agent-only: push to Loki, không cần inbound
    tolerations:
      - key: node-role.kubernetes.io/control-plane
        operator: Exists
        effect: NoSchedule         # Chạy trên tất cả 3 nodes kể cả control-plane
```

### 6d. Complex (initContainers workaround local-path + fsGroup)

```yaml
# kubernetes/monitoring/kube-prometheus-stack/helmrelease.yaml (trích)
spec:
  chart:
    spec:
      chart: kube-prometheus-stack
      version: ">=87.0.0 <88.0.0"
      sourceRef:
        kind: HelmRepository
        name: prometheus-community
        namespace: flux-system
  values:
    prometheus:
      prometheusSpec:
        storageSpec:
          volumeClaimTemplate:
            spec:
              storageClassName: local-path
              accessModes: [ReadWriteOnce]
              resources:
                requests:
                  storage: 30Gi
        # local-path là hostPath-backed, KHÔNG honor fsGroup.
        # Operator mount qua subPath (root:root 0755), Prometheus (uid 1000) không ghi được.
        # initContainer chown giải quyết. Cần namespace PSA = privileged.
        initContainers:
          - name: init-chown-data
            image: busybox:1.37
            command: ["sh", "-c", "chown -R 1000:2000 /prometheus"]
            securityContext:
              runAsUser: 0
              capabilities:
                add: ["CHOWN"]
                drop: ["ALL"]
    # Talos: disable components không expose endpoint
    kubeControllerManager:
      enabled: false
    kubeScheduler:
      enabled: false
    kubeEtcd:
      enabled: false
    kubeProxy:
      enabled: false
```

---

## 7. SOPS Integration

**`.sops.yaml`** (root repo):

```yaml
creation_rules:
  - path_regex: kubernetes/.*\.sops\.yaml
    encrypted_regex: ^(data|stringData)$
    age: age100gaz55vrmymru2n7a6tfp27m4706wu88scgp3xemlerr9f6lffsrflhw0  # Public key only
  - path_regex: infrastructure/talos/talsecret\.sops\.yaml
    age: age100gaz55vrmymru2n7a6tfp27m4706wu88scgp3xemlerr9f6lffsrflhw0
```

**Encrypted secret file** (`kubernetes/monitoring/kube-prometheus-stack/alertmanager-secret.sops.yaml`):

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: alertmanager-telegram
  namespace: monitoring
data:
  telegram_bot_token: ENC[AES256_GCM,data:...,type:str]
  telegram_chat_id: ENC[AES256_GCM,data:...,type:str]
sops:
  age:
    - enc: |
        -----BEGIN AGE ENCRYPTED FILE-----
        ...
        -----END AGE ENCRYPTED FILE-----
  encrypted_regex: ^(data|stringData)$
  version: 3.13.1
```

**One-time cluster setup** (không commit — gitignored):

```bash
# Tạo age keypair
age-keygen -o homelab.agekey            # Lưu private key an toàn ngoài repo

# Inject secret vào cluster (manual, trước flux bootstrap)
kubectl create secret generic sops-age \
  -n flux-system \
  --from-file=age.agekey=homelab.agekey

# Encrypt file mới
sops --encrypt --in-place kubernetes/monitoring/my-secret.sops.yaml
```

> WHY decryption ở Kustomization level (không phải HelmRelease): Kustomization decrypt trước khi
> render, Secret sẵn sàng trước khi Helm chart được apply. HelmRelease chỉ cần reference Secret
> name — không cần biết về SOPS.

---

## 8. Pod Security Admission (Talos default: baseline)

```yaml
# kubernetes/monitoring/kube-prometheus-stack/namespace.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: monitoring
  labels:
    # node-exporter dùng hostNetwork/hostPath/hostPort
    # Vector dùng hostPath /var/log
    # Prometheus dùng initContainer runAsUser: 0 (chown workaround)
    pod-security.kubernetes.io/enforce: privileged
    pod-security.kubernetes.io/audit: privileged
    pod-security.kubernetes.io/warn: privileged
```

Tương tự cho namespace `local-path-storage` (helper pods dùng hostPath).

---

## 9. Intervals Summary

| Resource | Interval | Lý do |
|---|---|---|
| GitRepository (flux-system) | 1m | Poll git nhanh để apply thay đổi |
| Kustomization: flux-system | 10m | Bootstrap ít thay đổi |
| Kustomization: platform | 10m | Infra ổn định |
| Kustomization: monitoring | 5m | Hay tune, muốn apply nhanh hơn |
| HelmRepository (tất cả) | 1h | Index Helm ít cần thiết |
| HelmRelease (tất cả) | 1h | Upgrade chart chủ động qua git |

---

## 10. Gotchas Đã Gặp

| # | Vấn đề | Giải pháp |
|---|---|---|
| 1 | PSA mặc định Talos = baseline; monitoring/storage pods bị reject | Label namespace `enforce: privileged` |
| 2 | local-path không honor fsGroup; Prometheus crash do subPath root:root | initContainer `chown -R 1000:2000` với `capabilities: add: [CHOWN]` |
| 3 | Vector VRL config có `{{ }}` bị Helm tpl() parse thành Go template | Pre-render ConfigMap, dùng `existingConfigMaps` trong HelmRelease |
| 4 | Alertmanager receivers: Helm REPLACE (không merge) list receivers | Phải khai báo `null` receiver + `telegram` receiver cùng lúc trong values |
| 5 | kube-prometheus-stack target Talos: etcd/scheduler/controller-manager không expose endpoint | `enabled: false` cho `kubeEtcd`, `kubeScheduler`, `kubeControllerManager`, `kubeProxy` |
| 6 | Cilium L2 announcement cần Lease RBAC | Tạo ClusterRole/ClusterRoleBinding cho `cilium` SA với `leases` resource |
| 7 | Control-plane taint: Vector DaemonSet không schedule lên CP node | `tolerations: [{key: node-role.kubernetes.io/control-plane, operator: Exists}]` |

---

## 11. Flux Bootstrap Commands (reference)

```bash
# Install Flux CLI
curl -s https://fluxcd.io/install.sh | sudo bash

# Bootstrap (GitHub — chạy 1 lần, idempotent)
flux bootstrap github \
  --owner=cuonghv00 \
  --repository=homelab \
  --branch=main \
  --path=kubernetes/flux/bootstrap \
  --personal

# Sau bootstrap, inject SOPS secret
kubectl create secret generic sops-age \
  -n flux-system \
  --from-file=age.agekey=homelab.agekey

# Reconcile thủ công
flux reconcile kustomization flux-system --with-source
flux reconcile kustomization platform
flux reconcile kustomization monitoring

# Debug
flux get all -A
flux events --watch
kubectl describe helmrelease <name> -n <ns>
```

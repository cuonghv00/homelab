# Design — Tích hợp Flux CD vào repo `gitops-engine`

- **Ngày:** 2026-06-28
- **Trạng thái:** Design (đầu vào cho project gitops-engine)
- **Output của phase này:** chỉ là tài liệu spec này. Không code, không thay đổi cluster.

---

## Context

Repo `homelab` đã triển khai Flux CD thành công: `GitRepository(flux-system)` trỏ về GitHub, một
bootstrap Kustomization fan-out ra các Kustomization `platform`/`monitoring` (có `dependsOn`), giải mã
secret bằng SOPS + age, và lấy Helm charts qua `HelmRepository` sources. Pattern này ổn định và là
khuôn mẫu để tái dùng.

Repo `gitops-engine` là một **mono-repo riêng** theo pipeline:

> `inputs (operator khai báo)` → `generator (Python, validate + render values)` → `applications/ (output)`

Generator đã được viết bằng Python. Trước đây dự định triển khai theo hướng ArgoCD nên các object
GitOps (Application/HelmRelease...) chưa tồn tại. Mục tiêu của thay đổi này: **thêm lớp Flux GitOps lên
trên pipeline sẵn có**, thay cho hướng ArgoCD cũ, bằng cách mở rộng generator để render thêm các Flux
object, và bootstrap Flux cho chính repo `gitops-engine` theo đúng pattern đã chạy thành công ở homelab.

Kết quả mong muốn: operator chỉ sửa file input → CI validate + render → `applications/` trở thành cây
Flux-consumable → Flux trong cluster reconcile. Một nguồn sự thật, có gác cổng validate, reproducible.

---

## Mục tiêu & phi mục tiêu

**Mục tiêu**
- Mở rộng generator (Python) để render **per-app `HelmRelease`** + **per-project Flux `Kustomization`**.
- Bootstrap Flux cho `gitops-engine` (GitLab-hosted) theo pattern homelab: GitRepository + bootstrap
  Kustomization fan-out + SOPS.
- Validate 3 lớp (input schema, chart values schema, rendered output) chạy cả local CLI lẫn GitLab CI.

**Phi mục tiêu (YAGNI giai đoạn đầu)**
- Không làm policy engine (conftest/OPA).
- Không đẩy charts lên OCI registry (giữ trong repo).
- Không tích hợp/đụng chạm repo `homelab` hiện tại.
- Không tự render trong cluster (Flux chỉ đọc output đã render, không postBuild phức tạp).

---

## Quyết định thiết kế (đã chốt qua brainstorming)

| # | Chủ đề | Quyết định |
|---|---|---|
| 1 | "operator" | Con người, khai báo qua declarative file. |
| 2 | Generator runtime | CLI chạy local + GitLab CI/CD. |
| 3 | Topology | Mono-repo `gitops-engine`, bootstrap Flux mới cho chính repo này (không dính homelab). |
| 4 | Output contract | Custom charts; output `applications/<project>/<app>/{values, shared_values}`. |
| 5 | Input layout | **Hybrid**: `project.yaml` (shared/defaults) + `apps/<app>.yaml` (override). |
| 6 | Validate scope | Lớp 1 (input schema) + lớp 2 (chart values schema) + lớp 3 (rendered output). Bỏ OPA. |
| 7 | Charts location | Trong repo: `charts/<name>/`. HelmRelease tham chiếu chart qua GitRepository path. |
| 8 | Generator tech | Python (đã có sẵn). |
| 9 | Host & source | GitLab; Flux GitRepository trỏ vào chính repo gitops-engine. |
| 10 | Sinh Flux objects | Viết tiếp trong generator dưới dạng **module render riêng**, không tách script. |

---

## Kiến trúc & data flow

```
operator (người)            generator (Python CLI, local + GitLab CI)        Flux (in-cluster)
─────────────────           ──────────────────────────────────────          ──────────────────
inputs/<project>/           1. validate input  (JSON Schema)                 GitRepository → repo
  project.yaml      ──►      2. merge shared+app values                       (GitLab, deploy token)
  apps/<app>.yaml           3. validate chart values (values.schema.json)         │
                            4. render → applications/                              ▼
                            5. validate output (kubeconform + flux build)    bootstrap Kustomization
                                                                              → fan-out 1 Ks/project
                                                                              → HelmRelease/app
                                                                                 (chart: charts/<name>)
```

Ánh xạ tư duy ArgoCD → Flux:
- ArgoCD `Application` (per app) → Flux `HelmRelease` (per app).
- ArgoCD `AppProject` / app-of-apps → Flux `Kustomization` (per project), fan-out từ 1 bootstrap
  Kustomization tĩnh.

---

## Repo layout (mono-repo `gitops-engine`)

```
inputs/
  <project>/
    project.yaml            # shared/defaults, env, chart pins, danh sách app + enable
    apps/<app>.yaml         # params/override riêng từng app
charts/<name>/              # custom Helm charts (mỗi chart kèm values.schema.json)
generator/                  # Python CLI hiện có
  render/flux.py            # MODULE MỚI: render HelmRelease + Kustomization từ template
  templates/                # helmrelease.yaml.j2, kustomization.yaml.j2
  schemas/                  # JSON Schema cho project.yaml & app.yaml
applications/               # OUTPUT render — Flux đọc folder này (generator sinh, KHÔNG sửa tay)
  <project>/
    kustomization.yaml      # gom các app của project
    <app>/
      helmrelease.yaml      # chart → charts/<name> (GitRepository), valuesFrom values bên dưới
      values.yaml           # per-app values đã merge
      shared_values.yaml    # shared values của project
clusters/<cluster>/flux-system/   # bootstrap Flux (gotk-components, gotk-sync) — pattern homelab
  apps.yaml                 # bootstrap Kustomization tĩnh → ./applications (điểm fan-out)
.sops.yaml                  # SOPS creation rule (reuse pattern homelab)
.gitlab-ci.yml              # pipeline validate + render + verify
```

> Lưu ý: `applications/` là **rendered output được commit vào git** (rendered-manifests pattern). Operator
> không sửa tay folder này; mọi thay đổi đi qua `inputs/` → generator.

---

## Thành phần

### 1. Input layer (`inputs/`)
- **`project.yaml`** — phạm vi project: env, pin version chart, defaults/shared values, danh sách app +
  bật/tắt. Map thẳng vào `shared_values` của output.
- **`apps/<app>.yaml`** — phạm vi app: chọn chart nào, override values đè lên shared. Map vào per-app
  `values`.
- Hybrid được chọn vì map 1:1 với 2 tầng giá trị output (shared + per-app), PR diff tách theo app,
  trong khi shared vẫn có chỗ ở tự nhiên ở cấp project.

### 2. Generator — module render Flux (`generator/render/flux.py`)
- Là **stage cuối** của generator, chạy sau khi đã merge + render values.
- Sinh từ template:
  - per-app `helmrelease.yaml`: `chart.spec.sourceRef` → `GitRepository`, path `./charts/<name>`;
    `valuesFrom`/`values` trỏ `values.yaml` + `shared_values.yaml` vừa render.
  - per-project `kustomization.yaml`: gom các app của project.
- Một lệnh `generator render` sinh trọn cây `applications/` (values + Flux objects) **atomically** →
  tránh drift giữa values và HelmRelease tham chiếu chúng.
- Tách bạch logic (module + template riêng, test độc lập được) nhưng nằm trong cùng tool — dùng lại
  input parsing/merge sẵn có thay vì script riêng phải parse lại.

### 3. Flux layer (`clusters/<cluster>/`)
- **GitRepository** → repo gitops-engine trên GitLab; auth bằng deploy token/PAT (`flux bootstrap
  gitlab`).
- **Bootstrap Kustomization tĩnh** (`apps.yaml`) → path `./applications`, `prune: true`, SOPS
  decryption. Đây là điểm fan-out ra từng project Kustomization do generator sinh.
- **Per-project Flux Kustomization** (generated): có `interval`, `prune`, và có thể `dependsOn` giữa
  các project.
- **Secrets:** SOPS + age, reuse `.sops.yaml`; secret `sops-age` trong cluster (giống homelab).

---

## Validation & CI

| Lớp | Nội dung | Công cụ | Chạy ở |
|---|---|---|---|
| 1 | Structural input | JSON Schema (`generator/schemas/`) — field bắt buộc, kiểu, enum env, naming | local CLI + GitLab CI |
| 2 | Chart values | `values.schema.json` mỗi chart / `helm lint` trên values đã render | local CLI + GitLab CI |
| 3 | Rendered output | `kubeconform` + `flux build` / `helm template --dry-run` trên object Flux đã sinh | GitLab CI (chặn merge) |

**`.gitlab-ci.yml`** (3 stage):
1. `validate` — lớp 1 + lớp 2.
2. `render` — chạy generator sinh `applications/`.
3. `verify` — lớp 3 trên output; fail thì chặn merge.

**Local CLI:** cùng các lệnh trên qua `make`/CLI để operator chạy trước khi commit.

---

## Error handling
- Validate fail ở bất kỳ lớp nào → generator exit non-zero, in lỗi rõ (file + field vi phạm); CI chặn
  merge. Không sinh output nửa vời (render là atomic: thành công toàn bộ hoặc không ghi).
- Chart không tồn tại trong `charts/` hoặc version không khớp → fail ở lớp 2/3 trước khi tới cluster.
- Flux reconcile lỗi trong cluster → quan sát qua `flux get/events`; không thuộc phạm vi generator.

---

## Giả định & điểm cần xác nhận khi triển khai
- **Cluster đích:** mặc định cluster Talos homelab hiện tại (cluster duy nhất hiện có). Layout
  `clusters/<cluster>/` để parameterize, đổi/thêm cluster sau dễ.
- `applications/` là rendered output được commit vào git; Flux chỉ đọc, không tự render trong cluster.
- GitLab deploy token/PAT cho Flux và quyền push cho CI render được cấp ngoài git (giữ secret ngoài
  repo, theo pattern homelab).

---

## Tiêu chí hoàn thành (cho project gitops-engine, ngoài phạm vi spec này)
- Operator sửa `inputs/` → `generator render` sinh đúng `applications/<project>/<app>/` gồm values +
  HelmRelease + per-project Kustomization.
- 3 lớp validate chạy được cả local lẫn GitLab CI và chặn merge khi fail.
- `flux bootstrap gitlab` lên cluster → bootstrap Kustomization fan-out, Flux reconcile các HelmRelease
  thành công, giải mã SOPS được.

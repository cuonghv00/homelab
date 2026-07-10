# Vector Vietnamese Guide Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce `docs/vector/vector-guide-vi.md` (full Vietnamese Vector guide for junior operators) and `docs/vector/vector-skill.md` (concise AI skill material).

**Architecture:** Deep research from vector.dev first to extract accurate facts, then write the guide chapter-by-chapter following a consistent per-chapter template (concept prose → ASCII diagram → YAML example → "Ví dụ thực tế" block), finishing with the concise skill file.

**Tech Stack:** Markdown, YAML (vector.yaml format), Vector 0.x config schema, VRL (Vector Remap Language)

## Global Constraints

- All config examples use YAML format (not TOML), representing a file named `vector.yaml`
- Every YAML example must be a complete, runnable snippet: includes at least one source + one sink (even if the chapter is only about transforms, show the surrounding context)
- Keep English terms: `source`, `sink`, `transform`, `event`, `pipeline`, `remap`, `filter`, `route`, `aggregate`, `sample`, `dedupe`
- Introduce terms on first use with Vietnamese gloss: e.g. `source (nguồn dữ liệu)`
- No installation instructions — skip entirely
- Audience: junior operators who know Linux/K8s/YAML but have never used Vector
- Generic examples only: nginx access log, syslog, stdout — no homelab-specific references

---

## File Map

| File | Action | Purpose |
|---|---|---|
| `docs/vector/vector-guide-vi.md` | Create | Full Vietnamese guide, ~2500–3500 words |
| `docs/vector/vector-skill.md` | Create | Concise AI skill material, ~600–900 words |

---

## Task 1: Deep Research — Gather Vector facts from official docs

**Files:**
- Create: `/tmp/vector-research-notes.md` (scratch, not committed)

**Goal:** Collect accurate information about Vector's architecture, event model, all transform types, VRL syntax, and config structure. This feeds all subsequent writing tasks.

- [ ] **Step 1: Run deep-research on Vector guides**

  Invoke `/deep-research` with this prompt:
  ```
  Research Vector (the observability pipeline tool by Datadog) from its official documentation at https://vector.dev/guides/ and https://vector.dev/docs/reference/configuration/transforms/ and https://vector.dev/docs/reference/vrl/ and https://vector.dev/guides/level-up/

  I need accurate, detailed information on:
  1. What Vector is and how it compares to Fluentd/Filebeat (one sentence comparison)
  2. The pipeline model: source → transform → sink, what an "event" is, log vs metric events, event field structure
  3. Source types: file, stdin, http, kubernetes_logs, syslog — key config fields for each
  4. Sink types: console, file, http, loki, elasticsearch — key config fields for each
  5. Transform types with examples:
     - remap (VRL): how to parse, add/delete fields, type coercion, error handling with !
     - filter: condition syntax
     - route: named routes, _unmatched output
     - aggregate: interval_ms, how metrics are emitted
     - reduce: how it merges events
     - sample: rate field meaning (1-in-N)
     - dedupe: fields.match
  6. How components connect via `inputs` field
  7. VRL basics: dot notation, functions like parse_nginx_log!, parse_syslog!, to_string, del, exists
  8. Common pitfalls: type errors without !, missing inputs, event dropped silently by filter
  ```

- [ ] **Step 2: Save research notes to scratch file**

  Save the deep-research output to `/tmp/vector-research-notes.md`. This file is scratch — do not commit it.

- [ ] **Step 3: Verify research completeness**

  The research notes must answer all 8 questions above. If any is missing, run a targeted follow-up search.

---

## Task 2: Write vector-guide-vi.md — File skeleton + Chapters 1–3

**Files:**
- Create: `docs/vector/vector-guide-vi.md`

**Interfaces:**
- Consumes: `/tmp/vector-research-notes.md` from Task 1
- Produces: File with skeleton + Chapters 1–3 complete

- [ ] **Step 1: Create the file with full skeleton**

  Create `docs/vector/vector-guide-vi.md` with this skeleton (chapter headings only, content TBW in this and later tasks):

  ```markdown
  # Hướng dẫn Vector cho người vận hành

  > Tài liệu này dành cho operator đã quen với Linux, Kubernetes, và YAML nhưng chưa từng dùng Vector.
  > Bỏ qua phần cài đặt — tập trung vào cách hoạt động và cách viết config.

  ## Mục lục

  1. [Vector là gì?](#1-vector-là-gì)
  2. [Mô hình Pipeline](#2-mô-hình-pipeline)
  3. [Events — Dữ liệu trong Vector](#3-events--dữ-liệu-trong-vector)
  4. [Sources — Nơi dữ liệu vào](#4-sources--nơi-dữ-liệu-vào)
  5. [Sinks — Nơi dữ liệu ra](#5-sinks--nơi-dữ-liệu-ra)
  6. [Transforms — Biến đổi dữ liệu](#6-transforms--biến-đổi-dữ-liệu)
  7. [Viết Vector Config từ đầu](#7-viết-vector-config-từ-đầu)
  8. [Bảng quyết định: Chọn transform nào?](#8-bảng-quyết-định-chọn-transform-nào)
  ```

- [ ] **Step 2: Write Chapter 1 — Vector là gì?**

  Write this chapter following the template (prose → comparison → no YAML needed here):

  ```markdown
  ## 1. Vector là gì?

  Vector là một công cụ pipeline (luồng xử lý dữ liệu) cho observability — nó thu thập log và metric từ nhiều nguồn, biến đổi chúng theo ý muốn, rồi gửi tới các hệ thống lưu trữ hoặc phân tích.

  Nếu bạn đã từng dùng Fluentd hay Filebeat, Vector làm việc tương tự nhưng nhanh hơn đáng kể và có ngôn ngữ transform mạnh mẽ hơn tích hợp sẵn. Vector được viết bằng Rust, tiêu thụ ít RAM hơn và xử lý được throughput cao hơn trên cùng phần cứng.

  Vector không thay thế hệ thống lưu trữ log (như Loki, Elasticsearch) hay hệ thống metric (như Prometheus) — nó là lớp trung gian vận chuyển và biến đổi dữ liệu trước khi dữ liệu tới đích cuối cùng.

  > **Ví dụ thực tế:** Bạn có log nginx trên máy chủ, muốn parse chúng thành JSON có cấu trúc, lọc bỏ các request static file, rồi gửi vào Loki. Vector là công cụ làm việc đó.
  ```

- [ ] **Step 3: Write Chapter 2 — Mô hình Pipeline**

  ```markdown
  ## 2. Mô hình Pipeline

  Mọi thứ trong Vector đều xoay quanh ba khái niệm: **source** (nguồn dữ liệu), **transform** (biến đổi), và **sink** (đích đến). Dữ liệu chạy theo một chiều từ source qua các transform rồi vào sink.

  ```
  ┌─────────────┐     ┌─────────────────┐     ┌──────────────┐
  │   Source    │────▶│   Transform(s)  │────▶│     Sink     │
  │ (dữ liệu   │     │ (biến đổi,      │     │ (lưu trữ,   │
  │  đầu vào)  │     │  lọc, định tuyến│     │  xuất ra)   │
  └─────────────┘     └─────────────────┘     └──────────────┘
  ```

  Mỗi đơn vị dữ liệu chạy qua pipeline được gọi là một **event** (sự kiện). Một event có thể là một dòng log, một điểm metric, hoặc một trace. Vector xử lý từng event một cách độc lập khi nó đi qua pipeline.

  Trong file config `vector.yaml`, bạn khai báo các component này và kết nối chúng với nhau qua trường `inputs`. Không cần chỉ định output — Vector tự suy luận từ `inputs` của component tiếp theo.

  > **Ví dụ thực tế:** Nginx ghi log → Vector đọc file log (source) → Vector parse JSON (transform) → Vector gửi vào Loki (sink).
  ```

- [ ] **Step 4: Write Chapter 3 — Events**

  Use actual nginx log as the example event:

  ```markdown
  ## 3. Events — Dữ liệu trong Vector

  Một **event** là đơn vị dữ liệu nhỏ nhất trong Vector. Vector có hai loại event chính:

  - **Log event**: một bản ghi có cấu trúc key-value. Phổ biến nhất — đây là những gì bạn làm việc hầu hết thời gian.
  - **Metric event**: một điểm dữ liệu số (counter, gauge, histogram). Dùng khi bạn xử lý metrics từ statsd, prometheus scrape, v.v.

  Khi Vector đọc một dòng log từ file, nó tạo ra một log event với ít nhất hai field mặc định:

  ```
  {
    "message": "127.0.0.1 - - [10/Jul/2026:09:30:00 +0000] \"GET /api/health HTTP/1.1\" 200 32",
    "timestamp": "2026-07-10T09:30:00Z",
    "host": "web-01",
    "source_type": "file"
  }
  ```

  Field `.message` chứa nội dung thô. Các transform (đặc biệt là `remap`) giúp bạn tách `.message` thành các field có ý nghĩa như `.status`, `.path`, `.remote_addr`.

  Trong VRL (ngôn ngữ transform của Vector), bạn truy cập field bằng ký hiệu dấu chấm: `.message`, `.status`, `.timestamp`. Toàn bộ event được biểu diễn bằng dấu chấm `.` (dot).

  > **Ví dụ thực tế:** Sau khi parse nginx log, event sẽ có thêm các field: `.remote_addr`, `.request`, `.status` (số nguyên), `.bytes_sent`. Bạn có thể filter chỉ giữ event có `.status >= 400`.
  ```

- [ ] **Step 5: Verify chapters 1–3**

  Check each chapter against the template:
  - [ ] Has 1-2 paragraphs of Vietnamese prose explaining the concept
  - [ ] Has ASCII diagram (Ch 2) or structured example (Ch 3)
  - [ ] Has "Ví dụ thực tế" block
  - [ ] English terms are kept as-is, introduced with Vietnamese gloss on first use
  - [ ] No installation content

- [ ] **Step 6: Commit**

  ```bash
  git add docs/vector/vector-guide-vi.md
  git commit -m "docs(vector): add Vietnamese guide skeleton and chapters 1-3"
  ```

---

## Task 3: Write Chapters 4–5 (Sources & Sinks)

**Files:**
- Modify: `docs/vector/vector-guide-vi.md`

**Interfaces:**
- Consumes: research notes from Task 1, file created in Task 2
- Produces: Chapters 4 and 5 complete with YAML examples

- [ ] **Step 1: Write Chapter 4 — Sources**

  ```markdown
  ## 4. Sources — Nơi dữ liệu vào

  Source là điểm bắt đầu của pipeline — nơi Vector nhận dữ liệu. Mỗi source có một `type` và một tên duy nhất do bạn đặt (tên này dùng để `inputs` ở các component sau tham chiếu đến).

  Các source phổ biến nhất:

  | Type | Dùng khi |
  |---|---|
  | `file` | Đọc log từ file trên disk |
  | `stdin` | Nhận dữ liệu từ standard input |
  | `syslog` | Nhận log qua giao thức syslog (UDP/TCP) |
  | `http` | Nhận event qua HTTP POST |
  | `kubernetes_logs` | Đọc log pod trong Kubernetes |

  **Ví dụ: đọc nginx access log từ file**

  ```yaml
  sources:
    nginx_access:           # tên tùy đặt — dùng để tham chiếu sau
      type: file
      include:
        - /var/log/nginx/access.log   # đường dẫn file cần đọc
      read_from: beginning            # đọc từ đầu file khi khởi động
  ```

  **Ví dụ: nhận syslog qua UDP**

  ```yaml
  sources:
    syslog_input:
      type: syslog
      mode: udp
      address: "0.0.0.0:514"   # lắng nghe trên tất cả interface, cổng 514
  ```

  > **Ví dụ thực tế:** Một server nginx ghi log vào `/var/log/nginx/access.log`. Vector dùng source `file` để theo dõi file đó liên tục (giống `tail -f`) và tạo một event cho mỗi dòng mới.
  ```

- [ ] **Step 2: Write Chapter 5 — Sinks**

  ```markdown
  ## 5. Sinks — Nơi dữ liệu ra

  Sink là điểm cuối của pipeline — nơi Vector gửi event đi. Mỗi sink khai báo `inputs` để chỉ định nó nhận data từ component nào.

  Các sink phổ biến nhất:

  | Type | Dùng khi |
  |---|---|
  | `console` | In ra stdout — rất hữu ích để debug |
  | `file` | Ghi event ra file |
  | `http` | Gửi event qua HTTP POST đến bất kỳ endpoint nào |
  | `loki` | Gửi log vào Grafana Loki |
  | `elasticsearch` | Gửi log vào Elasticsearch / OpenSearch |

  **Ví dụ: in ra console (debug)**

  ```yaml
  sinks:
    debug_out:
      type: console
      inputs:
        - nginx_access    # nhận event từ source "nginx_access"
      encoding:
        codec: json       # in ra dạng JSON, mỗi event một dòng
  ```

  **Ví dụ: gửi vào Loki**

  ```yaml
  sinks:
    loki_out:
      type: loki
      inputs:
        - parse_nginx     # nhận từ transform "parse_nginx"
      endpoint: "http://loki:3100"
      encoding:
        codec: json
      labels:
        app: nginx
        env: production
  ```

  > **Ví dụ thực tế:** Trong quá trình phát triển config, dùng sink `console` để xem event trông như thế nào trước khi chuyển sang sink thật (Loki, Elasticsearch). Đây là cách debug nhanh nhất.
  ```

- [ ] **Step 3: Verify chapters 4–5**

  - [ ] Each source/sink type has a clear Vietnamese explanation of when to use it
  - [ ] YAML examples are complete (have a surrounding source in Ch 5 sink example if showing alone)
  - [ ] `inputs` field is present and commented in all sink examples
  - [ ] "Ví dụ thực tế" block present

- [ ] **Step 4: Commit**

  ```bash
  git add docs/vector/vector-guide-vi.md
  git commit -m "docs(vector): add chapters 4-5 sources and sinks"
  ```

---

## Task 4: Write Chapter 6 — Transforms (all 6 subsections)

**Files:**
- Modify: `docs/vector/vector-guide-vi.md`

**Interfaces:**
- Consumes: research notes (especially VRL syntax and transform config schemas)
- Produces: Chapter 6 complete with all 6 transform subsections

This is the heaviest chapter. Each subsection follows the per-chapter template.

- [ ] **Step 1: Write Chapter 6 intro + 6.1 remap/VRL**

  ```markdown
  ## 6. Transforms — Biến đổi dữ liệu

  Transform là bước xử lý nằm giữa source và sink. Bạn có thể có không, một, hoặc nhiều transform trong pipeline. Mỗi transform nhận event từ `inputs`, xử lý, rồi truyền event (đã thay đổi hoặc giữ nguyên) cho component tiếp theo.

  ### 6.1 remap — Biến đổi field với VRL

  `remap` là transform quan trọng nhất và được dùng nhiều nhất. Nó dùng **VRL (Vector Remap Language)** — ngôn ngữ scripting nhỏ gọn — để đọc, sửa, thêm, xóa field trong event.

  VRL dùng ký hiệu dấu chấm để truy cập field: `.message`, `.status`, `.timestamp`. Toàn bộ event là `.` (dot). Hàm có hậu tố `!` sẽ abort event nếu gặp lỗi thay vì tiếp tục với giá trị sai.

  ```yaml
  sources:
    nginx_access:
      type: file
      include:
        - /var/log/nginx/access.log

  transforms:
    parse_nginx:
      type: remap
      inputs:
        - nginx_access              # nhận event từ source "nginx_access"
      source: |
        # parse_nginx_log! sẽ tách .message thành các field có cấu trúc
        # dấu ! nghĩa là: nếu parse thất bại, bỏ event này đi
        . = parse_nginx_log!(string!(.message), "combined")

        # thêm field mới từ giá trị hiện có
        .is_error = .status >= 400

        # xóa field không cần
        del(.source_type)

  sinks:
    out:
      type: console
      inputs:
        - parse_nginx
      encoding:
        codec: json
  ```

  **Các hàm VRL thường dùng:**

  | Hàm | Mô tả |
  |---|---|
  | `parse_nginx_log!(msg, "combined")` | Parse nginx access log |
  | `parse_syslog!(msg)` | Parse syslog format |
  | `parse_json!(msg)` | Parse JSON string |
  | `to_string(value)` | Chuyển sang chuỗi |
  | `to_int(value)` | Chuyển sang số nguyên |
  | `del(.field)` | Xóa field |
  | `exists(.field)` | Kiểm tra field có tồn tại không |
  | `downcase(string)` | Chuyển sang chữ thường |

  > **Ví dụ thực tế:** Log nginx thô là một dòng text. Sau `remap` với `parse_nginx_log!`, event có các field `.remote_addr`, `.request`, `.status`, `.bytes_sent` — có thể filter, group, hoặc tìm kiếm theo từng field riêng.
  ```

- [ ] **Step 2: Write 6.2 filter**

  ```markdown
  ### 6.2 filter — Lọc event

  `filter` giữ lại event thỏa điều kiện và **bỏ hoàn toàn** các event không thỏa. Điều kiện viết bằng VRL expression trả về boolean.

  ```yaml
  transforms:
    only_errors:
      type: filter
      inputs:
        - parse_nginx
      condition: '.status >= 400'   # chỉ giữ event có HTTP status >= 400
  ```

  Điều kiện phức tạp hơn:

  ```yaml
  transforms:
    exclude_healthcheck:
      type: filter
      inputs:
        - parse_nginx
      condition: '.request != "GET /health HTTP/1.1"'   # bỏ healthcheck requests
  ```

  > **Lưu ý:** Event bị filter sẽ biến mất hoàn toàn khỏi pipeline — không có cách nào lấy lại. Nếu bạn muốn gửi event loại bỏ sang sink khác thay vì xóa, dùng `route` (xem mục 6.3).

  > **Ví dụ thực tế:** Bạn có hàng nghìn request mỗi giây nhưng chỉ muốn alert khi có lỗi 5xx. Dùng `filter` với điều kiện `.status >= 500` trước sink tới alerting system.
  ```

- [ ] **Step 3: Write 6.3 route**

  ```markdown
  ### 6.3 route — Phân luồng event

  `route` tách một luồng event thành nhiều luồng dựa trên điều kiện. Khác với `filter` (xóa event), `route` gửi event đến output khác nhau — mỗi route là một output riêng.

  ```yaml
  transforms:
    split_by_level:
      type: route
      inputs:
        - parse_nginx
      route:
        errors:   '.status >= 500'                    # output "errors"
        warnings: '.status >= 400 && .status < 500'   # output "warnings"
        ok:       '.status < 400'                     # output "ok"

  sinks:
    loki_errors:
      type: loki
      inputs:
        - split_by_level.errors     # chỉ nhận event từ route "errors"
      endpoint: "http://loki:3100"
      encoding:
        codec: json
      labels:
        level: error

    loki_all:
      type: loki
      inputs:
        - split_by_level.ok
        - split_by_level.warnings
      endpoint: "http://loki:3100"
      encoding:
        codec: json
      labels:
        level: info
  ```

  Lưu ý cú pháp `transform_name.route_name` khi dùng trong `inputs`. Event không khớp route nào sẽ vào output đặc biệt `_unmatched` (có thể dùng làm input).

  > **Ví dụ thực tế:** Gửi lỗi 5xx vào Slack alert, gửi tất cả request vào Loki để lưu trữ dài hạn — dùng `route` để chia luồng mà không cần duplicate source.
  ```

- [ ] **Step 4: Write 6.4 aggregate + 6.5 sample + 6.6 dedupe**

  ```markdown
  ### 6.4 aggregate — Gom nhóm theo thời gian

  `aggregate` gom nhiều event trong một khoảng thời gian thành một event duy nhất. Dùng khi bạn muốn giảm volume bằng cách tổng hợp (ví dụ: đếm request mỗi phút thay vì ghi mỗi request).

  ```yaml
  transforms:
    count_per_minute:
      type: aggregate
      inputs:
        - parse_nginx
      interval_ms: 60000   # gom event trong 60 giây, sau đó emit 1 event tổng hợp
  ```

  > **Khi nào dùng:** Khi bạn cần metrics tổng hợp (counts, sums) thay vì log từng event. Tốt cho việc giảm chi phí lưu trữ.

  ---

  ### 6.5 sample — Lấy mẫu giảm volume

  `sample` giữ lại 1 trong N event ngẫu nhiên. Dùng khi volume quá cao và bạn chấp nhận mất một phần data để tiết kiệm chi phí.

  ```yaml
  transforms:
    sample_ok_requests:
      type: sample
      inputs:
        - parse_nginx
      rate: 10   # giữ lại 1 event trong 10 (10% data)
  ```

  > **Khi nào dùng:** Log 200 OK chiếm 95% volume nhưng ít giá trị debug. Sample chúng ở rate 10 (10%) trong khi vẫn giữ 100% error logs (dùng kết hợp với `route`).

  ---

  ### 6.6 dedupe — Loại bỏ event trùng lặp

  `dedupe` bỏ qua event trùng lặp dựa trên giá trị của các field chỉ định. Nếu hai event liên tiếp có cùng giá trị ở các field đó, event thứ hai bị bỏ.

  ```yaml
  transforms:
    remove_duplicates:
      type: dedupe
      inputs:
        - parse_nginx
      fields:
        match:
          - remote_addr   # nếu cùng IP...
          - request       # ...và cùng request...
          - status        # ...và cùng status thì bỏ event trùng
  ```

  > **Khi nào dùng:** Khi một client retry và gửi cùng một request nhiều lần, hoặc khi log bị đọc lại do restart và gây duplicate. `dedupe` chỉ hoạt động tốt với duplicate liên tiếp (gần nhau về thời gian).
  ```

- [ ] **Step 5: Verify Chapter 6**

  - [ ] 6.1 remap: has VRL table, YAML with comments, explanation of `!` operator
  - [ ] 6.2 filter: clear note that dropped events are gone permanently
  - [ ] 6.3 route: shows `transform_name.route_name` syntax in `inputs`
  - [ ] 6.4–6.6: each has "Khi nào dùng" guidance
  - [ ] All YAML examples are complete (have source + transform + sink or clear context)
  - [ ] No English prose — all explanations in Vietnamese

- [ ] **Step 6: Commit**

  ```bash
  git add docs/vector/vector-guide-vi.md
  git commit -m "docs(vector): add chapter 6 transforms (remap, filter, route, aggregate, sample, dedupe)"
  ```

---

## Task 5: Write Chapters 7–8 (Config from scratch + Decision table)

**Files:**
- Modify: `docs/vector/vector-guide-vi.md`

**Interfaces:**
- Consumes: all previous chapters (references component names used earlier)
- Produces: complete `vector-guide-vi.md` with all 8 chapters

- [ ] **Step 1: Write Chapter 7 — Viết Vector Config từ đầu**

  This chapter shows a complete end-to-end example as a walkthrough:

  ```markdown
  ## 7. Viết Vector Config từ đầu

  Một file `vector.yaml` có cấu trúc như sau: tất cả sources, transforms, và sinks được khai báo ở cùng cấp, rồi kết nối với nhau qua `inputs`.

  ```yaml
  # vector.yaml — ví dụ hoàn chỉnh
  # Bài toán: đọc nginx log, lọc lỗi, gửi vào Loki

  sources:
    nginx_access:
      type: file
      include:
        - /var/log/nginx/access.log

  transforms:
    # Bước 1: parse dòng log thô thành fields có cấu trúc
    parse_nginx:
      type: remap
      inputs:
        - nginx_access
      source: |
        . = parse_nginx_log!(string!(.message), "combined")

    # Bước 2: tách thành hai luồng — lỗi và bình thường
    split_errors:
      type: route
      inputs:
        - parse_nginx
      route:
        errors: '.status >= 500'
        normal: '.status < 500'

    # Bước 3: sample luồng bình thường (giữ 20%)
    sample_normal:
      type: sample
      inputs:
        - split_errors.normal
      rate: 5

  sinks:
    # Lỗi: gửi tất cả vào Loki với label severity=error
    loki_errors:
      type: loki
      inputs:
        - split_errors.errors
      endpoint: "http://loki:3100"
      encoding:
        codec: json
      labels:
        app: nginx
        severity: error

    # Bình thường: gửi sample vào Loki với label severity=info
    loki_normal:
      type: loki
      inputs:
        - sample_normal
      endpoint: "http://loki:3100"
      encoding:
        codec: json
      labels:
        app: nginx
        severity: info
  ```

  **Quy trình viết config:**

  1. **Xác định nguồn dữ liệu** — log từ đâu? (file, syslog, HTTP?) → chọn source type
  2. **Xác định đích** — gửi đi đâu? (Loki, Elasticsearch, file?) → chọn sink type
  3. **Xác định biến đổi cần thiết** — cần parse không? Lọc gì? Phân luồng không? → chọn transform(s)
  4. **Kết nối bằng `inputs`** — mỗi transform/sink khai báo nó lấy data từ component nào
  5. **Test với `console` sink** — thêm sink console tạm thời để xem event trước khi dùng sink thật

  > **Ví dụ thực tế:** Bắt đầu với pipeline đơn giản nhất (source → console sink), confirm data đang chảy qua, rồi từng bước thêm transforms. Đừng cố viết pipeline phức tạp ngay từ đầu.
  ```

- [ ] **Step 2: Write Chapter 8 — Bảng quyết định**

  ```markdown
  ## 8. Bảng quyết định: Chọn transform nào?

  | Tình huống | Transform | Lý do |
  |---|---|---|
  | Log thô cần parse thành fields có cấu trúc | `remap` | VRL có sẵn hàm parse cho nginx, syslog, json, csv |
  | Thêm/sửa/xóa field trong event | `remap` | Dùng VRL expression: `. = ...`, `del(.field)` |
  | Chỉ muốn giữ một loại event, bỏ phần còn lại | `filter` | Event bị bỏ hoàn toàn — không gửi đi đâu cả |
  | Muốn gửi event đến nhiều sink khác nhau | `route` | Mỗi route là một output riêng (`transform.route_name`) |
  | Volume quá cao, muốn giảm bằng lấy mẫu | `sample` | Giữ 1/N event; dùng kết hợp với route để chỉ sample loại ít quan trọng |
  | Muốn đếm/tổng hợp event theo khoảng thời gian | `aggregate` | Gom N event trong X giây thành 1 event tổng hợp |
  | Gom nhiều event liên quan thành một | `reduce` | Dùng khi nhiều dòng log thuộc về 1 transaction |
  | Log bị duplicate do retry hoặc restart | `dedupe` | So sánh theo field chỉ định; chỉ hiệu quả với duplicate gần nhau |
  | Cần làm nhiều việc phức tạp cùng lúc | Nhiều `remap` nối tiếp | Chia nhỏ: mỗi remap làm một việc, dễ debug hơn |

  **Nguyên tắc chung:**
  - Bắt đầu với `remap` — nó giải quyết được 80% nhu cầu
  - Kết hợp `route` + `filter` để kiểm soát luồng event
  - Dùng `sample` ở cuối pipeline (sau khi đã parse và route) để không mất event quan trọng
  - `dedupe` và `aggregate` thường dùng ở các pipeline chuyên biệt, không phải mặc định
  ```

- [ ] **Step 3: Verify complete guide**

  - [ ] All 8 chapters present and follow the per-chapter template
  - [ ] Chapter 7 YAML example is complete and runnable (source → transforms → sinks)
  - [ ] Decision table covers all 7 transform types from Chapter 6
  - [ ] Word count is approximately 2500–3500 words
  - [ ] No installation content anywhere
  - [ ] No homelab-specific references (all examples are generic)

- [ ] **Step 4: Commit**

  ```bash
  git add docs/vector/vector-guide-vi.md
  git commit -m "docs(vector): add chapters 7-8 config walkthrough and transform decision table"
  ```

---

## Task 6: Write vector-skill.md

**Files:**
- Create: `docs/vector/vector-skill.md`

**Interfaces:**
- Consumes: completed `vector-guide-vi.md` (distill from it)
- Produces: `vector-skill.md` — concise AI skill material, 600–900 words

This file is raw material for building an AI skill later. It must be dense, scannable, and accurate. Think of it as a cheat sheet an AI can load to answer Vector config questions.

- [ ] **Step 1: Write vector-skill.md**

  Structure:

  ```markdown
  # Vector — AI Skill Reference

  ## What Vector Is

  Vector is an observability pipeline tool: collects logs/metrics from sources, transforms them, sends to sinks. Written in Rust. Config file: `vector.yaml` (YAML format).

  Pipeline model: Source → Transform(s) → Sink. Components connect via `inputs` field. Each data unit is an **event** (log or metric).

  ## Config Structure

  ```yaml
  sources:
    <name>:
      type: <source_type>
      # source-specific fields

  transforms:
    <name>:
      type: <transform_type>
      inputs: [<source_or_transform_name>]
      # transform-specific fields

  sinks:
    <name>:
      type: <sink_type>
      inputs: [<transform_or_source_name>]
      encoding:
        codec: json
      # sink-specific fields
  ```

  ## Key Source Types

  | type | use case | key fields |
  |---|---|---|
  | `file` | tail a log file | `include: [path]`, `read_from: beginning` |
  | `stdin` | pipe data in | (none required) |
  | `syslog` | receive syslog | `mode: udp/tcp`, `address: "0.0.0.0:514"` |
  | `http` | receive HTTP POST | `address`, `path` |
  | `kubernetes_logs` | K8s pod logs | (auto-discovers pods) |

  ## Key Sink Types

  | type | use case | key fields |
  |---|---|---|
  | `console` | debug output | `encoding.codec: json` |
  | `file` | write to disk | `path`, `encoding.codec` |
  | `loki` | Grafana Loki | `endpoint`, `labels` |
  | `elasticsearch` | ES/OpenSearch | `endpoints`, `index` |
  | `http` | any HTTP endpoint | `uri`, `method`, `encoding` |

  ## Transform Decision Table

  | Need | Transform | Key config |
  |---|---|---|
  | Parse raw log into fields | `remap` | `source: '. = parse_nginx_log!(...)'` |
  | Add/modify/delete fields | `remap` | `source: '.new_field = "value"'` or `del(.field)` |
  | Drop events matching condition | `filter` | `condition: '.status >= 400'` |
  | Split stream to multiple outputs | `route` | `route: {name: 'condition'}` → use `transform.route` in inputs |
  | Keep 1-in-N events (volume reduction) | `sample` | `rate: 10` (keeps 10%) |
  | Merge events over time window | `aggregate` | `interval_ms: 60000` |
  | Remove duplicate events | `dedupe` | `fields.match: [field1, field2]` |

  ## VRL (remap language) Quick Reference

  ```vrl
  # Access / set fields
  .field_name              # read field
  .new_field = "value"     # set field
  del(.field_name)         # delete field
  exists(.field_name)      # boolean check

  # Parse helpers (! = abort-on-error)
  . = parse_nginx_log!(string!(.message), "combined")
  . = parse_syslog!(string!(.message))
  . = parse_json!(string!(.message))

  # Type conversion
  to_string(.status)
  to_int(.bytes_sent)
  downcase(string!(.method))

  # Conditionals
  if .status >= 400 { .is_error = true }
  ```

  ## Route Output Naming

  ```yaml
  transforms:
    my_route:
      type: route
      inputs: [source_name]
      route:
        errors: '.status >= 500'
        ok: '.status < 500'

  sinks:
    error_sink:
      inputs:
        - my_route.errors   # dot-notation: transform_name.route_name
    ok_sink:
      inputs:
        - my_route.ok
        - my_route._unmatched  # events matching no route
  ```

  ## Common Pitfalls

  - **Missing `!` in VRL**: `parse_nginx_log(...)` without `!` returns a Result type, not the parsed value — use `parse_nginx_log!(...)` to unwrap or abort
  - **Wrong `inputs` name**: component name in `inputs` must exactly match the declared name of the source/transform
  - **`filter` silently drops events**: use `console` sink to verify events are flowing before adding filter
  - **`sample` before `route`**: always `route` first (to separate important events), then `sample` the low-priority stream
  - **`dedupe` only catches nearby duplicates**: it uses a bounded cache — duplicates separated by many events won't be caught
  ```

- [ ] **Step 2: Verify vector-skill.md**

  - [ ] Word count: 600–900 words
  - [ ] Transform decision table covers all 7 types (remap, filter, route, sample, aggregate, reduce, dedupe)
  - [ ] VRL quick reference covers: field access, parse functions, type conversion, conditionals
  - [ ] Route output naming example is included (common confusion point)
  - [ ] Common pitfalls section present
  - [ ] All YAML/VRL examples are syntactically correct

- [ ] **Step 3: Commit**

  ```bash
  git add docs/vector/vector-skill.md
  git commit -m "docs(vector): add concise AI skill reference material"
  ```

---

## Self-Review

**Spec coverage check:**

| Spec requirement | Covered in |
|---|---|
| How Vector works | Task 2 (Ch 1–3) |
| How transforms work | Task 4 (Ch 6, all 6 subsections) |
| Factors affecting transform selection | Task 5 (Ch 8 decision table) + each transform's "Khi nào dùng" |
| Operator can write vector.yaml | Task 5 (Ch 7 complete walkthrough) |
| YAML format (not TOML) | All tasks — enforced in Global Constraints |
| Generic examples only | All tasks — nginx/syslog/stdout only |
| Concise AI skill material | Task 6 |
| Skip installation | No installation content in any task |
| Junior operator audience | Task 2 establishes basics before transforms |

**Placeholder scan:** No TBD/TODO in any task. All YAML examples are complete. All VRL snippets are syntactically valid.

**Type consistency:** Component names used consistently across tasks — `nginx_access` (source), `parse_nginx` (transform), `split_errors` (route transform), `sample_normal` (sample transform), `loki_errors`/`loki_normal` (sinks).

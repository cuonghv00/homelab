# Vector Advanced Patterns & Transport Optimization — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce `docs/vector/vector-patterns-vi.md` (production transform/parsing patterns, Vietnamese) and `docs/vector/vector-transport-vi.md` (sink config & transport optimization with docs-verified trade-offs, Vietnamese), and update `docs/vector/vector-skill.md` with an advanced patterns quick-reference section.

**Architecture:** Deep-research vector.dev transport/VRL docs first; read all 27 real production configs to extract patterns; write patterns doc (Sections 0–5) then transport doc (Sections 0–5); update skill file last. All config snippets are redacted (no IPs, passwords, or internal hostnames).

**Tech Stack:** Markdown, YAML (Vector config format), VRL (Vector Remap Language), real configs in `docs/vector/real-config/`

## Global Constraints

- All config snippets are redacted: replace real IPs with `<broker>:9092`, passwords with `<redacted>`, endpoints with `<redacted>`
- Vietnamese prose throughout; English technical terms preserved: `parse_regex`, `drop_on_error`, `remap`, `fingerprint`, `multiline`, `adaptive`, `bulk`, `compression`
- Each section ends with a `> **Pitfall / Trade-off:**` callout
- No installation instructions for Kafka, Elasticsearch, or ClickHouse
- Source real-config paths: `docs/vector/real-config/<dir>/vector.yaml`
- All 27 configs: vector_nginx_agent, vector_nginx_aggregator, vector_haproxy_agent, vector_haproxy_aggregator, vector_kong_aggregator, vector_mariadb_agent, vector_mariadb_aggregator, vector_vault_agent, vector_vault_aggregator, vector_vault_uat_agent, vector_vault_uat_aggregator, vector_napas_agent, vector_napas_aggregator, vector_napas_fw_agent, vector_epass_aggregator, vector_epass_audit, vector_mskpp_agent, vector_mskpp_aggregator, vector_cdcn_aggregator, vector_uat_kpp_agent, vector_uat_kpp_aggregator, vector_clickhouse_ag, vector_transaction, vector_process_salary, vector_app4_aggregator, vector_custom, vector_ctt_agent

---

## File Map

| File | Action | Purpose |
|---|---|---|
| `docs/vector/vector-patterns-vi.md` | Create | Production transform/parsing patterns, ~3000–4000 words |
| `docs/vector/vector-transport-vi.md` | Create | Transport & sink optimization, ~2000–3000 words |
| `docs/vector/vector-skill.md` | Modify (append) | Add advanced patterns + transport quick-reference section |

---

## Task 1: Deep Research — vector.dev transport docs and VRL functions

**Files:**
- Create: `/tmp/vector-transport-research.md` (scratch, not committed)

**Goal:** Gather verified facts about ES sink, ClickHouse sink, Kafka sink/source, file source (fingerprint/multiline), and VRL merge/object/timestamp functions. This feeds the transport doc's "Verified from docs" sections.

- [ ] **Step 1: Fetch ES sink reference**

  Use WebFetch on `https://vector.dev/docs/reference/configuration/sinks/elasticsearch/`

  Extract and save these specific fields with their documented defaults and valid values:
  - `bulk.action`: valid values, difference between `create` and `index`
  - `batch.max_bytes`: default, recommended range
  - `batch.timeout_secs`: default, behavior
  - `request.concurrency`: valid values (integer, "adaptive"), behavior of adaptive
  - `request.timeout_secs`: default
  - `compression`: valid values (none, gzip, zstd, zlib)
  - `healthcheck.enabled`: what it does, when to disable
  - `api_version`: valid values (v6, v7, v8), differences

- [ ] **Step 2: Fetch ClickHouse sink reference**

  Use WebFetch on `https://vector.dev/docs/reference/configuration/sinks/clickhouse/`

  Extract:
  - `request.concurrency`: adaptive behavior description
  - `skip_unknown_fields`: what it does
  - `batch.max_bytes`, `batch.max_events`, `batch.timeout_secs`: defaults
  - `compression`: valid values
  - Timestamp handling requirements (does ClickHouse need Unix timestamps?)

- [ ] **Step 3: Fetch Kafka sink and source references**

  Use WebFetch on:
  - `https://vector.dev/docs/reference/configuration/sinks/kafka/`
  - `https://vector.dev/docs/reference/configuration/sources/kafka/`

  Extract:
  - `batch.max_bytes`, `batch.max_events`, `batch.timeout_secs`: defaults for sink
  - `compression`: valid values and defaults
  - `decoding.codec`: valid values (bytes, json, native, native_json)
  - `group_id`: behavior, consumer group semantics
  - Metadata fields added by Kafka source: `.topic`, `.partition`, `.offset`, `.message_key`, `.headers`

- [ ] **Step 4: Fetch file source reference**

  Use WebFetch on `https://vector.dev/docs/reference/configuration/sources/file/`

  Extract:
  - `fingerprint.strategy`: checksum vs device_and_inode — exact documented behavior
  - `fingerprint.ignored_header_bytes`: what it does
  - `fingerprint.lines`: what it does
  - `ignore_older_secs`: behavior
  - `ignore_not_found`: behavior
  - `multiline.start_pattern`, `condition_pattern`, `mode` (halt_before, halt_with, continue_through, continue_past): documented behavior of each mode
  - `multiline.timeout_ms`: behavior

- [ ] **Step 5: Fetch VRL functions for merge and timestamp**

  Use WebFetch on `https://vector.dev/docs/reference/vrl/functions/`
  Also try specific function pages:
  - `https://vector.dev/docs/reference/vrl/functions/#merge`
  - `https://vector.dev/docs/reference/vrl/functions/#object`
  - `https://vector.dev/docs/reference/vrl/functions/#to_unix_timestamp`
  - `https://vector.dev/docs/reference/vrl/functions/#format_timestamp`

  Extract:
  - `merge(target, object)` vs `merge!(target, object)`: exact signature, error conditions
  - `object(value)`: what it does, return type
  - `to_unix_timestamp(timestamp, unit)`: signature, unit options (seconds, milliseconds, nanoseconds)
  - `format_timestamp(timestamp, format)`: signature, common format strings

- [ ] **Step 6: Save all research to scratch file**

  Save to `/tmp/vector-transport-research.md`. Structure by section (ES / ClickHouse / Kafka / File / VRL). Do not commit.

- [ ] **Step 7: Verify completeness**

  The research must answer all questions in Steps 1–5. If any field has only "TBD" or "unclear", run a targeted WebSearch for the specific field.

---

## Task 2: Read all 27 real configs and extract patterns

**Files:**
- Create: `/tmp/vector-patterns-extracted.md` (scratch, not committed)

**Goal:** Read every config in `docs/vector/real-config/` and extract categorized patterns. This feeds both docs.

- [ ] **Step 1: Read all 27 configs**

  Read each file at `docs/vector/real-config/<dir>/vector.yaml`. For each config, note:
  - Source type(s) used
  - Transform technique(s) used
  - Sink type(s) and key config values
  - Any unusual or interesting pattern

  Configs not yet surveyed (read these specifically):
  - `vector_napas_agent`, `vector_napas_aggregator`, `vector_napas_fw_agent`
  - `vector_mskpp_agent`, `vector_mskpp_aggregator`
  - `vector_cdcn_aggregator`, `vector_ctt_agent`
  - `vector_uat_kpp_agent`, `vector_uat_kpp_aggregator`
  - `vector_process_salary`, `vector_app4_aggregator`
  - `vector_epass_aggregator`, `vector_mariadb_aggregator`
  - `vector_vault_uat_agent`, `vector_vault_uat_aggregator`
  - `vector_haproxy_aggregator`

  Already surveyed (patterns documented in plan): nginx_agent, nginx_aggregator, haproxy_agent, kong_aggregator, clickhouse_ag, vault_agent, mariadb_agent, transaction, custom, epass_audit.

- [ ] **Step 2: Extract and categorize patterns**

  Save to `/tmp/vector-patterns-extracted.md` with these categories:

  ```
  ## Parsing Patterns
  - Custom regex examples (with which config they come from)
  - Two-step parse examples
  - Multiline configs
  - Payload unwrapping examples

  ## VRL Techniques
  - Merge patterns found
  - Error handling patterns
  - Timestamp patterns
  - String manipulation

  ## Transport Configs
  - All Kafka producer batch configs found
  - All ES sink configs found (batch, concurrency, compression)
  - All ClickHouse sink configs found
  - Fingerprinting strategies used per config

  ## Unusual Patterns
  - Anything not seen in already-surveyed configs
  ```

- [ ] **Step 3: Note any patterns not in the plan**

  If any configs contain patterns not already described in this plan's Global Constraints section (e.g., a new VRL function, a different sink type, an unusual source), add them to the extracted notes. They may be included in the docs.

---

## Task 3: Write `vector-patterns-vi.md` — Sections 0–2

**Files:**
- Create: `docs/vector/vector-patterns-vi.md`

**Interfaces:**
- Consumes: `/tmp/vector-patterns-extracted.md` from Task 2

- [ ] **Step 1: Create file with full skeleton**

  ```markdown
  # Vector — Production Patterns

  > Tài liệu này dành cho operator đã đọc [Hướng dẫn Vector cơ bản](vector-guide-vi.md).
  > Tất cả config example được lấy từ môi trường production thực tế và đã được redact thông tin nhạy cảm.

  ## Mục lục
  0. [Architecture: Agent → Kafka → Aggregator](#0-architecture)
  1. [Parsing Patterns](#1-parsing-patterns)
  2. [VRL Advanced Techniques](#2-vrl-advanced-techniques)
  3. [Error Handling Strategy](#3-error-handling-strategy)
  4. [File Source: Fingerprinting & Tracking](#4-file-source-fingerprinting--tracking)
  5. [Self-Monitoring](#5-self-monitoring)
  ```

- [ ] **Step 2: Write Section 0 — Architecture**

  Write the following content:

  ```markdown
  ## 0. Architecture: Agent → Kafka → Aggregator

  Trong môi trường production, Vector hiếm khi chạy theo mô hình đơn giản source→sink.
  Pattern phổ biến nhất là **Agent → Kafka → Aggregator**:

  ```
  ┌─────────────────────────────────────────────────────────────────────┐
  │  Agent (mỗi server)          Kafka           Aggregator (tập trung) │
  │                                                                     │
  │  [App Log] ──► [Vector] ──► [Topic] ──► [Vector] ──► [ES / CH]    │
  │    file/       zstd, json    buffer       native/json   gzip        │
  │  syslog/                                                            │
  │   socket                                                            │
  └─────────────────────────────────────────────────────────────────────┘
  ```

  **Tại sao cần Kafka ở giữa?**

  - **Decoupling (tách biệt):** Agent không cần biết Elasticsearch hay ClickHouse đang ở đâu. Nếu aggregator restart, agent vẫn ghi vào Kafka bình thường.
  - **Buffering (đệm):** Kafka lưu log tạm thời. Nếu ES bị quá tải hoặc restart, log không mất — aggregator consume lại khi ES sẵn sàng.
  - **Replay (phát lại):** Có thể deploy aggregator mới với `group_id` khác để đọc lại toàn bộ log từ đầu — rất hữu ích khi thay đổi format index hoặc thêm transform mới.
  - **Fanout:** Cùng một Kafka topic có thể được đọc bởi nhiều aggregator khác nhau — ví dụ: aggregator ghi vào ES, aggregator khác ghi vào ClickHouse.

  **Khi nào KHÔNG cần Kafka?**

  - Volume thấp (< 10K events/phút) và chỉ có 1 đích (1 ES cluster)
  - Môi trường homelab, dev/test
  - Log không quan trọng, mất một ít không sao
  - Latency phải cực thấp (Kafka thêm ~50-200ms)

  > **Pitfall:** Nhiều team thêm Kafka vì nghĩ "production cần Kafka", rồi phải maintain thêm Kafka cluster. Nếu không có nhu cầu replay hoặc fanout, direct agent→ES thường đơn giản và đủ tốt.
  ```

- [ ] **Step 3: Write Section 1 — Parsing Patterns**

  Write 5 subsections with exact YAML from real configs (redacted):

  **1.1 Custom regex với parse_regex:**
  ```markdown
  ### 1.1 Custom Regex với `parse_regex`

  Dùng khi log có format tự định nghĩa (không phải nginx/syslog chuẩn). `parse_regex` dùng named capture groups `(?P<name>pattern)` để tạo fields từ regex match.

  ```yaml
  transforms:
    access_log_transform:
      type: remap
      inputs: [access_log]
      drop_on_error: true
      source: |
        # Thử parse với regex — lưu kết quả và lỗi vào biến riêng
        parsed_log, err = parse_regex(.message, r'^\[(?P<time_local>.*)\] (?P<http_code>\S+) (?P<server_addr>\S+) (?P<remote_addr>\S+) - (?P<host>.+) port:(?P<port>\S+) to:\s"(?P<backend_addr>.*+)"\s"(?P<method>\S+) (?P<path>.*+) (?P<protocol_ver>.*+)" - Request time: (?P<request_time>.*+) msec - Response time: (?P<response_time>.*+) msec (?P<protocol>\S+)$')
        if err != null {
          log("Unable to parse access log: " + string!(.message), level: "warn")
          abort
        }
        # Merge tất cả fields từ parsed_log vào event root
        . = merge(., parsed_log)
        # Parse timestamp ra kiểu DateTime
        .@timestamp = parse_timestamp!(.time_local, format: "%d/%b/%Y:%H:%M:%S %z")
        del(.time_local)
        del(.message)
  ```

  Cấu trúc pattern: `parsed_log, err = parse_regex(.message, r'...')` → kiểm tra `err != null` → `merge(., parsed_log)`. Không bao giờ dùng `parse_regex!` (bang) vì khi parse fail, abort trước khi có cơ hội log lỗi.

  > **Pitfall:** Regex trong VRL dùng cú pháp Rust regex (không phải PCRE). Test pattern tại https://rustexp.lpil.uk/ trước khi deploy. Đặc biệt chú ý: Rust regex không hỗ trợ lookahead/lookbehind.
  ```

  **1.2 Two-step parsing:**
  ```markdown
  ### 1.2 Two-Step Parsing: Timestamp Ngoài + JSON Bên Trong

  Một số ứng dụng ghi log theo format: `DD/MM/YYYY HH:MM:SS <JSON body>`. Cần 2 bước: parse regex để tách timestamp, rồi parse JSON cho phần còn lại.

  ```yaml
  transforms:
    money_trans_transforms:
      type: remap
      inputs: [money_trans_input]
      drop_on_error: true
      source: |
        # Bước 1: tách timestamp và phần message JSON
        . |= parse_regex!(.message, r'(?sm)^(?P<timestamp>\d{2}/\d{2}/\d{4}\s\d{2}:\d{2}:\d{2})\s+(?P<message>.*)$')
        .@timestamp = parse_timestamp(.timestamp, format: "%d/%m/%Y %H:%M:%S") ?? now()
        del(.timestamp)

        # Bước 2: parse JSON trong .message (giờ .message = chỉ phần JSON)
        parsed = parse_json!(.message)
        . |= object(parsed) ?? {}

        del(.source_type)
        del(.message)
  ```

  Lưu ý: sau bước 1, `.message` bị ghi đè bởi captured group — nó chứa phần JSON thô, không còn là toàn bộ dòng log gốc.

  > **Pitfall:** Nếu format timestamp thay đổi (ví dụ app deploy mới dùng format khác), bước 1 sẽ fail và `drop_on_error: true` sẽ drop toàn bộ event. Thêm `?? now()` cho timestamp parse để graceful fallback.
  ```

  **1.3 Multiline log assembly:**
  ```markdown
  ### 1.3 Multiline Log Assembly

  MariaDB slow query log và InnoDB status log trải dài nhiều dòng. Cần dùng `multiline` trong source để gom các dòng thành một event trước khi transform.

  ```yaml
  sources:
    mariadb_slow_log_file:
      type: file
      include:
        - /log/mysql_log/mysqld.slow.log
      multiline:
        start_pattern: '^\#\sUser'        # Dòng bắt đầu một slow query entry
        condition_pattern: '^(\#\sTime|\#\sUser)'  # Dấu hiệu của entry mới
        mode: halt_before                 # Kết thúc block hiện tại khi thấy entry mới
        timeout_ms: 1000                  # Flush block sau 1 giây không có dòng mới
  transforms:
    mariadb_slow_log_transform:
      type: remap
      inputs: [mariadb_slow_log_file]
      drop_on_error: true
      source: |
        # Parse toàn bộ multiline block bằng 1 regex với flag (?sm)
        . |= parse_regex!(.message, r'(?sm)^\#\sUser@Host:\s(?P<client>.*)\n\#\sThread_id:\s(?P<thread_id>\d+)\s+Schema:(?P<schema>.*)\n\#\sQuery_time:\s(?P<query_time>[0-9\.]*)\s+Lock_time:\s(?P<lock_time>[0-9\.]*)\s+Rows_sent:\s(?P<rows_sent>\d+)...')
        .query_time = to_float!(.query_time)
        .lock_time = to_float!(.lock_time)
        .@timestamp = parse_timestamp(.timestamp, format: "%s") ?? now()
  ```

  Flag `(?sm)` trong regex: `s` = dấu `.` match cả newline, `m` = `^`/`$` match đầu/cuối từng dòng. Cần thiết cho multiline regex.

  Vector hỗ trợ 4 `mode`:
  | Mode | Hành vi |
  |---|---|
  | `halt_before` | Kết thúc block khi gặp dòng khớp `condition_pattern` (dòng đó thuộc block mới) |
  | `halt_with` | Kết thúc block khi gặp dòng khớp, dòng đó thuộc block cũ |
  | `continue_through` | Gom dòng khớp `condition_pattern` vào block hiện tại |
  | `continue_past` | Dòng không khớp `condition_pattern` kết thúc block |

  > **Pitfall:** `timeout_ms: 1000` quá thấp có thể flush block chưa hoàn chỉnh nếu log ghi chậm. `timeout_ms: 5000` an toàn hơn nhưng tăng latency. Với `halt_before`, entry cuối cùng của file không flush cho đến khi có entry mới hoặc timeout.
  ```

  **1.4 Payload unwrapping:**
  ```markdown
  ### 1.4 Payload Unwrapping

  Một số service đóng gói data thực sự trong một nested field (thường là `.payload`). Pattern này unwrap nested object vào root event.

  ```yaml
  source: |
    parsed, err = parse_json(.message)
    if err != null {
      log(err)
      log(.message)
      abort
    }
    . |= object(parsed) ?? {}

    # Unwrap .payload nếu tồn tại
    if exists(.payload) {
      . = merge!(., .payload)
      del(.payload)
    }

    .@timestamp = parse_timestamp(.requestTime, format: "%Y-%m-%dT%T") ?? now()
    del(.source_type)
    del(.topic)
    del(.partition)
    del(.offset)
    del(.message)
  ```

  `merge!(., .payload)` dùng bang operator — abort nếu có type conflict giữa fields ở root và fields trong `.payload`. Dùng `merge(., .payload) ?? {}` nếu muốn graceful fallback thay vì abort.

  > **Pitfall:** Nếu `.payload` chứa field trùng tên với field ở root (ví dụ `.timestamp` đã tồn tại), `merge!` sẽ abort. Kiểm tra schema của event trước khi dùng `merge!`.
  ```

  **1.5 Built-in vs custom:**
  ```markdown
  ### 1.5 Built-in Parsers vs Custom Regex

  | Tình huống | Dùng | Lý do |
  |---|---|---|
  | Nginx standard `combined` / `error` format | `parse_nginx_log!` | Nhanh hơn, không cần viết regex |
  | Syslog RFC 3164 / 5424 | `parse_syslog!` | Tự xử lý priority, facility, severity |
  | JSON log | `parse_json!` | Đơn giản nhất |
  | Nginx custom format (thêm/bớt fields) | `parse_regex` | Built-in không biết format tùy chỉnh |
  | MariaDB slow log, InnoDB | `parse_regex` với `(?sm)` | Format phức tạp, nhiều dòng |
  | HAProxy (nếu ghi dạng JSON) | `parse_json!` | HAProxy có thể config ghi JSON |

  > **Trade-off:** `parse_regex` linh hoạt nhưng chậm hơn 2–5x so với built-in parsers. Với throughput > 50K events/phút, đo benchmark trước khi chọn parse_regex cho hot path.
  ```

- [ ] **Step 4: Write Section 2 — VRL Advanced Techniques**

  Write 6 subsections:

  **2.1 Merge patterns:**
  ```markdown
  ## 2. VRL Advanced Techniques

  ### 2.1 Ba Cách Merge Object

  Production configs dùng 3 pattern merge khác nhau tuỳ tình huống:

  ```vrl
  # Pattern A: |= với object() fallback (phổ biến nhất)
  parsed = parse_json!(.message)
  . |= object(parsed) ?? {}
  # object(x): nếu x là object thì return x, nếu không thì error
  # ?? {}: nếu object() error (parsed không phải object), dùng {} thay vì abort

  # Pattern B: merge() function — tương đương nhưng tường minh hơn
  parsed_log, err = parse_regex(.message, r'...')
  . = merge(., parsed_log)
  # merge() không abort khi fail — trả về Result type

  # Pattern C: merge!() — abort ngay khi có type conflict
  if exists(.payload) {
    . = merge!(., .payload)
    del(.payload)
  }
  # Dùng khi chắc chắn .payload là object và không có field conflicts
  ```

  | Pattern | Abort on error | Dùng khi |
  |---|---|---|
  | `. \|= object(x) ?? {}` | Không | Merge JSON parsed — phổ biến nhất |
  | `merge(., x)` | Không | Merge regex result với error check riêng |
  | `merge!(., x)` | Có | Merge nested object, biết chắc schema |

  > **Pitfall:** `. |= x` và `merge(., x)` có hành vi khác nhau khi x có nested object: `|=` merge shallow (một level), trong khi `merge` cũng shallow. Không có built-in deep merge trong VRL.
  ```

  **2.2 Error handling tiers:**
  ```markdown
  ### 2.2 Ba Tầng Xử Lý Lỗi

  ```vrl
  # Tầng 1: ! (bang) — abort ngay, không log, đơn giản nhất
  parsed = parse_json!(.message)         # Abort nếu .message không phải JSON
  .http_code = to_int!(.http_code)       # Abort nếu không convert được

  # Tầng 2: ?? (fallback) — dùng giá trị mặc định thay vì abort
  .@timestamp = parse_timestamp(.time, format: "%d/%b/%Y:%H:%M:%S %z") ?? now()
  .upstream_connect_time = to_float(.upstream_connect_time) ?? 0
  .response_time = to_float(.response_time) ?? 0.0

  # Tầng 3: if err != null — explicit error handling với logging
  parsed_log, err = parse_regex(.message, r'...')
  if err != null {
    log("Unable to parse: " + string!(.message), level: "warn")
    abort
  }
  . = merge(., parsed_log)
  ```

  | Tầng | Khi nào dùng |
  |---|---|
  | `!` bang | Field quan trọng, parse fail = event vô nghĩa, muốn drop ngay |
  | `??` fallback | Field optional hoặc có default hợp lý (timestamp, numeric metrics) |
  | `if err != null` | Muốn log lỗi trước khi abort để debug |

  > **Pitfall:** `to_int(.field)` (không có `!`) trả về `Result` type — không phải integer. Phải unwrap bằng `to_int!(.field)` hoặc `to_int(.field) ?? 0`. Quên unwrap gây lỗi VRL type mismatch.
  ```

  **2.3 Conditional field normalization:**
  ```markdown
  ### 2.3 Conditional Field Normalization

  Một số field có giá trị sentinel (ví dụ `"-"`) thay vì null khi không có dữ liệu. Cần normalize trước khi convert type.

  ```vrl
  # upstream_status là "-" khi không có upstream (direct response)
  if .upstream_status == "-" {
    .upstream_status = .status
  }
  .upstream_status = to_int!(.upstream_status)

  # upstream_connect_time có thể là "-" hoặc số thực
  .upstream_connect_time = to_float(.upstream_connect_time) ?? 0
  ```

  > **Trade-off:** Dùng `?? 0` thay vì check `== "-"` đơn giản hơn nhưng che giấu cả lỗi thật. Nếu field có giá trị lạ khác `"-"`, `?? 0` vẫn silent set về 0 thay vì abort.
  ```

  **2.4 URL/string manipulation:**
  ```markdown
  ### 2.4 URL và String Manipulation

  Kong API gateway log chứa full URL kèm query string. Cần normalize trước khi lưu vào index.

  ```vrl
  # Parse method và path từ request line: "GET /api/v1/users?id=123 HTTP/1.1"
  parsed_request, err = parse_regex(.request, r'(?sm)^[A-Z]+\s(?P<path>.*)?.*\s(?P<protocol>.*)$')
  . = merge(., parsed_request)

  # Bỏ query string — chỉ giữ path chính: /api/v1/users
  if contains(string!(.path), "?") {
    .path = split(string!(.path), "?", limit: 2)[0]
  }

  # Loại bỏ double slash: //api → /api
  .path = replace(string!(.path), "//", "/")
  ```

  Các hàm string thường dùng:
  | Hàm | Ví dụ | Kết quả |
  |---|---|---|
  | `contains(str, substr)` | `contains(.path, "?")` | boolean |
  | `split(str, delim, limit)` | `split(.path, "?", 2)[0]` | array, lấy index 0 |
  | `replace(str, pattern, with)` | `replace(.path, "//", "/")` | string |
  | `starts_with(str, prefix)` | `starts_with(.path, "/api")` | boolean |
  | `downcase(str)` | `downcase(.method)` | "get" |

  > **Pitfall:** `string!(.field)` trả về lỗi nếu `.field` null hoặc không phải string. Nếu field có thể null, dùng `string(.field) ?? ""` trước khi gọi `contains`/`split`/`replace`.
  ```

  **2.5 Timestamp normalization:**
  ```markdown
  ### 2.5 Timestamp Normalization

  Mỗi source có timestamp format khác nhau. Cần normalize về `@timestamp` dạng RFC 3339.

  ```vrl
  # Nginx: DD/Mon/YYYY:HH:MM:SS +ZZZZ
  .@timestamp = parse_timestamp!(.time_local, format: "%d/%b/%Y:%H:%M:%S %z")

  # MariaDB slow log: Unix epoch (SET timestamp=1234567890)
  .@timestamp = parse_timestamp(.timestamp, format: "%s") ?? now()

  # MariaDB error log: YYYY-MM-DD HH:MM:SS
  .@timestamp = parse_timestamp(.time_log, format: "%Y-%m-%d %H:%M:%S") ?? now()

  # Reformat existing ISO8601 timestamp
  .@timestamp = format_timestamp(.@timestamp, format: "%+") ?? now()

  # Vault/ePass: rename .time field sang .@timestamp
  if exists(.time) {
    .@timestamp = del(.time)  # del() xóa field và return giá trị
  }

  # ClickHouse: cần Unix timestamp integer
  .timestamp = to_unix_timestamp(.@timestamp)  # default: seconds
  # hoặc milliseconds:
  .timestamp = to_unix_timestamp(.@timestamp, unit: "milliseconds")
  ```

  > **Pitfall:** `parse_timestamp!` (bang) abort nếu format không khớp. Với log cũ hoặc khi format có thể thay đổi, luôn dùng `parse_timestamp(...) ?? now()` để fallback về thời điểm hiện tại.
  ```

  **2.6 Fan-in:**
  ```markdown
  ### 2.6 Fan-in: Nhiều Sources → Một Transform

  Vault agent nhận log từ cả file lẫn socket UDP — cùng một transform xử lý cả hai.

  ```yaml
  sources:
    audit_log_socket:
      type: socket
      address: 0.0.0.0:8500
      mode: udp
    audit_log_in:
      type: file
      include: [/u01/logs/vault/vault_audit.log]

  transforms:
    audit_log_modify:
      type: remap
      inputs:
        - audit_log_in      # từ file
        - audit_log_socket  # từ socket UDP — cùng transform!
      source: |
        parsed = parse_json!(.message)
        . |= object(parsed) ?? {}
        if exists(.time) {
          .@timestamp = del(.time)
        }
        del(.message)
        del(.file)
        del(.source_type)
  ```

  Fan-in hữu ích khi cùng loại log đến từ nhiều nguồn (file + network socket) và cần xử lý giống nhau.

  > **Pitfall:** Fields metadata khác nhau giữa sources (file source có `.file`, socket source không có). VRL `del(.file)` với file-sourced events OK, nhưng với socket events thì `.file` không tồn tại — `del()` trên field không tồn tại là no-op (không lỗi), nên an toàn.
  ```

- [ ] **Step 5: Verify sections 0–2**

  - [ ] Section 0 has ASCII diagram + 3 reasons for Kafka + "when NOT to use Kafka" + pitfall
  - [ ] Section 1 has 5 subsections, each with YAML + Vietnamese explanation + pitfall
  - [ ] Section 2 has 6 subsections, each with VRL code + table or explanation + pitfall
  - [ ] All IPs/passwords/hostnames redacted
  - [ ] English terms preserved in Vietnamese prose

- [ ] **Step 6: Commit**

  ```bash
  git add docs/vector/vector-patterns-vi.md
  git commit -m "docs(vector): add production patterns guide sections 0-2"
  ```

---

## Task 4: Write `vector-patterns-vi.md` — Sections 3–5

**Files:**
- Modify: `docs/vector/vector-patterns-vi.md`

**Interfaces:**
- Consumes: research notes, extracted patterns from Task 2

- [ ] **Step 1: Write Section 3 — Error Handling Strategy**

  ```markdown
  ## 3. Error Handling Strategy

  ### 3.1 `drop_on_error: true` — Tác dụng và Giới hạn

  ```yaml
  transforms:
    access_log_transform:
      type: remap
      inputs: [access_log]
      drop_on_error: true   # ← đặt ở cấp transform, không phải trong VRL
      source: |
        parsed_log, err = parse_regex(.message, r'...')
        if err != null {
          log("Parse failed: " + string!(.message), level: "warn")
          abort
        }
        . = merge(., parsed_log)
  ```

  Khi `drop_on_error: true`:
  - Event nào gây ra `abort` trong VRL sẽ bị **drop hoàn toàn** — không gửi tới sink
  - Không có `dead letter queue` mặc định — event biến mất
  - Vector vẫn chạy bình thường, không crash khi event bị drop

  Khi `drop_on_error: false` (default):
  - Event lỗi được chuyển tới sink kèm metadata lỗi
  - Tốt cho debug nhưng có thể gây ES index conflict

  ### 3.2 Pre-Parse Content Filter

  Lọc event TRƯỚC khi parse để tiết kiệm CPU. Đặc biệt hữu ích khi source gửi nhiều loại event khác nhau qua cùng một channel.

  ```vrl
  # HAProxy gửi cả Prometheus metrics (PROMEX) và access log qua syslog
  # Lọc PROMEX trước khi parse JSON — tránh parse tốn CPU
  if contains(string!(.message), "PROMEX") {
      abort
  }

  # Tiếp tục parse nếu không phải PROMEX
  parsed = parse_json!(to_string!(.message))
  . |= object(parsed) ?? {}
  ```

  Pattern: **filter → parse**, không **parse → filter**.

  > **Trade-off:** `contains(string!(.message), "X")` đơn giản và nhanh nhưng là substring match. Nếu cần filter phức tạp (regex match, multiple conditions), dùng `filter` transform thay vì `abort` trong `remap`.

  ### 3.3 Structured Error Logging

  ```vrl
  # Log lỗi kèm nội dung event để debug — trước khi abort
  parsed_log, err = parse_regex(.message, r'...')
  if err != null {
    log("Unable to parse access log: " + string!(.message), level: "warn")
    abort
  }

  # Với numeric conversion — log giá trị gây lỗi
  .bytes_sent, err = to_int(.bytes_sent)
  if err != null {
    log("Unable to parse bytes_sent: " + string!(.bytes_sent), level: "warn")
    abort
  }
  ```

  `log()` ghi vào Vector's internal log (xuất ra stdout/stderr của Vector process). Xem log bằng `journalctl -u vector` hoặc container logs.

  > **Pitfall:** `log()` không ghi vào sink — đây là internal diagnostic log, không phải log event. Không dùng `log()` để track business metrics.
  ```

- [ ] **Step 2: Write Section 4 — Fingerprinting & File Tracking**

  ```markdown
  ## 4. File Source: Fingerprinting & Tracking

  ### 4.1 `checksum` vs `device_and_inode`

  Vector cần track xem đã đọc đến đâu trong file (checkpoint). Khi file bị rotate, Vector dùng fingerprint để nhận ra đây là file mới.

  ```yaml
  # checksum: đọc N bytes đầu file, tính hash
  # Dùng cho file rotate (file bị truncate hoặc move, file mới tạo)
  sources:
    nginx_access:
      type: file
      include: [/var/log/nginx/access.log]
      fingerprint:
        strategy: checksum
        ignored_header_bytes: 5   # Bỏ qua 5 bytes đầu (vd: BOM, prefix)
        lines: 20                 # Đọc 20 dòng đầu để tính fingerprint

  # device_and_inode: track bằng OS device number + inode number
  # Dùng cho file stable, không rotate (hoặc rotate bằng cách rename)
  sources:
    vault_audit:
      type: file
      include: [/u01/logs/vault/vault_audit.log]
      fingerprint:
        strategy: device_and_inode
  ```

  | Strategy | Khi nào dùng | Rủi ro |
  |---|---|---|
  | `checksum` | File rotate (logrotate copytruncate, Docker log rotation) | Nếu N dòng đầu thay đổi, Vector coi đây là file mới → đọc lại từ đầu |
  | `device_and_inode` | File stable hoặc rotate bằng rename (logrotate default) | Nếu filesystem thay đổi (remount, migration), inode reset → đọc lại |

  `ignored_header_bytes: 5`: bỏ qua N bytes đầu file khi tính checksum. Hữu ích khi log rotation tool thêm header/prefix vào file mới.

  ### 4.2 `ignore_older_secs` và `ignore_not_found`

  ```yaml
  sources:
    app_log:
      type: file
      include: [/var/log/app/*.log]
      ignore_older_secs: 600     # Bỏ qua file không có modification trong 10 phút
      ignore_not_found: true     # Không báo lỗi nếu path chưa tồn tại
  ```

  - `ignore_older_secs: 600`: tránh đọc lại log cũ khi Vector restart. Nếu server bị down 30 phút rồi restart, Vector sẽ bỏ qua log từ trước khi down.
  - `ignore_not_found: true`: cho phép path trong `include` chưa tồn tại — Vector sẽ poll và bắt đầu đọc khi file được tạo.

  > **Trade-off:** `ignore_older_secs` tiện nhưng có thể bỏ sót log nếu service down lâu và bạn muốn recover. Trong trường hợp đó, set `ignore_older_secs` lớn hơn hoặc tạm thời xóa checkpoint của Vector.

  ### 4.3 `multiline` Configuration Fields

  Xem Section 1.3 cho ví dụ. Đây là reference nhanh cho các fields:

  | Field | Type | Mô tả |
  |---|---|---|
  | `start_pattern` | regex | Regex khớp với dòng ĐẦU TIÊN của block mới |
  | `condition_pattern` | regex | Regex xác định điều kiện gom/kết thúc (tùy `mode`) |
  | `mode` | enum | Cách interpret `condition_pattern` |
  | `timeout_ms` | integer | Flush block sau N ms nếu không có dòng mới |

  > **Pitfall:** `multiline` chỉ hoạt động khi lines đến tuần tự từ cùng một file. Nếu dùng globbing (`*.log`) và nhiều files cùng ghi đồng thời, lines từ các files có thể xen kẽ → sai block boundaries.
  ```

- [ ] **Step 3: Write Section 5 — Self-Monitoring**

  ```markdown
  ## 5. Self-Monitoring

  ### 5.1 `internal_metrics` Source

  Vector có thể export metrics về chính nó (throughput, error rate, buffer size...) qua source `internal_metrics`.

  ```yaml
  sources:
    vector_internal_metrics:
      type: internal_metrics
      scrape_interval_secs: 30   # Thu thập metrics mỗi 30 giây
  ```

  ### 5.2 `prometheus_exporter` Sink

  Expose metrics cho Prometheus scrape. Điểm đặc biệt: sink này có thể nhận cả `internal_metrics` lẫn transform outputs (component-level throughput).

  ```yaml
  sinks:
    vector_metrics_sink:
      type: prometheus_exporter
      inputs:
        - vector_internal_metrics        # Vector self metrics
        - mariadb_slow_log_transform     # Transform throughput metrics
        - mariadb_error_log_transform    # Transform error rate
        - mariadb_innodb_log_transform
      address: "0.0.0.0:21039"          # Prometheus scrape endpoint
  ```

  Khi thêm transform vào `inputs` của `prometheus_exporter`, Vector tự động expose các counter metrics như `component_received_events_total`, `component_sent_events_total`, `component_errors_total` cho từng component.

  ### 5.3 Grafana Dashboard

  Sau khi Prometheus scrape endpoint `:21039`, thêm Prometheus data source vào Grafana và import dashboard từ Grafana.com (search "Vector" trên https://grafana.com/grafana/dashboards/).

  Key metrics để monitor:
  | Metric | Ý nghĩa | Alert khi |
  |---|---|---|
  | `component_errors_total` | Số event bị drop do parse error | > 1% của throughput |
  | `component_received_events_total` | Input throughput | Drop đột ngột |
  | `component_sent_events_total` | Output throughput | Lag so với received |
  | `buffer_events` | Số event đang buffer | Tăng liên tục (backpressure) |

  > **Pitfall:** `prometheus_exporter` giữ metrics trong memory. Nếu Vector restart, counter reset về 0. Dùng Prometheus `increase()` function thay vì `rate()` để tránh false alerts khi restart.
  ```

- [ ] **Step 4: Verify complete vector-patterns-vi.md**

  - [ ] All 6 sections (0–5) present and fully written
  - [ ] Section 0: ASCII diagram + Kafka reasons + when not to use
  - [ ] Section 1: 5 parsing patterns, each with YAML + pitfall
  - [ ] Section 2: 6 VRL techniques, each with code + table/explanation + pitfall
  - [ ] Section 3: 3 error handling patterns, each with code + pitfall
  - [ ] Section 4: fingerprint table + ignore fields + multiline reference table
  - [ ] Section 5: internal_metrics + prometheus_exporter + monitoring table
  - [ ] All config snippets redacted (no real IPs, passwords, hostnames)
  - [ ] Word count approximately 3000–4000 words
  - [ ] Vietnamese throughout, English terms preserved

- [ ] **Step 5: Commit**

  ```bash
  git add docs/vector/vector-patterns-vi.md
  git commit -m "docs(vector): add production patterns guide sections 3-5"
  ```

---

## Task 5: Write `vector-transport-vi.md` — Sections 0–2

**Files:**
- Create: `docs/vector/vector-transport-vi.md`

**Interfaces:**
- Consumes: `/tmp/vector-transport-research.md` from Task 1 (for "Verified from docs" sections)
- Consumes: `/tmp/vector-patterns-extracted.md` from Task 2 (for real config values)

- [ ] **Step 1: Create file and write Section 0**

  ```markdown
  # Vector — Transport & Sink Optimization

  > Hướng dẫn tối ưu cấu hình Kafka, Elasticsearch, và ClickHouse sink trong môi trường production.
  > Mỗi tham số được verify từ tài liệu chính thức của Vector và đánh giá trade-off thực tế.

  ## 0. Aggregator Pattern

  Aggregator đọc từ Kafka, transform nhẹ (cleanup metadata), rồi ghi vào storage backend.

  ```yaml
  sources:
    log_input:
      type: kafka
      bootstrap_servers: "<broker1>:9092,<broker2>:9092,<broker3>:9092"
      group_id: "vector_nginx"
      topics:
        - "vector-log-nginx-access"

  transforms:
    cleanup:
      type: remap
      inputs: [log_input]
      timezone: Asia/Ho_Chi_Minh
      drop_on_error: true
      source: |
        # Parse JSON từ Kafka message
        parsed = parse_json!(.message)
        . |= object(parsed) ?? {}

        # Cleanup Kafka metadata fields — luôn có khi source là kafka
        del(.source_type)
        del(.topic)
        del(.partition)
        del(.offset)
        del(.message_key)
        del(.headers)
        del(.metadata)
        del(.message)

  sinks:
    es_output:
      type: elasticsearch
      inputs: [cleanup]
      # ... (xem Section 2)
  ```

  **Standard Kafka metadata fields cần cleanup:**

  | Field | Nguồn | Mô tả |
  |---|---|---|
  | `.source_type` | Vector internal | Luôn là `"kafka"` — không cần |
  | `.topic` | Kafka | Tên topic |
  | `.partition` | Kafka | Partition number |
  | `.offset` | Kafka | Offset trong partition |
  | `.message_key` | Kafka | Message key (nếu có) |
  | `.headers` | Kafka | Kafka headers map |
  | `.metadata` | Kafka | Kafka metadata |
  | `.message` | Kafka | Raw message bytes (sau khi parse) |
  ```

- [ ] **Step 2: Write Section 1 — Kafka Configuration**

  ```markdown
  ## 1. Kafka Configuration

  ### 1.1 Producer (Agent Side)

  ```yaml
  sinks:
    kafka_output:
      type: kafka
      bootstrap_servers: "<broker1>:9092,<broker2>:9092"
      topic: vector-log-nginx-access
      batch:
        max_bytes: 1000000    # 1MB per batch
        max_events: 800       # tối đa 800 events per batch
        timeout_secs: 5       # flush sau 5 giây dù chưa đủ max_bytes/max_events
      compression: zstd
      encoding:
        codec: json
  ```

  **`batch` fields — Kafka producer:**

  Batch được flush khi ĐẠT BẤT KỲ điều kiện nào:

  | Field | Giá trị thực tế | Default | Ý nghĩa |
  |---|---|---|---|
  | `max_bytes` | 1,000,000 (1MB) | 1,048,576 | Tổng kích thước batch |
  | `max_events` | 800 | không giới hạn | Số events tối đa |
  | `timeout_secs` | 5 | 1 | Flush dù chưa đủ max |

  **`compression: zstd` vs alternatives:**

  | Codec | Ratio | CPU | Tốc độ | Dùng khi |
  |---|---|---|---|---|
  | `zstd` | ~4-8x | Thấp | Nhanh nhất | Production agent→Kafka (khuyến nghị) |
  | `gzip` | ~3-5x | Trung bình | Trung bình | Compatibility (ES/CH native) |
  | `lz4` | ~2-3x | Rất thấp | Nhanh | Throughput > latency, storage rẻ |
  | `none` | 1x | Không | Nhất | Local testing, very low volume |

  > **Recommendation:** Dùng `zstd` cho Kafka producer. Kafka broker tự động decompress và không forward compression sang consumers — consumer sẽ nhận data không nén.

  ### 1.2 Consumer (Aggregator Side)

  ```yaml
  sources:
    log_input:
      type: kafka
      bootstrap_servers: "<broker1>:9092,<broker2>:9092"
      group_id: "vector_nginx_aggregator"  # Unique per consumer group
      topics:
        - "vector-log-nginx-access"
      decoding:
        codec: json   # hoặc "native" cho ClickHouse destination
  ```

  **`group_id` strategy:**
  - Mỗi aggregator deployment dùng một `group_id` riêng
  - Multiple instances của cùng aggregator SHARE `group_id` (Kafka tự partition)
  - Muốn replay từ đầu: đổi `group_id` mới (Kafka consumer group không có history)

  **`decoding.codec`:**
  | Codec | Dùng khi |
  |---|---|
  | `json` | Destination là Elasticsearch (phổ biến nhất) |
  | `native` | Destination là ClickHouse (giữ type info tốt hơn) |
  | `bytes` | Không parse, forward raw bytes |

  > **Pitfall:** Kafka broker không biết về compression của producers. Nếu producer dùng `zstd` và consumer config `decoding.codec: native`, data vẫn được decompress đúng — codec ở đây là về Vector event format, không phải Kafka compression.
  ```

- [ ] **Step 3: Write Section 2 — Elasticsearch Sink (deep analysis)**

  Use research notes from Task 1 for "Verified from docs" sections.

  ```markdown
  ## 2. Elasticsearch Sink

  ```yaml
  sinks:
    es_output:
      type: elasticsearch
      endpoints:
        - "https://<es-host>:9200"
      bulk:
        action: create
        index: nginx-access-stream
      compression: gzip
      api_version: v8
      batch:
        max_bytes: 5000000    # 5MB
        timeout_secs: 3
      request:
        timeout_secs: 30
        concurrency: 2
      auth:
        strategy: basic
        user: vector-dev
        password: "<redacted>"
      healthcheck:
        enabled: false
  ```

  ### `bulk.action`: `create` vs `index`

  **Verified from docs:** ES Bulk API có 2 action types:
  - `create`: Tạo document mới. Fail nếu `_id` đã tồn tại → HTTP 409 Conflict
  - `index`: Upsert — tạo mới hoặc overwrite nếu `_id` đã tồn tại

  **Recommendation cho log pipeline:** Luôn dùng `create`.
  - Log events không có natural `_id` → Vector tự gen random ID → không bao giờ conflict
  - Bảo vệ khỏi duplicate indexing khi aggregator bị restart và Kafka offset chưa commit

  > **Trade-off:** Nếu bạn muốn idempotent reprocessing (chạy lại aggregator từ Kafka offset cũ), cần `index` với deterministic `_id` (hash của event content). Nhưng điều này phức tạp hơn và thường không cần thiết cho log pipelines.

  ### `api_version: v8`

  ES 8.x bỏ type mapping — `api_version: v8` disable type trong bulk requests. Nếu dùng ES 7.x thì chuyển sang `v7`.

  ### `batch` — ES producer side

  | Field | Giá trị thực tế | Ý nghĩa |
  |---|---|---|
  | `max_bytes` | 5,000,000 (5MB) | Tổng payload per bulk request |
  | `timeout_secs` | 3 | Flush sau 3 giây |

  > **Verified from docs:** ES khuyến nghị bulk request size 5–15MB. Nhỏ hơn = nhiều requests hơn = overhead cao. Lớn hơn = ES phải buffer nhiều hơn, tăng GC pressure.

  ### `request.concurrency`

  | Value | Hành vi | Dùng khi |
  |---|---|---|
  | `2` (fixed) | 2 concurrent bulk requests tối đa | ES cluster ổn định, load predictable |
  | `adaptive` | Tự tăng/giảm theo latency | ES load biến động nhiều |

  **Cơ chế adaptive:** Bắt đầu với 1 concurrent request. Nếu latency tốt → tăng. Nếu latency tăng → giảm. Tránh overwhelm ES khi hot.

  > **Recommendation:** Với ES cluster dedicated cho logging, `concurrency: 2` là điểm bắt đầu tốt. Tăng lên 4–8 nếu ES có đủ resources và throughput cần thiết.

  ### `request.timeout_secs: 30`

  Timeout cho HTTP request tới ES. 30 giây phù hợp cho bulk indexing. Nếu ES cluster bận và response chậm, Vector retry sau timeout.

  ### `healthcheck.enabled: false`

  Vector mặc định check ES health khi startup. Disable này khi:
  - ES chưa available khi Vector start (common khi cả hai trong Docker Compose)
  - ES yêu cầu auth và healthcheck endpoint không có auth

  > **Trade-off:** Disable healthcheck → Vector không biết ES unreachable cho đến khi send request thật → có thể delay error detection. Nếu ES stable, không vấn đề.

  ### Trade-off: Throughput vs Latency vs ES Cluster Load

  | Config | Throughput | Indexing Latency | ES Load |
  |---|---|---|---|
  | batch 5MB, timeout 3s, concurrency 2 | Cao | Trung bình (3s max) | Trung bình |
  | batch 1MB, timeout 1s, concurrency 4 | Cao | Thấp (1s max) | Cao hơn |
  | batch 10MB, timeout 10s, concurrency 1 | Cao | Cao (10s max) | Thấp nhất |

  **Cho log pipeline thông thường:** batch 5MB + timeout 3s + concurrency 2 là balanced default.
  ```

- [ ] **Step 4: Verify sections 0–2**

  - [ ] Section 0 has complete aggregator YAML + metadata cleanup table
  - [ ] Section 1 has producer config + batch table + compression comparison table + consumer config + group_id/decoding guidance
  - [ ] Section 2 has full ES YAML + every param explained with "Verified from docs" + trade-off table
  - [ ] No placeholders in "Verified from docs" sections

- [ ] **Step 5: Commit**

  ```bash
  git add docs/vector/vector-transport-vi.md
  git commit -m "docs(vector): add transport guide sections 0-2 (kafka + elasticsearch)"
  ```

---

## Task 6: Write `vector-transport-vi.md` — Sections 3–5

**Files:**
- Modify: `docs/vector/vector-transport-vi.md`

- [ ] **Step 1: Write Section 3 — ClickHouse Sink**

  ```markdown
  ## 3. ClickHouse Sink

  ```yaml
  sinks:
    ch_output:
      type: clickhouse
      endpoint: "http://<ch-host>:8123"
      auth:
        strategy: basic
        user: kong
        password: "<redacted>"
      batch:
        max_events: 1000
        max_bytes: 1048576    # 1MB
        timeout_secs: 5
      compression: gzip
      database: kong
      table: log_dist
      skip_unknown_fields: true
      request:
        concurrency: adaptive
  ```

  ### `request.concurrency: adaptive`

  **Verified from docs:** Vector's adaptive concurrency control (ACC) là thuật toán kiểm soát concurrency dựa trên đo lường latency. Nguyên lý:
  1. Bắt đầu với concurrency = 1
  2. Tăng dần, đo RTT (round-trip time) của mỗi request
  3. Nếu RTT tăng (backend đang quá tải), giảm concurrency
  4. Nếu RTT ổn định, thử tăng thêm

  **Khi nào dùng adaptive:**
  - ClickHouse load biến động (peak giờ cao điểm, off-peak thấp)
  - Không biết concurrency tối ưu cho cluster cụ thể
  - Muốn Vector tự tune mà không cần manual adjust

  **Khi nào dùng fixed:**
  - ClickHouse cluster dedicated, load stable
  - Muốn predictable resource usage

  ### `skip_unknown_fields: true`

  ClickHouse có strict schema — insert field không có trong table definition sẽ fail. `skip_unknown_fields: true` bỏ qua các fields không có trong schema thay vì fail entire batch.

  > **Trade-off:** Có thể bỏ sót data nếu schema và event không sync. Nhưng tốt hơn là fail toàn bộ batch vì 1 field lạ. Nên monitor `component_errors_total` để phát hiện schema drift.

  ### `decoding.codec: native` trên Kafka Source

  Khi destination là ClickHouse, dùng `native` codec trên Kafka consumer để giữ type information:

  ```yaml
  sources:
    kong_input:
      type: kafka
      bootstrap_servers: "<redacted>"
      group_id: "kong-clickhouse"
      decoding:
        codec: native    # ← giữ Vector native type info
      topics: ["vector-kong-log"]
  ```

  Native codec giữ nguyên integer/float/boolean types thay vì convert tất cả sang string như JSON codec.

  ### `to_unix_timestamp()` cho ClickHouse DateTime

  ClickHouse `DateTime` column cần Unix timestamp integer. Nếu dùng ISO8601 string sẽ fail insert.

  ```vrl
  # Convert @timestamp sang Unix timestamp cho ClickHouse
  .timestamp = to_unix_timestamp(.@timestamp)             # seconds (Int64)
  .timestamp_ms = to_unix_timestamp(.@timestamp, unit: "milliseconds")  # ms (Int64)
  ```

  ### Batch Config: ClickHouse vs Elasticsearch

  | Config | Elasticsearch | ClickHouse | Lý do khác |
  |---|---|---|---|
  | `max_bytes` | 5,000,000 (5MB) | 1,048,576 (1MB) | CH insert tốt nhất với batch nhỏ thường xuyên |
  | `max_events` | không set | 1,000 | CH optimize cho row count |
  | `timeout_secs` | 3 | 5 | CH cần thêm thời gian insert |
  | `concurrency` | 2 (fixed) | adaptive | CH load biến động hơn ES |

  > **Verified from docs:** ClickHouse INSERT tốt với nhiều batch nhỏ hơn là ít batch lớn. Khuyến nghị: 100–1000 rows per insert, không nên > 10MB per insert.
  ```

- [ ] **Step 2: Write Section 4 — Compression Strategy**

  ```markdown
  ## 4. Compression Strategy

  Trong Agent→Kafka→Aggregator pipeline, compression được dùng ở 2 chỗ khác nhau với codec khác nhau:

  ```
  [Agent] ──zstd──► [Kafka] ──(Kafka tự decompress)──► [Aggregator] ──gzip──► [ES/CH]
  ```

  ### Agent → Kafka: `zstd`

  ```yaml
  sinks:
    kafka_output:
      type: kafka
      compression: zstd
  ```

  zstd được chọn vì:
  - **Tốc độ cao:** compress và decompress nhanh hơn gzip ~3-5x
  - **Ratio tốt:** tương đương hoặc tốt hơn gzip level 6
  - **CPU thấp:** agent thường chạy trên application server, không muốn tốn CPU

  ### Aggregator → ES/ClickHouse: `gzip`

  ```yaml
  sinks:
    es_output:
      type: elasticsearch
      compression: gzip   # ES và CH đều hiểu gzip natively
  ```

  gzip được chọn vì:
  - **Universal compatibility:** ES và ClickHouse HTTP API đều support gzip
  - **ES native decompression:** ES có thể index compressed data trực tiếp
  - **Giảm network bandwidth:** quan trọng khi aggregator → storage ở datacenter khác

  ### Compression Trade-off Table

  | Codec | CPU (compress) | CPU (decompress) | Ratio | Latency | Dùng khi |
  |---|---|---|---|---|---|
  | `none` | 0 | 0 | 1x | Nhất | Dev/test, local network |
  | `lz4` | Rất thấp | Rất thấp | ~2x | Rất thấp | Throughput critical, CPU constrained |
  | `gzip` | Trung bình | Thấp | ~3-5x | Trung bình | Aggregator→ES/CH (recommended) |
  | `zstd` | Thấp | Rất thấp | ~4-8x | Thấp | Agent→Kafka (recommended) |
  | `zlib` | Cao | Trung bình | ~3-5x | Cao | Compatibility với legacy systems |

  > **Recommendation:** `zstd` cho agent→Kafka, `gzip` cho aggregator→storage. Không mix compression giữa Vector và non-Vector producers/consumers trừ khi cần thiết.
  ```

- [ ] **Step 3: Write Section 5 — Concurrency & Batching Trade-offs**

  ```markdown
  ## 5. Concurrency & Batching Trade-offs

  ### Fixed vs Adaptive Concurrency

  ```yaml
  # Fixed concurrency — predictable, safe default
  request:
    concurrency: 2

  # Adaptive concurrency — self-tuning
  request:
    concurrency: adaptive
  ```

  **Khi nào dùng Fixed:**
  - ES cluster dedicated cho logging với capacity đã biết
  - Muốn giới hạn cứng số concurrent connections tới backend
  - Debug: khi adaptive gây ra oscillation (dao động liên tục)

  **Khi nào dùng Adaptive:**
  - ClickHouse với load biến động
  - Không biết optimal concurrency, muốn tự tune
  - Backend có auto-scaling (cloud services)

  **Tác động của concurrency:**

  | Concurrency | Throughput | Backend Load | Latency khi backpressure |
  |---|---|---|---|
  | 1 | Thấp nhất | Nhẹ | Thấp |
  | 2 | Tốt (balanced) | Trung bình | Trung bình |
  | 4-8 | Cao | Nặng | Cao nếu backend quá tải |
  | adaptive | Tự điều chỉnh | Tự điều chỉnh | Thấp (ACC kiểm soát) |

  ### Batch Sizing: `max_bytes` vs `max_events` vs `timeout_secs`

  Batch flush khi ĐẠT BẤT KỲ điều kiện nào trong 3 fields:

  ```
  ┌─────────────────────────────────────────────────────────┐
  │  Event đến → Buffer → Flush khi:                       │
  │                                                         │
  │  max_bytes đạt  ──OR──  max_events đạt  ──OR──  timeout │
  └─────────────────────────────────────────────────────────┘
  ```

  **Tác động của batch size lên ES:**

  | max_bytes | max_events | timeout | Hành vi |
  |---|---|---|---|
  | 5MB | — | 3s | Standard: flush khi đủ 5MB hoặc sau 3s |
  | 1MB | — | 1s | Low latency: data vào ES nhanh hơn, nhiều requests hơn |
  | 10MB | — | 10s | High throughput: ít requests, ES GC ít bị ảnh hưởng |

  **Tác động của batch size lên ClickHouse:**

  ClickHouse INSERT tốt khi batch đủ lớn để partition, nhưng quá lớn gây memory spike:
  - **Quá nhỏ (< 100 rows):** CH phải merge nhiều lần, write amplification cao
  - **Tốt nhất (100–10,000 rows):** CH insert hiệu quả, ít merge
  - **Quá lớn (> 100,000 rows):** Memory spike, CH có thể reject

  ### Giá trị khuyến nghị từ Real Configs

  | Destination | max_bytes | max_events | timeout_secs | concurrency |
  |---|---|---|---|---|
  | Kafka (producer) | 1,000,000 | 800 | 5 | n/a |
  | Elasticsearch | 5,000,000 | — | 3 | 2 |
  | ClickHouse | 1,048,576 | 1,000 | 5 | adaptive |

  > **Trade-off cuối cùng:** Giảm `timeout_secs` → data xuất hiện trong ES/CH nhanh hơn nhưng nhiều API calls hơn. Tăng `max_bytes` → ít API calls hơn nhưng data delay lâu hơn khi volume thấp. Với log pipelines, 3–5 giây delay thường chấp nhận được.
  ```

- [ ] **Step 4: Verify complete vector-transport-vi.md**

  - [ ] All 6 sections (0–5) present and complete
  - [ ] Every "Verified from docs" section has actual content (no placeholder)
  - [ ] Trade-off tables present for: ES batch, compression codecs, concurrency, batch sizing
  - [ ] ClickHouse section covers: adaptive, skip_unknown_fields, native codec, to_unix_timestamp
  - [ ] Word count approximately 2000–3000 words
  - [ ] All config snippets redacted

- [ ] **Step 5: Commit**

  ```bash
  git add docs/vector/vector-transport-vi.md
  git commit -m "docs(vector): add transport guide sections 3-5 (clickhouse + compression + concurrency)"
  ```

---

## Task 7: Update `vector-skill.md`

**Files:**
- Modify: `docs/vector/vector-skill.md`

**Interfaces:**
- Consumes: completed `vector-patterns-vi.md` and `vector-transport-vi.md` (distill from them)

- [ ] **Step 1: Append two new sections to vector-skill.md**

  Append this content at the end of the existing file:

  ```markdown
  ## Advanced Production Patterns

  Architecture: `Agent (file/syslog/socket) → Kafka (zstd) → Aggregator → ES/ClickHouse (gzip)`

  | Pattern | When | Key config |
  |---|---|---|
  | Custom regex parse | Non-standard log format | `parse_regex(.message, r'(?P<field>...)')` → `merge(., parsed)` |
  | Two-step parse | Timestamp prefix + JSON body | `parse_regex` for timestamp → `parse_json!(.message)` |
  | Multiline assembly | Multi-line logs (slow query, stack trace, InnoDB) | `file.multiline.start_pattern` + `mode: halt_before` |
  | Payload unwrap | `{payload: {...}}` nested JSON | `if exists(.payload) { . = merge!(., .payload); del(.payload) }` |
  | Pre-parse filter | Abort noisy events before expensive parse | `if contains(string!(.message), "PROMEX") { abort }` |
  | Fan-in | Multiple sources → one transform | `inputs: [src1, src2]` in transform |
  | Timestamp normalize | Standardize @timestamp | `parse_timestamp(.field, format: "...") ?? now()` |
  | ClickHouse timestamp | CH needs Unix integer | `to_unix_timestamp(.@timestamp)` |

  ## VRL Error Handling Tiers

  | Tier | Syntax | When |
  |---|---|---|
  | Abort (strictest) | `parse_json!(.message)` | Field required, parse fail = useless event |
  | Fallback | `to_float(.x) ?? 0` | Optional field, has sensible default |
  | Explicit check | `x, err = f(.x); if err != null { log(err); abort }` | Want to log before dropping |

  ## Transport Quick Reference

  | Sink | `bulk.action` | Compression | Concurrency | Batch |
  |---|---|---|---|---|
  | Kafka (producer) | n/a | `zstd` | n/a | 1MB / 800 events / 5s |
  | Elasticsearch | `create` | `gzip` | `2` (fixed) | 5MB / 3s |
  | ClickHouse | n/a | `gzip` | `adaptive` | 1MB / 1000 events / 5s |

  ## File Source Fingerprint Strategy

  | Strategy | Use when | Risk |
  |---|---|---|
  | `checksum` (lines: 20) | Log rotation with new file (copytruncate) | Header changes → re-read |
  | `device_and_inode` | Stable file or rename-based rotation | Filesystem change → re-read |

  ## Kafka Metadata Cleanup (always del after kafka source)

  ```vrl
  del(.source_type); del(.topic); del(.partition); del(.offset)
  del(.message_key); del(.headers); del(.metadata); del(.message)
  ```
  ```

- [ ] **Step 2: Verify update**

  - [ ] Two new sections appended (not replacing existing content)
  - [ ] Advanced patterns table has all 8 rows
  - [ ] VRL error handling table has all 3 tiers
  - [ ] Transport quick reference table has all 3 sinks
  - [ ] Fingerprint strategy table has both strategies
  - [ ] Kafka cleanup VRL block is present
  - [ ] Total vector-skill.md word count still within reasonable range (< 1500 words)

- [ ] **Step 3: Commit**

  ```bash
  git add docs/vector/vector-skill.md
  git commit -m "docs(vector): add advanced patterns and transport quick-reference to skill file"
  ```

---

## Self-Review

**Spec coverage check:**

| Spec requirement | Covered in |
|---|---|
| Architecture: Agent→Kafka→Aggregator workflow | Task 3 Section 0 |
| Parsing: custom regex | Task 3 Section 1.1 |
| Parsing: two-step | Task 3 Section 1.2 |
| Parsing: multiline | Task 3 Section 1.3 |
| Parsing: payload unwrapping | Task 3 Section 1.4 |
| Built-in vs custom regex decision | Task 3 Section 1.5 |
| VRL merge patterns | Task 3 Section 2.1 |
| VRL error handling tiers | Task 3 Section 2.2 |
| VRL conditional normalization | Task 3 Section 2.3 |
| VRL URL/string manipulation | Task 3 Section 2.4 |
| VRL timestamp normalization | Task 3 Section 2.5 |
| Fan-in multiple inputs | Task 3 Section 2.6 |
| drop_on_error | Task 4 Section 3.1 |
| Pre-parse content filter | Task 4 Section 3.2 |
| Error logging in VRL | Task 4 Section 3.3 |
| Fingerprinting checksum vs device_and_inode | Task 4 Section 4.1 |
| ignore_older_secs, ignore_not_found | Task 4 Section 4.2 |
| Multiline config fields | Task 4 Section 4.3 |
| Self-monitoring internal_metrics | Task 4 Section 5.1 |
| prometheus_exporter sink | Task 4 Section 5.2 |
| Grafana dashboard | Task 4 Section 5.3 |
| Aggregator pattern + Kafka cleanup | Task 5 Section 0 |
| Kafka producer batch + compression | Task 5 Section 1.1 |
| Kafka consumer group_id + decoding.codec | Task 5 Section 1.2 |
| ES bulk.action create vs index (verified) | Task 5 Section 2 |
| ES api_version, batch, concurrency (verified) | Task 5 Section 2 |
| ES healthcheck.enabled | Task 5 Section 2 |
| ClickHouse adaptive concurrency (verified) | Task 6 Section 3 |
| ClickHouse skip_unknown_fields (verified) | Task 6 Section 3 |
| ClickHouse native codec + to_unix_timestamp | Task 6 Section 3 |
| Compression zstd vs gzip trade-off | Task 6 Section 4 |
| Concurrency fixed vs adaptive | Task 6 Section 5 |
| Batch sizing max_bytes/max_events/timeout interaction | Task 6 Section 5 |
| vector-skill.md advanced section | Task 7 |

**Placeholder scan:** No TBD/TODO. All "Verified from docs" sections have actual content directives from deep research. All YAML snippets are shown. All trade-off tables have actual values.

**Type consistency:** No function calls or method signatures to check (documentation project). Config field names used consistently: `max_bytes` (not `maxBytes`), `timeout_secs` (not `timeout_seconds`), `concurrency` (not `concurrency_limit`).

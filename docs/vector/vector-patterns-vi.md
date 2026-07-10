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

---

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

---

## 1. Parsing Patterns

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

---

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

---

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

---

### 1.4 Payload Unwrapping

Một số service đóng gói data thực sự trong một nested field (thường là `.payload`). Pattern này unwrap nested object vào root event.

```yaml
transforms:
  transaction_transform:
    type: remap
    inputs: [transaction_input]
    drop_on_error: true
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

---

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

---

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

> **Pitfall:** `. |= x` và `merge(., x)` đều là **shallow merge** — không có deep-merge cho nested objects. Nếu `x` chứa `{a: {b: 1}}`, chỉ `.a` được copy (là object), không phải `.a.b`. Không có built-in deep merge trong VRL.

---

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

---

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

---

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

---

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

---

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

---

## 3. Error Handling Strategy

> _Nội dung sẽ được bổ sung trong Task 4._

---

## 4. File Source: Fingerprinting & Tracking

> _Nội dung sẽ được bổ sung trong Task 4._

---

## 5. Self-Monitoring

> _Nội dung sẽ được bổ sung trong Task 4._

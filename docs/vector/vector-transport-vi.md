# Vector — Transport & Sink Optimization

> Hướng dẫn tối ưu cấu hình Kafka, Elasticsearch, và ClickHouse sink trong môi trường production.
> Mỗi tham số được verify từ tài liệu chính thức của Vector và đánh giá trade-off thực tế.

---

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

> **Pitfall:** Nếu không `del(.message)` sau khi parse JSON, raw bytes sẽ được ghi vào Elasticsearch cùng với tất cả các fields đã parse — tăng storage không cần thiết và có thể gây nhầm lẫn khi query.

---

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
| `max_bytes` | 1,000,000 (1MB) | không có (optional) | Tổng kích thước batch |
| `max_events` | 800 | không có (optional) | Số events tối đa |
| `timeout_secs` | 5 | 1 | Flush dù chưa đủ max |

> **Verified from docs (Kafka sink):** `batch.max_bytes` và `batch.max_events` đều là optional fields không có documented default — nếu không set, sink chỉ flush theo `timeout_secs` (default 1 giây). `encoding.codec` là **required** field, không có default. Các configs trong production (nginx_agent, epass_audit) đều đặt `max_bytes: 1,000,000` và `max_events: 800` để kiểm soát batch size rõ ràng thay vì phụ thuộc vào timeout.

**`compression: zstd` vs alternatives:**

| Codec | Ratio | CPU | Tốc độ | Dùng khi |
|---|---|---|---|---|
| `zstd` | ~4-8x | Thấp | Nhanh nhất | Production agent→Kafka (khuyến nghị) |
| `gzip` | ~3-5x | Trung bình | Trung bình | Compatibility (ES/CH native) |
| `lz4` | ~2-3x | Rất thấp | Nhanh | Throughput > latency, storage rẻ |
| `snappy` | ~2-3x | Rất thấp | Rất nhanh | Throughput cực cao, latency nhạy cảm |
| `none` | 1x | Không | Nhất | Local testing, very low volume |

> **Recommendation:** Dùng `zstd` cho Kafka producer. librdkafka (Kafka client tích hợp trong Vector) tự động decompress batch nhận từ broker — Vector không cần cấu hình thêm gì về compression phía consumer. Broker giữ nguyên compressed batches và chỉ forward chúng tới consumer. Tất cả 10+ Kafka producers trong production configs đều dùng `compression: zstd`.

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

**Group ID naming patterns quan sát được trong production:**

| Pattern | Ví dụ | Dùng khi |
|---|---|---|
| Service-based | `vector_nginx`, `vector_kong_aggregator` | Single service, rõ ràng |
| Log-type-based | `mariadb_slow_monitor`, `mariadb_error_monitor` | Nhiều log type từ 1 service |
| Env-suffixed | `vault_log_prod_2`, `vault_log_uat` | Multi-env cùng Kafka cluster |

> **Verified from docs (Kafka source):** `group_id` là required field. Kafka distributes partitions among consumers in the same group — each partition consumed by exactly one consumer instance at a time. Field names added by Kafka source (`.topic`, `.partition`, `.offset`, `.message_key`, `.headers`) đều có thể override qua `*_key` parameters nếu cần.

**`decoding.codec`:**

| Codec | Default | Dùng khi |
|---|---|---|
| `bytes` | Có (default) | Forward raw bytes, không parse |
| `json` | Không | Destination là Elasticsearch (phổ biến nhất) |
| `native` | Không | Destination là ClickHouse (giữ type info tốt hơn — Vector binary protobuf format) |

> **Pitfall:** Kafka broker không biết về compression của producers. Nếu producer dùng `zstd` và consumer config `decoding.codec: native`, data vẫn được decompress đúng — `decoding.codec` ở đây là về Vector event format (cách Vector serialize event trước khi đưa vào Kafka), không phải Kafka wire compression. `decoding.codec: native` chỉ hoạt động khi producer cũng dùng `encoding.codec: native` (Vector-to-Vector pipeline).

---

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

> **Nhận xét pattern:** Config trên là "standard pattern" của 12+ aggregators trong production. Outlier duy nhất: `cdcn_aggregator` dùng `timeout_secs: 10`, `concurrency: adaptive`, `request.timeout_secs: 120` — phù hợp với pipeline có ES load biến động cao.

### `bulk.action`: `create` vs `index`

**Verified from docs (ES Bulk API):**

| Action | Hành vi | HTTP khi _id trùng |
|---|---|---|
| `index` | Upsert — tạo mới hoặc overwrite nếu `_id` đã tồn tại | 200 OK (overwrite) |
| `create` | Tạo document mới. Fail nếu `_id` đã tồn tại | 409 Conflict |
| `update` | Partial update document đã tồn tại | — |

- **Default của Vector:** `index`
- **Production configs:** Tất cả đều dùng `create` (không có config nào dùng `index`)

**Recommendation cho log pipeline:** Luôn dùng `create`.

- Log events không có natural `_id` → Vector tự gen random ID → không bao giờ conflict
- Bảo vệ khỏi duplicate indexing khi aggregator bị restart và Kafka offset chưa commit
- Đặc biệt bắt buộc khi dùng **Elasticsearch Data Streams** — Data Streams là append-only, chỉ chấp nhận `create`

> **Trade-off:** Nếu bạn muốn idempotent reprocessing (chạy lại aggregator từ Kafka offset cũ mà không tạo duplicate), cần dùng `index` với deterministic `_id` (ví dụ: hash của event content). Điều này phức tạp hơn và thường không cần thiết cho log pipelines — log events được designed để là immutable, append-only records.

### `api_version: v8`

**Verified from docs:**

| Version | Dùng khi | Đặc điểm |
|---|---|---|
| `auto` | Default — tự detect từ endpoint | Assume `v8` nếu endpoint unreachable |
| `v6` | Elasticsearch 6.x | Legacy type mapping |
| `v7` | Elasticsearch 7.x | Type mapping vẫn có nhưng deprecated |
| `v8` | Elasticsearch 8.x | Không có type mapping trong bulk requests |

ES 8.x bỏ hoàn toàn type mapping — `api_version: v8` disable `_type` field trong bulk requests. Nếu dùng ES 7.x thì dùng `v7`. **Amazon OpenSearch Serverless yêu cầu set `api_version` explicitly** (không dùng `auto`).

Production configs: mix giữa `v8` (nginx, napas, kong, cdcn, uat_kpp, transaction, app4) và `auto` (haproxy, vault, epass, mariadb, mskpp) tùy thuộc vào ES cluster version.

> **Pitfall:** Dùng `api_version: auto` có thể fail lúc startup nếu ES chưa sẵn sàng để respond version negotiation request. Với production, nên pin version cụ thể (`v8` cho ES 8.x) để tránh startup race condition.

### `compression: gzip`

**Verified from docs (ES sink compression):**

- **Default:** `none` (không nén)
- **Valid values:** `none`, `gzip`, `snappy`, `zlib`, `zstd`
- Tất cả algorithms dùng default compression level

Production pattern: hầu hết dùng `gzip` — được ES native support và không cần decompress riêng. Một số configs dùng `none` (haproxy, vault prod) — có thể do bandwidth không phải bottleneck hoặc ES cluster ở cùng network.

> **Trade-off:** `gzip` tăng CPU ~10-30% trên aggregator (tùy throughput), nhưng giảm network bandwidth 60-70% khi gửi lên ES. Với throughput < 10MB/s, overhead không đáng kể. Với throughput > 100MB/s, cân nhắc monitor CPU trước khi enable.

### `batch` — ES producer side

| Field | Giá trị thực tế | Default (docs) | Ý nghĩa |
|---|---|---|---|
| `max_bytes` | 5,000,000 (5MB) | 10,000,000 (10MB) | Tổng payload per bulk request (uncompressed) |
| `timeout_secs` | 3 | 1 | Flush sau N giây |

> **Verified from docs:** `batch.max_bytes` default là 10MB (`10000000`), tính theo uncompressed event size trước khi serialize. `batch.timeout_secs` default là 1 giây.
>
> Production configs dùng **5MB** (thay vì default 10MB) và **3 giây** (thay vì default 1 giây) — đây là "balanced default" cho throughput vs latency vs ES cluster load.
>
> ES khuyến nghị bulk request size 5–15MB. Nhỏ hơn = nhiều HTTP requests hơn = overhead cao. Lớn hơn = ES phải buffer nhiều hơn, tăng GC pressure trên JVM heap.

> **Pitfall:** `max_bytes` được tính trên uncompressed event size. Nếu dùng `compression: gzip`, batch thực sự gửi lên ES có thể nhỏ hơn nhiều so với 5MB. Điều này không ảnh hưởng đến correctness, nhưng ES bulk request nhỏ hơn mong đợi.

### `request.concurrency`

**Verified from docs:**

| Value | Hành vi |
|---|---|
| `"adaptive"` (default) | Vector ARC — tự động optimize theo p90 latency của downstream ES |
| positive integer (vd. `2`) | Fixed — tối đa N concurrent bulk requests |
| `"none"` | Fixed concurrency = 1 |

**Cơ chế ARC (Adaptive Request Concurrency):** Bắt đầu với 1 concurrent request. Nếu latency tốt → tăng. Nếu latency tăng → giảm. Cơ chế tương tự TCP congestion control (AIMD). Tránh overwhelm ES khi hot.

| Scenario | Dùng | Lý do |
|---|---|---|
| ES cluster ổn định, load predictable | `concurrency: 2` | Predictable, dễ debug, không oscillate |
| ES load biến động cao | `concurrency: adaptive` | Auto-tune theo backpressure |
| High-throughput, ES có nhiều resources | `concurrency: 4-8` | Tăng throughput |

**Production observation:** 12+ aggregators dùng `concurrency: 2`. Chỉ `cdcn_aggregator` và `clickhouse_ag` dùng `adaptive` — cả hai đều có ES/ClickHouse load biến động.

> **Recommendation:** Với ES cluster dedicated cho logging, `concurrency: 2` là điểm bắt đầu tốt. Tăng lên 4–8 nếu ES có đủ resources và throughput cần thiết. Dùng `adaptive` nếu ES có variable load patterns.

> **Pitfall:** Tăng `concurrency` quá cao (> 8) mà ES cluster không có đủ thread pool có thể dẫn đến `TOO_MANY_REQUESTS` (429) errors. Vector sẽ retry, nhưng nếu cluster đang overloaded thì concurrency cao làm tình hình tệ hơn. Với `adaptive`, Vector tự giảm khi detect backpressure — đây là lý do `adaptive` an toàn hơn cho trường hợp không biết capacity.

### `request.timeout_secs: 30`

**Verified from docs:** Default là **60 giây**. Production configs dùng **30 giây**.

Timeout cho HTTP request tới ES. Nếu ES cluster bận và response chậm hơn N giây, Vector retry sau timeout. Với bulk indexing bình thường, 30 giây là đủ. Outlier: `cdcn_aggregator` dùng `timeout_secs: 120` — ES cluster đó có response time cao hơn bình thường do data volume lớn.

> **Pitfall:** Đừng set timeout quá thấp (< 10 giây) cho bulk request. ES cần time để process large batches. Timeout quá thấp sẽ gây retry storm — nhiều request fail → Vector retry → ES càng bận hơn → vòng lặp.

### `healthcheck.enabled: false`

**Verified from docs:** Default là `true`. Khi enabled, Vector verify ES accessible khi sink initialize. On failure: log error nhưng Vector vẫn start (soft failure). Để force exit on failure, dùng `--require-healthy` CLI flag.

**Khi nào disable:**

| Scenario | Lý do disable |
|---|---|
| ES chưa available khi Vector start | Docker Compose race condition — Vector start trước ES |
| ES healthcheck endpoint cần auth riêng | Healthcheck HTTP GET không send auth header giống bulk request |
| Testing / short-lived pipelines | Không cần wait for healthcheck |

Production: 4 configs disable (`haproxy_aggregator`, `vault_aggregator`, `vault_uat_aggregator`, `transaction`). Tất cả đều là môi trường production nơi ES và Vector có thể restart independently.

> **Trade-off:** Disable healthcheck → Vector không biết ES unreachable cho đến khi send request thật → delay error detection vài giây đến vài chục giây (tuỳ `batch.timeout_secs`). Nếu ES stable, không vấn đề. Nếu cần early warning, giữ `enabled: true` và monitor Vector logs.

### Trade-off: Throughput vs Latency vs ES Cluster Load

| Config | Throughput | Indexing Latency | ES Load |
|---|---|---|---|
| batch 5MB, timeout 3s, concurrency 2 | Cao | Trung bình (≤3s) | Trung bình (**production default**) |
| batch 1MB, timeout 1s, concurrency 4 | Cao | Thấp (≤1s) | Cao hơn |
| batch 10MB, timeout 10s, concurrency 1 | Cao | Cao (≤10s) | Thấp nhất |
| batch 5MB, timeout 10s, concurrency adaptive | Biến động | Biến động | Tự điều chỉnh |

**Cho log pipeline thông thường:** batch 5MB + timeout 3s + concurrency 2 là balanced default — được verify qua 12+ production configs.

**Cho pipeline volume lớn với ES có nhiều resources:** Tăng concurrency lên 4-8 trước, batch size sau.

---

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

> **Pitfall:** Adaptive concurrency bắt đầu với concurrency = 1 mỗi khi Vector restart — throughput sẽ thấp trong vài chục giây đầu khi ACC đang probe optimal concurrency. Nếu ClickHouse có latency SLA ngặt, dùng `concurrency: 2` (fixed) để tránh cold-start performance dip.

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

> **Pitfall:** `decoding.codec: native` chỉ hoạt động khi cả producer và consumer đều là Vector. Nếu một producer khác (logstash, filebeat) ghi vào cùng Kafka topic, native codec sẽ fail decode. Dùng `json` nếu topic có nhiều producer khác nhau.

### `to_unix_timestamp()` cho ClickHouse DateTime

ClickHouse `DateTime` column cần Unix timestamp integer. Nếu dùng ISO8601 string sẽ fail insert.

```vrl
# Convert @timestamp sang Unix timestamp cho ClickHouse
.timestamp = to_unix_timestamp(.@timestamp)             # seconds (Int64)
.timestamp_ms = to_unix_timestamp(.@timestamp, unit: "milliseconds")  # ms (Int64)
```

**Verified from docs:** Signature của `to_unix_timestamp`: `to_unix_timestamp(timestamp, unit?)`. Tham số `unit` nhận các giá trị: `"seconds"` (default), `"milliseconds"`, `"microseconds"`, `"nanoseconds"`. Hàm trả về `integer` — phù hợp trực tiếp với ClickHouse `DateTime`, `DateTime64`, hoặc `Int64` columns.

> **Pitfall:** ClickHouse `DateTime` column cần `Int32` (seconds), `DateTime64(3)` cần `Int64` (milliseconds). Dùng sai unit sẽ gây insert fail hoặc data corruption (timestamp lệch 1000x). Kiểm tra column type trong ClickHouse trước khi deploy.

### Batch Config: ClickHouse vs Elasticsearch

| Config | Elasticsearch | ClickHouse | Lý do khác |
|---|---|---|---|
| `max_bytes` | 5,000,000 (5MB) | 1,048,576 (1MB) | CH insert tốt nhất với batch nhỏ thường xuyên |
| `max_events` | không set | 1,000 | CH optimize cho row count |
| `timeout_secs` | 3 | 5 | CH cần thêm thời gian insert |
| `concurrency` | 2 (fixed) | adaptive | CH load biến động hơn ES |

> **Verified from docs:** ClickHouse INSERT tốt với nhiều batch nhỏ hơn là ít batch lớn. Khuyến nghị: 100–10,000 rows per insert là tối ưu; không nên vượt quá 100,000 rows hoặc 10MB per insert để tránh memory spike và CH reject.

> **Pitfall / Trade-off:** `skip_unknown_fields: true` là safety net, không phải giải pháp lâu dài. Nếu log schema thay đổi thường xuyên, nên update ClickHouse table definition trước khi deploy — fields bị bỏ qua silently sẽ không xuất hiện trong CH và không có error nào được throw.

---

## 4. Compression Strategy

Trong Agent→Kafka→Aggregator pipeline, compression được dùng ở 2 chỗ khác nhau với codec khác nhau:

```
[Agent] ──zstd──► [Kafka] ──(Aggregator tự decompress)──► [Aggregator] ──gzip──► [ES/CH]
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

> **Pitfall:** Nếu Kafka broker version cũ (< 2.1), `zstd` compression không được hỗ trợ. Kiểm tra broker version trước khi dùng `zstd` — dùng `lz4` hoặc `gzip` nếu broker cũ hơn 2.1.

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

> **Trade-off:** `gzip` tăng CPU ~10-30% trên aggregator (tùy throughput). Nếu aggregator và storage trong cùng datacenter (low-latency, high-bandwidth internal network), cân nhắc `none` để giảm CPU overhead — network không phải bottleneck trong trường hợp này.

### Compression Trade-off Table

| Codec | CPU (compress) | CPU (decompress) | Ratio | Latency | Dùng khi |
|---|---|---|---|---|---|
| `none` | 0 | 0 | 1x | Nhất | Dev/test, local network |
| `lz4` | Rất thấp | Rất thấp | ~2x | Rất thấp | Throughput critical, CPU constrained |
| `gzip` | Trung bình | Thấp | ~3-5x | Trung bình | Aggregator→ES/CH (recommended) |
| `zstd` | Thấp | Rất thấp | ~4-8x | Thấp | Agent→Kafka (recommended) |
| `zlib` | Cao | Trung bình | ~3-5x | Cao | Compatibility với legacy systems |

> **Recommendation:** `zstd` cho agent→Kafka, `gzip` cho aggregator→storage. Không mix compression giữa Vector và non-Vector producers/consumers trừ khi cần thiết.

> **Pitfall / Trade-off:** Kafka broker lưu và forward compressed batches nguyên vẹn — consumer tự decompress. Vì vậy, compression codec của Kafka producer (`zstd`) và codec của Vector ES/CH sink (`gzip`) là hoàn toàn độc lập. Đừng nhầm lẫn giữa Kafka wire compression và Vector sink HTTP compression.

---

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

> **Pitfall:** Khi adaptive concurrency bị "stuck" ở concurrency = 1 (thường xảy ra khi backend latency cao ngay từ cold start), tăng `request.timeout_secs` để cho phép ACC thu thập đủ sample trước khi quyết định tăng concurrency. Nếu vẫn không tăng, switch sang fixed concurrency để debug bottleneck.

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

> **Pitfall:** `timeout_secs` cần đủ lớn để batch tích lũy trong off-peak hours. Nếu set quá thấp (1–2s), Vector liên tục flush batch nhỏ — với ClickHouse, nhiều INSERT nhỏ dưới 100 rows tăng merge overhead đáng kể và có thể trigger "too many parts" error.

### Giá trị khuyến nghị từ Real Configs

| Destination | max_bytes | max_events | timeout_secs | concurrency |
|---|---|---|---|---|
| Kafka (producer) | 1,000,000 | 800 | 5 | n/a |
| Elasticsearch | 5,000,000 | — | 3 | 2 |
| ClickHouse | 1,048,576 | 1,000 | 5 | adaptive |

> **Trade-off cuối cùng:** Giảm `timeout_secs` → data xuất hiện trong ES/CH nhanh hơn nhưng nhiều API calls hơn. Tăng `max_bytes` → ít API calls hơn nhưng data delay lâu hơn khi volume thấp. Với log pipelines, 3–5 giây delay thường chấp nhận được.

> **Pitfall / Trade-off:** `max_bytes` được tính trên uncompressed event size trước khi serialize. Khi dùng `compression: gzip`, actual HTTP payload gửi lên ES/CH nhỏ hơn nhiều so với `max_bytes`. Điều này không ảnh hưởng correctness, nhưng cần tính đến khi estimate network bandwidth — throughput thực tế trên wire thấp hơn `max_bytes × flush_rate`.

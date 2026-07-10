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

---

## 1. Vector là gì?

Vector là một công cụ pipeline (luồng xử lý dữ liệu) cho observability — nó thu thập log và metric từ nhiều nguồn, biến đổi chúng theo ý muốn, rồi gửi tới các hệ thống lưu trữ hoặc phân tích. Vector được viết bằng Rust, chạy như một tiến trình duy nhất không cần runtime phụ thuộc, tiêu thụ ít RAM hơn và xử lý throughput cao hơn so với các công cụ cùng loại trên cùng phần cứng.

Nếu bạn đã từng dùng Fluentd hay Filebeat, Vector làm việc tương tự nhưng nhanh hơn đáng kể và có ngôn ngữ transform mạnh mẽ hơn tích hợp sẵn. Vector không thay thế hệ thống lưu trữ log (như Loki, Elasticsearch) hay hệ thống metric (như Prometheus) — nó là lớp trung gian vận chuyển và biến đổi dữ liệu trước khi dữ liệu tới đích cuối cùng.

> **Ví dụ thực tế:** Bạn có log nginx trên máy chủ, muốn parse chúng thành JSON có cấu trúc, lọc bỏ các request static file, rồi gửi vào Loki. Vector là công cụ làm việc đó — một file config YAML duy nhất mô tả toàn bộ luồng xử lý từ đầu đến cuối.

---

## 2. Mô hình Pipeline

Mọi thứ trong Vector đều xoay quanh ba khái niệm: **source** (nguồn dữ liệu), **transform** (biến đổi), và **sink** (đích đến). Dữ liệu chạy theo một chiều từ source qua các transform rồi vào sink. Vector xây dựng các component này thành một đồ thị có hướng không chu trình (DAG) — mỗi component khai báo nó nhận dữ liệu từ đâu, và Vector tự sắp xếp thứ tự xử lý.

Mỗi đơn vị dữ liệu chạy qua pipeline được gọi là một **event** (sự kiện). Một event có thể là một dòng log, một điểm metric, hoặc một trace. Vector xử lý từng event một cách độc lập khi nó đi qua pipeline. Trong file config `vector.yaml`, bạn khai báo các component và kết nối chúng với nhau qua trường `inputs` — mỗi transform hoặc sink khai báo nó nhận input từ component nào (chi tiết ở Chương 7).

```
┌─────────────┐     ┌─────────────────┐     ┌──────────────┐
│   Source    │────▶│   Transform(s)  │────▶│     Sink     │
│ (dữ liệu   │     │ (biến đổi,      │     │ (lưu trữ,   │
│  đầu vào)  │     │  lọc, định tuyến│     │  xuất ra)   │
└─────────────┘     └─────────────────┘     └──────────────┘
```

Ví dụ config tối giản thể hiện một pipeline hoàn chỉnh: đọc syslog từ file, thêm tag môi trường, rồi in ra stdout để kiểm tra:

```yaml
# vector.yaml — pipeline đơn giản: file → remap → console
sources:
  syslog_file:                        # tên tự đặt cho source này
    type: file                        # đọc từ file trên đĩa
    include:
      - /var/log/syslog               # đường dẫn file cần đọc

transforms:
  add_env_tag:                        # tên tự đặt cho transform này
    type: remap                       # dùng VRL để biến đổi event
    inputs:
      - syslog_file                   # nhận event từ source ở trên
    source: |
      .environment = "production"     # thêm field mới vào mỗi event

sinks:
  debug_out:                          # tên tự đặt cho sink này
    type: console                     # xuất ra stdout
    inputs:
      - add_env_tag                   # nhận event từ transform ở trên
    encoding:
      codec: json                     # định dạng JSON khi in ra
```

> **Ví dụ thực tế:** Nginx ghi log → Vector đọc file log (source) → Vector parse và lọc (transform) → Vector gửi vào Loki (sink). Chỉ một file `vector.yaml` mô tả toàn bộ luồng đó.

---

## 3. Events — Dữ liệu trong Vector

Một **event** là đơn vị dữ liệu nhỏ nhất trong Vector. Vector có hai loại event chính:

- **Log event**: một bản ghi có cấu trúc key-value. Phổ biến nhất — đây là những gì bạn làm việc hầu hết thời gian.
- **Metric event**: một điểm dữ liệu số (counter, gauge, histogram). Dùng khi bạn xử lý metrics từ statsd, prometheus scrape, v.v.

Khi Vector đọc một dòng log từ file, nó tạo ra một log event với ít nhất bốn field mặc định. Ví dụ với một dòng nginx access log:

```json
{
  "message": "127.0.0.1 - - [10/Jul/2026:09:30:00 +0000] \"GET /api/health HTTP/1.1\" 200 32",
  "timestamp": "2026-07-10T09:30:00Z",
  "host": "web-01",
  "source_type": "file"
}
```

Field `.message` chứa nội dung thô của dòng log. Các transform — đặc biệt là `remap` dùng ngôn ngữ VRL (Vector Remap Language) — giúp bạn tách `.message` thành các field có ý nghĩa như `.status`, `.request`, `.client`. Trong VRL, bạn truy cập field bằng ký hiệu dấu chấm: `.message`, `.status`, `.timestamp`. Toàn bộ event được biểu diễn bằng dấu chấm `.` (dot) — khi bạn gán `. = ...` nghĩa là bạn đang thay thế toàn bộ event.

Ví dụ config tối giản: đọc nginx log, parse thành các field rời rạc, rồi xem kết quả trên stdout:

```yaml
# vector.yaml — pipeline đọc nginx log và parse event
sources:
  nginx_logs:
    type: file
    include:
      - /var/log/nginx/access.log     # đọc nginx access log

transforms:
  parse_nginx:
    type: remap                       # dùng VRL để biến đổi event
    inputs:
      - nginx_logs                    # nhận event thô từ source
    source: |
      # parse_nginx_log tách .message thành các field có cấu trúc
      . = parse_nginx_log!(.message, format: "combined")
      # sau bước này event có: .client, .request, .status, .size, ...

sinks:
  stdout_out:
    type: console                     # in ra stdout để kiểm tra
    inputs:
      - parse_nginx                   # nhận event đã parse
    encoding:
      codec: json                     # in dạng JSON để thấy rõ các field
```

> **Ví dụ thực tế:** Sau khi parse nginx log, event sẽ có thêm các field: `.client` (địa chỉ IP), `.request` (dòng request thô, ví dụ: "GET /api/health HTTP/1.1"), `.status` (mã HTTP dạng số nguyên). Bạn có thể dùng thêm một transform `filter` để chỉ giữ lại các event có `.status >= 400` — tức là chỉ lấy các request lỗi.

---

## 4. Sources — Nơi dữ liệu vào

Source là điểm bắt đầu của mọi pipeline — nơi Vector nhận dữ liệu đầu vào. Mỗi source có một `type` xác định cách Vector thu thập dữ liệu (đọc file, lắng nghe socket, nhận HTTP POST, v.v.) và một tên duy nhất do bạn tự đặt. Tên đó được các component phía sau dùng trong trường `inputs` để tham chiếu đến source này.

Mỗi dòng dữ liệu đọc được từ source trở thành một **event** độc lập trong pipeline. Vector tự động thêm các field metadata như `message` (nội dung thô), `timestamp`, `host`, và `source_type` vào mỗi event. Bạn không cần khai báo `inputs` trong source — source luôn là điểm khởi đầu, không có upstream.

Các source phổ biến nhất:

| Type | Dùng khi |
|---|---|
| `file` | Đọc log từ file trên disk, theo dõi liên tục như `tail -f` |
| `stdin` | Nhận dữ liệu từ standard input — hữu ích để test và piping |
| `syslog` | Nhận log qua giao thức syslog (UDP/TCP/Unix socket) |
| `http` | Nhận event qua HTTP POST từ ứng dụng hoặc webhook |
| `kubernetes_logs` | Đọc log của tất cả container trên một node Kubernetes |

**Ví dụ: đọc nginx access log từ file**

```yaml
# vector.yaml — đọc nginx access log và in ra console để xem event thô
sources:
  nginx_access:                         # tên tự đặt — dùng để tham chiếu sau
    type: file
    include:
      - /var/log/nginx/access.log       # đường dẫn file cần theo dõi
    read_from: beginning                # đọc từ đầu file khi khởi động lần đầu
                                        # dùng "end" để chỉ đọc dòng mới từ bây giờ

sinks:
  debug_out:
    type: console                       # in ra stdout để kiểm tra event thô
    inputs:
      - nginx_access                    # nhận event từ source "nginx_access"
    encoding:
      codec: json                       # mỗi event in ra một dòng JSON
```

**Ví dụ: nhận syslog qua UDP**

```yaml
# vector.yaml — nhận syslog qua UDP và in ra console
sources:
  syslog_input:                         # tên tự đặt
    type: syslog
    mode: udp                           # giao thức: "tcp", "udp", hoặc "unix"
    address: "0.0.0.0:514"             # lắng nghe trên tất cả interface, cổng 514
                                        # Vector tự parse RFC 3164/5424 và tạo các field:
                                        # .appname, .severity, .facility, .hostname, ...

sinks:
  debug_out:
    type: console
    inputs:
      - syslog_input                    # nhận event từ source "syslog_input"
    encoding:
      codec: json                       # in ra JSON để thấy các field syslog đã được parse
```

> **Ví dụ thực tế:** Một server nginx ghi log vào `/var/log/nginx/access.log`. Vector dùng source `file` để theo dõi file đó liên tục (giống `tail -f`) và tạo một event cho mỗi dòng mới. Event chứa dòng log thô trong field `.message` — các transform phía sau sẽ parse `.message` thành các field có cấu trúc như `.status`, `.request`, `.client`.

---

## 5. Sinks — Nơi dữ liệu ra

Sink là điểm cuối của pipeline — nơi Vector gửi event đến đích lưu trữ hoặc xử lý. Khác với source, mỗi sink **bắt buộc phải khai báo `inputs`** để chỉ định nó nhận dữ liệu từ component nào (source hoặc transform). Một sink có thể nhận từ nhiều component cùng lúc bằng cách liệt kê nhiều tên trong danh sách `inputs`.

Việc lựa chọn sink phù hợp phụ thuộc vào đích đến cuối cùng của dữ liệu. Trong giai đoạn viết và kiểm tra config, sink `console` là lựa chọn nhanh nhất để xem event trông như thế nào trước khi chuyển sang sink thật.

Các sink phổ biến nhất:

| Type | Dùng khi |
|---|---|
| `console` | In ra stdout — rất hữu ích để debug config và xem event thô |
| `file` | Ghi event ra file trên disk, hỗ trợ path template theo ngày/giờ |
| `http` | Gửi event qua HTTP POST đến bất kỳ endpoint nào |
| `loki` | Gửi log vào Grafana Loki để lưu trữ và tìm kiếm |
| `elasticsearch` | Gửi log vào Elasticsearch hoặc OpenSearch để index và phân tích |

**Ví dụ: in ra console (debug)**

```yaml
# vector.yaml — file source → console sink (kiểm tra event)
sources:
  nginx_access:
    type: file
    include:
      - /var/log/nginx/access.log       # file log cần đọc

sinks:
  debug_out:
    type: console                       # xuất ra stdout — hữu ích khi debug
    inputs:
      - nginx_access                    # nhận event từ source "nginx_access"
    target: stdout                      # "stdout" (mặc định) hoặc "stderr"
    encoding:
      codec: json                       # in ra dạng JSON, mỗi event một dòng
```

**Ví dụ: gửi vào Loki**

```yaml
# vector.yaml — file source → loki sink
sources:
  nginx_access:
    type: file
    include:
      - /var/log/nginx/access.log       # đọc nginx access log

sinks:
  loki_out:
    type: loki
    inputs:
      - nginx_access                    # nhận event từ source "nginx_access"
    endpoint: "http://loki:3100"        # địa chỉ Loki server
    labels:
      app: nginx                        # label cố định — dùng để lọc log trong Grafana
      env: production                   # label môi trường
      # labels được Loki dùng để index và tra cứu; giữ số lượng label ít và có giá trị cố định
    encoding:
      codec: json                       # gửi mỗi event dưới dạng JSON đến Loki
```

> **Ví dụ thực tế:** Trong quá trình phát triển config, hãy dùng sink `console` trước để xem event trông như thế nào — kiểm tra xem các field đã đúng tên, đúng kiểu dữ liệu, và không còn field thừa. Khi kết quả đã ổn, chỉ cần đổi `type: console` thành `type: loki` (hoặc `type: elasticsearch`) và thêm các field cần thiết. Đây là cách debug nhanh nhất và tránh gửi dữ liệu sai vào hệ thống lưu trữ thật.

---

## 6. Transforms — Biến đổi dữ liệu

Transform là bước xử lý nằm giữa source và sink. Mỗi transform nhận event từ `inputs`, xử lý — parse, lọc, tách luồng, hoặc gom nhóm — rồi truyền event đã thay đổi cho component tiếp theo. Trong một pipeline, bạn có thể có không, một, hoặc nhiều transform kết nối nối tiếp hoặc song song — các transform sau tham chiếu các transform trước qua trường `inputs`, giống như cách transform tham chiếu source.

Một số transform như `remap` và `filter` là stateless: mỗi event được xử lý độc lập, không giữ trạng thái giữa các event. Một số transform khác như `dedupe` và `reduce` là stateful: chúng duy trì bộ nhớ tạm giữa các event. Biết sự khác biệt này giúp bạn thiết kế pipeline đúng và tránh các hành vi không mong muốn khi Vector restart.

### 6.1 remap — Biến đổi field với VRL

`remap` là transform quan trọng nhất và được dùng nhiều nhất trong Vector. Nó dùng **VRL (Vector Remap Language)** — một ngôn ngữ scripting nhỏ gọn, được biên dịch sang Rust — để đọc, sửa, thêm, xóa field trong mỗi event.

VRL dùng ký hiệu dấu chấm để truy cập field: `.message`, `.status`, `.timestamp`. Toàn bộ event được biểu diễn bởi dấu chấm `.` — khi bạn viết `. = something` nghĩa là bạn đang thay thế toàn bộ event bằng giá trị mới. Hàm có hậu tố `!` (bang operator) sẽ abort — bỏ event hiện tại — nếu hàm đó gặp lỗi. Ngược lại, nếu không dùng `!`, lỗi sẽ bị bỏ qua âm thầm và event có thể chứa dữ liệu sai. Đây là cơ chế bảo vệ: thà bỏ một event hỏng còn hơn để dữ liệu sai lan vào pipeline.

```yaml
# vector.yaml — parse nginx log: file → remap → console
sources:
  nginx_access:
    type: file
    include:
      - /var/log/nginx/access.log

transforms:
  parse_nginx:
    type: remap
    inputs:
      - nginx_access              # nhận event thô từ source "nginx_access"
    source: |
      # parse_nginx_log! tách .message thành các field có cấu trúc
      # dấu ! nghĩa là: nếu parse thất bại, bỏ event này đi
      . = parse_nginx_log!(string!(.message), format: "combined")

      # thêm field mới từ giá trị hiện có
      .is_error = .status >= 400

      # xóa field không cần thiết
      del(.source_type)

sinks:
  out:
    type: console
    inputs:
      - parse_nginx               # nhận event đã xử lý từ transform
    encoding:
      codec: json
```

**Các hàm VRL thường dùng:**

| Hàm | Mô tả |
|---|---|
| `parse_nginx_log!(msg, format: "combined")` | Parse nginx access log thành các field rời rạc |
| `parse_syslog!(msg)` | Parse syslog format (RFC 3164/5424) |
| `parse_json!(msg)` | Parse chuỗi JSON thành object |
| `to_string(value)` | Chuyển giá trị bất kỳ sang chuỗi |
| `to_int!(value)` | Chuyển sang số nguyên (dấu `!` = abort nếu lỗi) |
| `del(.field)` | Xóa field khỏi event |
| `exists(.field)` | Kiểm tra field có tồn tại trong event không |
| `downcase(string)` | Chuyển chuỗi sang chữ thường |

> **Ví dụ thực tế:** Log nginx thô là một dòng text khó tìm kiếm và lọc. Sau `remap` với `parse_nginx_log!`, event có các field riêng biệt: `.client` (địa chỉ IP), `.request` (dòng request), `.status` (mã HTTP), `.size` (kích thước response) — bạn có thể filter, group, hoặc cảnh báo dựa trên từng field thay vì phải dùng regex trên chuỗi thô.

---

### 6.2 filter — Lọc event

`filter` giữ lại event thỏa điều kiện và **bỏ hoàn toàn** (drop permanently) các event không thỏa. Điều kiện viết bằng VRL expression trả về boolean.

> **Lưu ý quan trọng:** Event bị filter bỏ sẽ biến mất hoàn toàn khỏi pipeline — không có cách nào lấy lại, không có output `.dropped`. Nếu bạn muốn gửi event không thỏa điều kiện sang sink khác thay vì xóa chúng, hãy dùng `route` (xem mục 6.3).

```yaml
# vector.yaml — chỉ giữ lại request lỗi: file → remap → filter → console
sources:
  nginx_access:
    type: file
    include:
      - /var/log/nginx/access.log

transforms:
  parse_nginx:
    type: remap
    inputs:
      - nginx_access
    source: |
      . = parse_nginx_log!(string!(.message), format: "combined")

  only_errors:
    type: filter
    inputs:
      - parse_nginx               # nhận event từ transform "parse_nginx"
    condition: '.status >= 400'   # chỉ giữ event có HTTP status >= 400

sinks:
  out:
    type: console
    inputs:
      - only_errors
    encoding:
      codec: json
```

Điều kiện phức tạp hơn — loại bỏ healthcheck request:

```yaml
# vector.yaml — bỏ healthcheck request: file → remap → filter → console
sources:
  nginx_access:
    type: file
    include:
      - /var/log/nginx/access.log

transforms:
  parse_nginx:
    type: remap
    inputs:
      - nginx_access
    source: |
      . = parse_nginx_log!(string!(.message), format: "combined")

  exclude_healthcheck:
    type: filter
    inputs:
      - parse_nginx
    condition: '.request != "GET /health HTTP/1.1"'   # bỏ healthcheck requests

sinks:
  out:
    type: console
    inputs:
      - exclude_healthcheck
    encoding:
      codec: json
```

> **Ví dụ thực tế:** Bạn có hàng nghìn request mỗi giây nhưng chỉ muốn alert khi có lỗi. Dùng `filter` với điều kiện `.status >= 500` trước sink tới alerting system — các request thành công bị loại bỏ ngay tại đây, không tiêu tốn băng thông hay chi phí lưu trữ.

---

### 6.3 route — Phân luồng event

`route` tách một luồng event thành nhiều luồng dựa trên điều kiện. Khác với `filter` xóa event không thỏa, `route` **gửi event đến output khác nhau** — mỗi route có tên riêng và trở thành một output độc lập mà các component phía sau có thể dùng làm input.

Để tham chiếu output của route trong `inputs` của sink hoặc transform khác, dùng cú pháp `tên_transform.tên_route`. Event không khớp với bất kỳ route nào sẽ vào output đặc biệt `_unmatched` — bạn có thể dùng `tên_transform._unmatched` trong `inputs` để bắt các event này thay vì để chúng bị mất.

```yaml
# vector.yaml — phân luồng theo status: file → remap → route → nhiều sink
sources:
  nginx_access:
    type: file
    include:
      - /var/log/nginx/access.log

transforms:
  parse_nginx:
    type: remap
    inputs:
      - nginx_access
    source: |
      . = parse_nginx_log!(string!(.message), format: "combined")

  split_by_status:
    type: route
    inputs:
      - parse_nginx               # nhận event từ transform "parse_nginx"
    route:
      errors:   '.status >= 500'                    # output "errors"
      warnings: '.status >= 400 && .status < 500'   # output "warnings"
      ok:       '.status < 400'                     # output "ok"

sinks:
  out_errors:
    type: console
    inputs:
      - split_by_status.errors    # cú pháp: tên_transform.tên_route
    target: stderr                # lỗi 5xx ra stderr
    encoding:
      codec: json

  out_all:
    type: console
    inputs:
      - split_by_status.warnings
      - split_by_status.ok
      - split_by_status._unmatched  # bắt event không khớp route nào
    target: stdout
    encoding:
      codec: json
```

> **Ví dụ thực tế:** Gửi lỗi 5xx vào hệ thống alert, ghi tất cả request vào Loki để lưu trữ dài hạn — dùng `route` để chia luồng mà không cần đọc source hai lần. Đây là điểm khác biệt chính so với `filter`: `filter` xóa event không thỏa điều kiện, còn `route` chuyển hướng chúng sang đích khác để xử lý tiếp.

---

### 6.4 aggregate — Gom nhóm metric theo thời gian

`aggregate` gom nhiều metric event trong một khoảng thời gian thành một metric event duy nhất. Transform này hoạt động trên **metric event** (counter, gauge, histogram) — không phải log event. Nó gom các metric có cùng tên và tags trong mỗi cửa sổ thời gian: counter được cộng lại, gauge giữ giá trị mới nhất, sau đó một event tổng hợp được emit ra cuối mỗi chu kỳ.

```yaml
# vector.yaml — gom metric theo 60 giây: internal_metrics → aggregate → console
sources:
  internal:
    type: internal_metrics   # nguồn metric nội bộ của Vector (counter, gauge, v.v.)

transforms:
  gom_metric:
    type: aggregate
    inputs:
      - internal
    interval_ms: 60000        # gom metric trong 60 giây, sau đó emit 1 event tổng hợp

sinks:
  out:
    type: console
    inputs:
      - gom_metric
    encoding:
      codec: json
```

> **Khi nào dùng:** Khi bạn nhận nhiều metric event cùng tên từ nhiều nguồn và muốn giảm số event gửi đến sink bằng cách gom chúng lại theo khoảng thời gian. Giúp giảm chi phí lưu trữ và giảm tải cho sink. Lưu ý: `aggregate` dành cho metric event — nếu cần gom nhiều log line thành một event duy nhất, dùng `reduce` thay thế.

> **Ví dụ thực tế:** Thay vì ghi mỗi counter event riêng lẻ vào Prometheus, dùng `aggregate` với `interval_ms: 10000` để gom counter trong 10 giây thành một giá trị tổng — giảm số lần ghi xuống đáng kể mà không mất thông tin quan trọng.

---

### 6.5 sample — Lấy mẫu giảm volume

`sample` giữ lại 1 trong N event ngẫu nhiên, bỏ phần còn lại. Dùng khi volume log quá cao và bạn chấp nhận mất một phần data không quan trọng để tiết kiệm chi phí lưu trữ và băng thông.

Trường `rate` xác định tỉ lệ: `rate: 10` nghĩa là giữ 1 event trong 10 — tức 10% data được giữ lại, 90% bị bỏ. `rate: 1` giữ tất cả (không sample). `rate: 100` chỉ giữ 1%.

```yaml
# vector.yaml — sample 10% request thành công: file → remap → sample → console
sources:
  nginx_access:
    type: file
    include:
      - /var/log/nginx/access.log

transforms:
  parse_nginx:
    type: remap
    inputs:
      - nginx_access
    source: |
      . = parse_nginx_log!(string!(.message), format: "combined")

  sample_ok_requests:
    type: sample
    inputs:
      - parse_nginx               # nhận event từ transform "parse_nginx"
    rate: 10                      # giữ lại 1 event trong 10 (10% data)

sinks:
  out:
    type: console
    inputs:
      - sample_ok_requests
    encoding:
      codec: json
```

> **Khi nào dùng:** Log 200 OK chiếm 95% volume nhưng ít giá trị debug trong điều kiện bình thường. Kết hợp `route` và `sample`: dùng `route` để tách luồng lỗi và luồng thành công, giữ 100% event lỗi, áp `sample` lên luồng thành công với `rate: 10` để chỉ giữ 10%. Nhờ đó bạn không bỏ sót lỗi nào trong khi vẫn giảm đáng kể chi phí lưu trữ.

> **Ví dụ thực tế:** Một API gateway nhận 50.000 request/phút; 48.000 là 200 OK, 2.000 là lỗi. Nếu bạn sample OK requests ở `rate: 10`, bạn chỉ lưu khoảng 4.800 OK log/phút thay vì 48.000 — tiết kiệm 90% storage cho phần data ít giá trị nhất — trong khi vẫn giữ toàn bộ 2.000 error log/phút để debug.

---

### 6.6 dedupe — Loại bỏ event trùng lặp

`dedupe` bỏ qua event trùng lặp dựa trên giá trị của các field chỉ định. Khi hai event có cùng giá trị ở các field được liệt kê trong `fields.match`, event thứ hai bị bỏ qua. Transform dùng bộ nhớ đệm LRU (Least Recently Used cache) có kích thước giới hạn — mặc định 5000 event.

Giới hạn này có nghĩa là: `dedupe` chỉ hoạt động tốt với **duplicate liên tiếp hoặc gần nhau về thời gian**. Nếu hai event giống hệt nhau nhưng cách nhau hơn 5000 event khác, bộ nhớ đệm sẽ đã bị đẩy ra và event thứ hai sẽ được coi là mới.

```yaml
# vector.yaml — loại bỏ duplicate request: file → remap → dedupe → console
sources:
  nginx_access:
    type: file
    include:
      - /var/log/nginx/access.log

transforms:
  parse_nginx:
    type: remap
    inputs:
      - nginx_access
    source: |
      . = parse_nginx_log!(string!(.message), format: "combined")

  remove_duplicates:
    type: dedupe
    inputs:
      - parse_nginx               # nhận event từ transform "parse_nginx"
    fields:
      match:
        - client                  # nếu cùng IP...
        - request                 # ...và cùng request...
        - status                  # ...và cùng status thì bỏ event trùng

sinks:
  out:
    type: console
    inputs:
      - remove_duplicates
    encoding:
      codec: json
```

> **Khi nào dùng:** Khi client retry và gửi cùng một request nhiều lần liên tiếp, hoặc khi Vector restart và đọc lại phần cuối file log gây ra duplicate trong thời gian ngắn. Không thích hợp để phát hiện duplicate phân tán qua thời gian dài — bộ nhớ đệm có giới hạn và trạng thái mất khi Vector restart.

> **Ví dụ thực tế:** Một script gặp lỗi và retry liên tục, gửi cùng một HTTP request thất bại 20 lần trong 5 giây. Thay vì ghi 20 event giống hệt nhau vào Loki, `dedupe` với `fields.match: [client, request, status]` chỉ giữ lại event đầu tiên và bỏ 19 event còn lại — giảm noise trong log khi debug.

---

## 7. Viết Vector Config từ đầu

Một file `vector.yaml` có cấu trúc ba khối song song ở cùng cấp: `sources`, `transforms`, và `sinks`. Không có thứ bậc hay wrapper bên ngoài — ba khối này đều là key ở cấp cao nhất của file YAML. Các component trong từng khối kết nối với nhau qua trường `inputs`: mỗi transform hoặc sink khai báo tên của component phía trước mà nó nhận dữ liệu từ đó. Vector đọc toàn bộ file, xây dựng đồ thị kết nối, rồi kiểm tra tính hợp lệ trước khi bắt đầu chạy — nếu có tên component nào sai, Vector từ chối khởi động và in ra lỗi rõ ràng.

**Quy trình viết config 5 bước:**

1. **Xác định nguồn dữ liệu** — log từ đâu? (file, syslog, HTTP?) → chọn source type
2. **Xác định đích** — gửi đi đâu? (Loki, Elasticsearch, file?) → chọn sink type
3. **Xác định biến đổi cần thiết** — cần parse không? Lọc gì? Phân luồng không? → chọn transform(s)
4. **Kết nối bằng `inputs`** — mỗi transform/sink khai báo nó lấy data từ component nào
5. **Test với `console` sink** — thêm sink console tạm thời để xem event trước khi dùng sink thật

**Ví dụ hoàn chỉnh: đọc nginx log, lọc lỗi, gửi vào Loki với sampling**

Bài toán: đọc nginx access log, tách riêng lỗi 5xx, gửi toàn bộ lỗi vào Loki và chỉ gửi 20% traffic bình thường (để tiết kiệm lưu trữ).

```yaml
# vector.yaml — bài toán: đọc nginx log, lọc lỗi, gửi vào Loki với sampling
# Luồng: nginx_access → parse_nginx → split_errors
#                                    ├─ errors  → loki_errors
#                                    └─ normal  → sample_normal → loki_normal

sources:
  nginx_access:
    type: file                          # đọc từ file trên disk như tail -f
    include:
      - /var/log/nginx/access.log       # đường dẫn file log nginx

transforms:
  # Bước 1: parse dòng log thô thành fields có cấu trúc
  parse_nginx:
    type: remap
    inputs:
      - nginx_access                    # nhận event thô từ source
    source: |
      # parse_nginx_log! tách .message thành các field rời rạc:
      # .client, .request, .status, .size, .referer, .agent, ...
      . = parse_nginx_log!(string!(.message), format: "combined")

  # Bước 2: tách thành hai luồng — lỗi 5xx và traffic bình thường
  split_errors:
    type: route
    inputs:
      - parse_nginx                     # nhận event đã parse từ bước 1
    route:
      errors: '.status >= 500'          # output: split_errors.errors
      normal: '.status < 500'           # output: split_errors.normal

  # Bước 3: sample luồng bình thường để giảm volume (giữ 20%)
  sample_normal:
    type: sample
    inputs:
      - split_errors.normal             # chỉ sample luồng bình thường, không đụng đến lỗi
    rate: 5                             # giữ 1 trong 5 event = 20% data

sinks:
  # Lỗi 5xx: gửi tất cả vào Loki — không sample để không bỏ sót lỗi nào
  loki_errors:
    type: loki
    inputs:
      - split_errors.errors             # nhận từ route output "errors"
    endpoint: "http://loki:3100"
    encoding:
      codec: json
    labels:
      app: nginx
      severity: error                   # label Loki để lọc theo loại log trong Grafana

  # Traffic bình thường: chỉ gửi 20% sample vào Loki để tiết kiệm lưu trữ
  loki_normal:
    type: loki
    inputs:
      - sample_normal                   # nhận từ transform sample_normal
    endpoint: "http://loki:3100"
    encoding:
      codec: json
    labels:
      app: nginx
      severity: info                    # label riêng để phân biệt với lỗi trong Grafana
```

> **Ví dụ thực tế:** Bắt đầu với pipeline đơn giản nhất (source → console sink), xác nhận data đang chảy qua và event trông đúng như mong đợi, rồi từng bước thêm transforms. Đừng cố viết pipeline phức tạp ngay từ đầu — thêm một transform, chạy thử, kiểm tra output console, rồi mới thêm transform tiếp theo. Khi output đã đúng, thay `type: console` bằng sink thật.

---

## 8. Bảng quyết định: Chọn transform nào?

Khi đối mặt với một bài toán xử lý log, câu hỏi đầu tiên thường là: "Nên dùng transform nào?" Bảng dưới đây tổng hợp các tình huống phổ biến và transform phù hợp, kèm lý do ngắn gọn để bạn ra quyết định nhanh mà không cần đọc lại toàn bộ tài liệu.

| Tình huống | Transform | Lý do |
|---|---|---|
| Log thô cần parse thành fields có cấu trúc | `remap` | VRL có sẵn hàm: `parse_nginx_log!`, `parse_syslog!`, `parse_json!` |
| Thêm/sửa/xóa field trong event | `remap` | VRL expression: `.field = value`, `del(.field)` |
| Chỉ muốn giữ một loại event, bỏ phần còn lại | `filter` | Event bị bỏ hoàn toàn — không gửi đi đâu cả |
| Muốn gửi event đến nhiều sink khác nhau | `route` | Mỗi route là output riêng (`tên_transform.tên_route`) |
| Volume quá cao, muốn giảm bằng lấy mẫu | `sample` | Giữ 1/N event; dùng sau `route` để không mất event quan trọng |
| Muốn đếm/tổng hợp metric theo khoảng thời gian | `aggregate` | Chỉ cho metric event; dùng kết hợp với `internal_metrics` source |
| Gom nhiều event liên quan thành một | `reduce` | Dùng khi nhiều dòng log thuộc 1 transaction |
| Log bị duplicate do retry hoặc restart | `dedupe` | So sánh theo field chỉ định; chỉ hiệu quả với duplicate gần nhau |
| Cần làm nhiều việc phức tạp cùng lúc | Nhiều `remap` nối tiếp | Chia nhỏ: mỗi remap một việc, dễ debug hơn |

**Nguyên tắc chung:**

- Bắt đầu với `remap` — nó giải quyết được 80% nhu cầu
- Kết hợp `route` + `filter` để kiểm soát luồng event
- Dùng `sample` ở cuối pipeline (sau khi đã parse và route) để không mất event quan trọng
- `dedupe` và `aggregate` thường dùng ở các pipeline chuyên biệt, không phải mặc định

---

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

Field `.message` chứa nội dung thô của dòng log. Các transform — đặc biệt là `remap` dùng ngôn ngữ VRL (Vector Remap Language) — giúp bạn tách `.message` thành các field có ý nghĩa như `.status`, `.path`, `.remote_addr`. Trong VRL, bạn truy cập field bằng ký hiệu dấu chấm: `.message`, `.status`, `.timestamp`. Toàn bộ event được biểu diễn bằng dấu chấm `.` (dot) — khi bạn gán `. = ...` nghĩa là bạn đang thay thế toàn bộ event.

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

> **Ví dụ thực tế:** Một server nginx ghi log vào `/var/log/nginx/access.log`. Vector dùng source `file` để theo dõi file đó liên tục (giống `tail -f`) và tạo một event cho mỗi dòng mới. Event chứa dòng log thô trong field `.message` — các transform phía sau sẽ parse `.message` thành các field có cấu trúc như `.status`, `.path`, `.client`.

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

> *Nội dung chương này sẽ được hoàn thiện trong phần tiếp theo.*

---

## 7. Viết Vector Config từ đầu

> *Nội dung chương này sẽ được hoàn thiện trong phần tiếp theo.*

---

## 8. Bảng quyết định: Chọn transform nào?

> *Nội dung chương này sẽ được hoàn thiện trong phần tiếp theo.*

---

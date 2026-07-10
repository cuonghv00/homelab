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

> *Nội dung chương này sẽ được hoàn thiện trong phần tiếp theo.*

---

## 5. Sinks — Nơi dữ liệu ra

> *Nội dung chương này sẽ được hoàn thiện trong phần tiếp theo.*

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

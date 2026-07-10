# Design Spec: Vietnamese Vector Guide

**Date:** 2026-07-10  
**Status:** Approved

## Goal

Produce two files in `docs/vector/`:
1. `vector-guide-vi.md` — full Vietnamese operator guide for Vector
2. `vector-skill.md` — concise AI skill material (raw content, not packaged as a skill yet)

## Audience

Junior operators: know Linux/Kubernetes/YAML but have never used Vector and need concepts explained from scratch (pipeline, source, sink) before getting to transforms.

## Scope

- Skip installation
- Cover: how Vector works, how transforms work, factors affecting transform selection
- End goal: operator can write a working `vector.yaml` config

## Content Sources (Deep Research)

- `https://vector.dev/guides/` — guides overview
- `https://vector.dev/docs/reference/configuration/transforms/` — transform reference
- `https://vector.dev/docs/reference/vrl/` — VRL language
- `https://vector.dev/guides/level-up/` — advanced patterns

## Structure: `vector-guide-vi.md` (~2500–3500 words)

```
1. Vector là gì?
   - Định nghĩa ngắn, vị trí trong observability stack
   - So sánh 1 câu với Fluentd/Filebeat

2. Mô hình Pipeline
   - Source → Transform → Sink (ASCII diagram)
   - Khái niệm "event" là đơn vị dữ liệu cơ bản

3. Events — Dữ liệu trong Vector
   - Log event vs Metric event
   - Cấu trúc fields, metadata
   - Ví dụ event thực tế (nginx access log)

4. Sources — Nơi dữ liệu vào
   - file, stdin, http, kubernetes_logs, syslog
   - YAML config example + giải thích

5. Sinks — Nơi dữ liệu ra
   - console, file, http, loki, elasticsearch
   - YAML config example + giải thích

6. Transforms — Biến đổi dữ liệu (phần trọng tâm)
   6.1 remap / VRL — phân tích, sửa đổi field (quan trọng nhất)
   6.2 filter — loại bỏ event không cần
   6.3 route — phân luồng tới nhiều sink
   6.4 aggregate / reduce — gom nhóm event
   6.5 sample — lấy mẫu giảm volume
   6.6 dedupe — loại trùng lặp

7. Viết Vector Config từ đầu
   - Cấu trúc file vector.yaml
   - Kết nối components với `inputs`
   - Ví dụ hoàn chỉnh đầu đến cuối

8. Bảng quyết định: Chọn transform nào?
   - Bảng: Tình huống → Transform → Lý do
```

## Structure: `vector-skill.md` (~600–900 words)

```
- Quick reference: key concepts, event model
- Transform decision table (compact)
- Reusable YAML config patterns/templates
- Common pitfalls
```

## Per-Chapter Template

Each chapter follows this consistent pattern:
1. Concept explanation (Vietnamese prose, 1-2 paragraphs)
2. ASCII diagram if helpful (pipeline flow, event structure)
3. Minimal YAML code block
4. Inline comments in Vietnamese explaining each line
5. "Ví dụ thực tế" block — concrete scenario

## Language & Terminology

- Vietnamese technical writing
- Keep English terms when no good Vietnamese equivalent: `source`, `sink`, `transform`, `event`, `pipeline`
- Introduce terms on first use: `source (nguồn dữ liệu)`
- Never translate component names: `remap`, `filter`, `route`, `aggregate`

## Config Format

- **YAML only** (file named `vector.yaml`)
- Examples small enough to read in 30 seconds
- Every example includes a complete source → transform → sink chain (no orphaned snippets)

## Out of Scope

- Installation / deployment
- Homelab-specific examples (generic only)
- Packaging the skill file (just raw material)

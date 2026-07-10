# Design Spec: Vector Advanced Patterns & Transport Optimization

**Date:** 2026-07-10
**Status:** Approved

## Goal

Produce two new files in `docs/vector/`:
1. `vector-patterns-vi.md` — production transform/parsing patterns catalog (Vietnamese)
2. `vector-transport-vi.md` — sink config & transport optimization guide (Vietnamese)

Plus update `vector-skill.md` with advanced patterns from real configs.

## Source Material

27 real production Vector configs in `docs/vector/real-config/`, covering:
- nginx agent/aggregator, haproxy agent/aggregator, kong aggregator
- mariadb agent/aggregator, vault agent/aggregator
- napas/epass/mskpp/cdcn agents and aggregators
- clickhouse aggregator, transaction processor, custom configs

## Architecture Pattern (from real configs)

```
[App/Service] → [Vector Agent] → [Kafka] → [Vector Aggregator] → [Elasticsearch / ClickHouse]
     file/syslog/socket          zstd        native/json              gzip / native
```

## Audience

- **Primary:** Operators who have read `vector-guide-vi.md` and want production-grade patterns
- **Secondary:** Senior DevOps who need a reference without concept explanations
- **Format:** Brief context at start of each section (for beginners), then dense reference content

## Approach

Workflow-driven (Approach B): patterns presented in the order they appear in a real production workflow.

## Document 1: `vector-patterns-vi.md`

### Structure

```
## 0. Architecture: Agent → Kafka → Aggregator
  - Workflow diagram (ASCII)
  - Why Kafka as middleware: decoupling, buffering, replay
  - When NOT to use Kafka (direct agent→sink cases)

## 1. Parsing Patterns
  1.1 Custom regex với parse_regex (nginx custom format example)
  1.2 Two-step parsing (extract timestamp → parse JSON inside)
  1.3 Multiline log assembly (MariaDB slow query, InnoDB)
  1.4 Payload unwrapping (nested .payload → root merge)
  1.5 Built-in parsers vs custom regex (decision guide)

## 2. VRL Advanced Techniques
  2.1 Merge patterns: |= object(parsed) ?? {} vs merge(., x)
  2.2 Error handling tiers: ! vs ?? vs if err != null
  2.3 Conditional field normalization (.upstream_status == "-")
  2.4 URL/string manipulation: split, replace, contains
  2.5 Timestamp normalization: parse_timestamp, format_timestamp, to_unix_timestamp
  2.6 Fan-in: multiple inputs → one transform

## 3. Error Handling Strategy
  3.1 drop_on_error: true — effect and limitations
  3.2 Pre-parse content filter (abort on PROMEX, etc.)
  3.3 Structured error logging with log() in VRL

## 4. File Source: Fingerprinting & Tracking
  4.1 checksum vs device_and_inode
  4.2 ignore_older_secs, ignore_not_found
  4.3 multiline: start_pattern, condition_pattern, mode, timeout_ms

## 5. Self-Monitoring
  5.1 internal_metrics source
  5.2 prometheus_exporter sink
  5.3 Expose Vector metrics to Prometheus/Grafana
```

### Content Format Per Section

```
### [Pattern Name]

[1-2 sentence context — when you need this]

[Real config snippet — IPs/passwords/hostnames redacted]

[Explanation of key fields in Vietnamese]

> **Pitfall / Trade-off:** [one concrete gotcha]
```

### Language

- Vietnamese prose, English terms preserved (`parse_regex`, `drop_on_error`, etc.)
- All config snippets redacted (no IPs, no passwords, no internal hostnames)
- Each section ends with a pitfall or trade-off callout

---

## Document 2: `vector-transport-vi.md`

### Structure

```
## 0. Aggregator Pattern
  - Kafka consumer → transform → sink workflow
  - Standard Kafka metadata cleanup fields

## 1. Kafka Configuration
  ### Producer (Agent side)
  - batch: max_bytes / max_events / timeout_secs
  - compression: zstd vs gzip vs none
  - Topic naming

  ### Consumer (Aggregator side)
  - group_id strategy
  - decoding.codec: native vs json
  - Cleanup: del(.topic), del(.partition), del(.offset), del(.message_key), del(.headers)

## 2. Elasticsearch Sink ★ Deep analysis
  - bulk.action: create vs index
  - api_version: v8
  - compression: gzip
  - batch: max_bytes / timeout_secs
  - request: timeout_secs / concurrency (fixed vs adaptive)
  - healthcheck.enabled: false
  - Trade-off table: throughput vs latency vs ES cluster load

## 3. ClickHouse Sink ★ Deep analysis
  - request.concurrency: adaptive — how it works
  - skip_unknown_fields: true
  - decoding.codec: native on Kafka source
  - to_unix_timestamp() requirement
  - Batch config differences vs ES

## 4. Compression Strategy
  - zstd (agent→Kafka): speed vs ratio
  - gzip (aggregator→ES/CH): compatibility, ES native decompression
  - Trade-off table: CPU cost vs compression ratio vs latency

## 5. Concurrency & Batching Trade-offs
  - Fixed concurrency (2) vs adaptive — when each is appropriate
  - Batch sizing: max_bytes vs max_events vs timeout_secs interaction
  - Impact of batch size on ES indexing latency and ClickHouse insert performance
```

### Content Format Per Section (transport doc)

```
### [Config Parameter / Feature]

[What it does — 1 sentence]

**Real config (from production):**
```yaml
[redacted snippet]
```

**Verified from docs:** [what the official docs say about valid ranges, defaults, behavior]

**Trade-off:**
| Option | Throughput | Latency | Backend load |
|--------|-----------|---------|--------------|
| ...    | ...       | ...     | ...          |

**Recommendation:** [concrete guidance]
```

---

## Deep Research URLs

For `vector-transport-vi.md` verification:
- `https://vector.dev/docs/reference/configuration/sinks/elasticsearch/`
- `https://vector.dev/docs/reference/configuration/sinks/clickhouse/`
- `https://vector.dev/docs/reference/configuration/sinks/kafka/`
- `https://vector.dev/docs/reference/configuration/sources/kafka/`
- `https://vector.dev/docs/reference/configuration/sources/file/` (fingerprint, multiline)
- `https://vector.dev/docs/about/under-the-hood/networking/` (concurrency model)

For `vector-patterns-vi.md` VRL verification:
- `https://vector.dev/docs/reference/vrl/functions/` (merge, object, parse_regex, format_timestamp, to_unix_timestamp)

---

## Update: `vector-skill.md`

Add a new section at the end:

```
## Advanced Production Patterns (Quick Reference)

| Pattern | When | Key config |
|---------|------|------------|
| Multiline assembly | Multi-line logs (slow query, stack trace) | file.multiline.start_pattern |
| Two-step parse | Timestamp prefix + JSON body | parse_regex → parse_json |
| Payload unwrap | Nested {payload: {...}} | if exists(.payload) { . = merge!(., .payload) } |
| Fan-in | Multiple sources → one transform | inputs: [src1, src2] |
| Pre-parse filter | Abort noisy events early | if contains(.message, "X") { abort } |

## Transport Quick Reference

| Sink | Action | Compression | Concurrency |
|------|--------|-------------|-------------|
| Elasticsearch | bulk.action: create | gzip | fixed 2 |
| ClickHouse | - | gzip | adaptive |
| Kafka (producer) | - | zstd | - |
```

---

## Out of Scope

- Installation / deployment of Kafka, Elasticsearch, ClickHouse
- Vector Kubernetes deployment (separate topic)
- Alerting / routing transforms (covered in vector-guide-vi.md)
- Specific internal service names or IP addresses from real configs

# Vector — AI Skill Reference

## What Vector Is

Vector is an observability pipeline tool that collects logs and metrics from sources, transforms them, and sends them to sinks. Written in Rust; config file is `vector.yaml` (YAML format, not TOML). Pipeline model: Source → Transform(s) → Sink; components connect via the `inputs` field; each unit of data flowing through the pipeline is an **event** (log or metric).

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

All three blocks (`sources`, `transforms`, `sinks`) are top-level YAML keys at the same level. Sources never declare `inputs`; transforms and sinks always must.

## Key Source Types

| type | use case | key fields |
|---|---|---|
| `file` | Tail a log file on disk (like `tail -f`) | `include: [path]`, `read_from: beginning` |
| `stdin` | Receive data piped to Vector's stdin | (none required) |
| `syslog` | Listen for syslog messages over UDP/TCP | `mode: udp/tcp`, `address: "0.0.0.0:514"` |
| `http` | Receive events via HTTP POST | `address`, `path` |
| `kubernetes_logs` | Collect logs from all pods on a K8s node | (auto-discovers pods via node API) |

## Key Sink Types

| type | use case | key fields |
|---|---|---|
| `console` | Print events to stdout — use for debug/verify | `encoding.codec: json` |
| `file` | Write events to disk; supports date/time path templates | `path`, `encoding.codec` |
| `loki` | Ship logs to Grafana Loki | `endpoint`, `labels` |
| `elasticsearch` | Index logs in Elasticsearch or OpenSearch | `endpoints`, `index` |
| `http` | POST events to any HTTP endpoint | `uri`, `method`, `encoding` |

## Transform Decision Table

| Need | Transform | Key config |
|---|---|---|
| Parse raw log string into structured fields | `remap` | `source: '. = parse_nginx_log!(.message, format: "combined")'` |
| Add, modify, or delete fields in an event | `remap` | `.field = value` or `del(.field)` in VRL source |
| Drop events matching a condition (permanently) | `filter` | `condition: '.status >= 400'` |
| Split one stream into multiple named outputs | `route` | `route: {name: 'condition'}` → reference as `transform.route_name` in inputs |
| Reduce volume by keeping 1-in-N events | `sample` | `rate: 10` keeps 10% of events; apply after routing, not before |
| Aggregate metric events over a time window | `aggregate` | `interval_ms: 60000`; works on metric events only, not log events |
| Merge multiple related log lines into one event | `reduce` | Groups events by shared field; emits one merged event |
| Drop duplicate events (bounded, nearby only) | `dedupe` | `fields.match: [field1, field2]` — LRU cache, default 5000 entries |

## VRL Quick Reference

```vrl
# Field access and mutation
.field_name              # read a field value
.new_field = "value"     # set (or create) a field
del(.field_name)         # delete a field from the event
exists(.field_name)      # returns boolean — true if field is present

# . (dot) represents the entire event
. = parse_nginx_log!(string!(.message), format: "combined")
# ↑ replaces the whole event with parsed fields

# Parse functions — ! means abort-on-error (drop the event if parsing fails)
. = parse_nginx_log!(string!(.message), format: "combined")
. = parse_syslog!(string!(.message))
. = parse_json!(string!(.message))

# Type conversion
to_string(.status)        # infallible — returns string
to_int!(.bytes_sent)      # fallible — aborts event if conversion fails
downcase(string!(.method))

# Conditionals
if .status >= 400 {
  .is_error = true
}
```

The `!` suffix on any VRL function means: if the function returns an error, abort processing and drop the event. Without `!`, the function returns a `Result` type, not the unwrapped value — a common source of silent bugs.

## Route Output Naming

Reference a route's output using dot notation: `transform_name.route_name`. Events matching no route go to `_unmatched`.

```yaml
transforms:
  split_by_status:
    type: route
    inputs: [parse_nginx]
    route:
      errors:   '.status >= 500'
      warnings: '.status >= 400 && .status < 500'
      ok:       '.status < 400'

sinks:
  error_sink:
    type: loki
    inputs:
      - split_by_status.errors       # dot-notation: transform_name.route_name
  normal_sink:
    type: loki
    inputs:
      - split_by_status.ok
      - split_by_status.warnings
      - split_by_status._unmatched   # events that matched no route condition
```

## Common Pitfalls

- **Missing `!` on VRL fallible functions**: `parse_nginx_log(...)` without `!` returns a `Result` type, not the parsed object — the event will not be transformed correctly. Always use `parse_nginx_log!(...)`, `parse_json!(...)`, etc. to unwrap or abort.
- **Wrong `inputs` name**: the name in `inputs` must exactly match the declared component name (case-sensitive). A typo silently produces an empty pipeline; Vector will error on startup.
- **`filter` silently drops events**: dropped events are gone with no trace. Always wire a `console` sink to verify events are flowing through the correct branch before adding a `filter`.
- **`sample` before `route`**: sampling before routing can discard high-priority events (errors). Always `route` first to isolate important streams, then apply `sample` only to low-priority output.
- **`dedupe` only catches nearby duplicates**: the LRU cache has a bounded size (default 5000 events). Two identical events separated by more than 5000 other events will both be passed through. State is also lost on Vector restart.
- **`aggregate` only works on metric events**: passing log events through `aggregate` has no effect. For merging multiple log lines into one, use `reduce` instead.

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

## Kafka Metadata Cleanup

Always delete Kafka metadata after source:

```vrl
del(.source_type); del(.topic); del(.partition); del(.offset)
del(.message_key); del(.headers); del(.metadata); del(.message)
```

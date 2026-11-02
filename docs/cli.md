# tadpole-mt CLI reference

Global options: `--config <path>`, `--db <path>` (env `TADPOLE_MT_CONFIG`, `TADPOLE_MT_DB`).

Output: JSON (pretty) or NDJSON on stdout. Errors: `{"error":"<message>"}` on stderr.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | success |
| 1 | error |
| 2 | usage error |
| 3 | radio device not found |
| 4 | daemon not reachable |

## Commands

| Command | Needs | Output |
|---|---|---|
| `devices` | — | array: bus, address, vid, pid, product, serial, device_id, open_error |
| `config [--write]` | — | effective config, derived LoRa parameters (freq_hz, bw_hz, sf, cr, sync_word, ldro, channel hashes), paths |
| `daemon` | radio | foreground; lifecycle events as NDJSON on stderr. Stops on Ctrl+C |
| `status` | daemon | pid, session_id, usb_serial, radio parameters, frames received |
| `listen [--ok-only] [--seconds N]` | daemon | NDJSON frame events (below) |
| `packets [--since S] [--from ID] [--port P] [--status ST] [--dups] [--limit N]` | DB | `v_packets` rows, newest first. Duplicates excluded unless `--dups` |
| `nodes [--since S]` | DB | node summary rows, most recently heard first; `age_s` = seconds since last heard |
| `node <ID>` | DB | `{node, device_metrics[≤50]}` |
| `sql "<query>"` | DB | rows as JSON array. Read-only connection. `*_json` columns are returned as JSON |
| `schema` | — | DDL |
| `decode <hex>` | — | decode a raw frame with configured channels |

Node IDs: `!xxxxxxxx`, `xxxxxxxx`, `0xxxxxxxxx`, decimal, `^all` (broadcast).

`--since S` is seconds before now.

## Frame event (listen)

```json
{"frame_id":1,"rx_time":1793653795.73,"status":"ok",
 "from":"!aa7bc52c","to":"^all","id":2340549495,
 "hop_start":4,"hop_limit":0,"hops_taken":4,"relay_node":150,"next_hop":0,"want_ack":false,"via_mqtt":false,
 "channel_hash":85,"channel":"EdgeFastLow","port":"NODEINFO_APP",
 "data":{"portnum":"NODEINFO_APP","payload":{...},"payload_type":"User"},
 "rssi":-43.0,"snr":13.25,"freq_error_hz":-459.0,"len":99,"airtime_ms":967.7,"dup_of":null}
```

- `data`: `meshtastic.Data` as proto3 JSON (proto field names, default values omitted). `payload` decoded per port: protobuf → object (`payload_type` = message name), text ports → string, unknown → hex.
- `hops_taken` = `hop_start - hop_limit`; null if sender did not set `hop_start`.
- `relay_node` / `next_hop`: last byte of the relaying / intended next-hop node number (0 = none).
- `dup_of`: frame id of the first reception of the same (`from`,`id`) within 1 h; null for first reception.
- `header_error` events carry only `frame_id`, `rx_time`, `status`, `rssi` (instantaneous).

## Daemon API

TCP `config.api` (default `127.0.0.1:47403`), one JSON object per line.

| Request | Response |
|---|---|
| `{"cmd":"status"}` | one status object |
| `{"cmd":"subscribe"}` | `{"subscribed":true}` then frame events until disconnect |

One daemon per stick; a second daemon fails to bind the API port.

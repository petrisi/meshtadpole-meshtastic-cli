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
| `config [--write] [--set PATH=VALUE]...` | — | effective config (private key redacted), derived LoRa parameters (freq_hz, bw_hz, sf, cr, sync_word, ldro, channel hashes), paths. `--set` writes the file; value parsed as JSON else string; `node.num` accepts node ids. Restart daemon to apply |
| `identity` | — | our node num/id, names, role, public key derived from `node.private_key` |
| `send <TO> <TEXT> [--channel N] [--no-pkc] [--no-ack]` | daemon | send result (below). Unicast uses PKC when the destination key is known |
| `nodeinfo <TO>` | daemon | send our User (incl. public key) over the channel with want_response |
| `admin <NODE> get <ITEM>...` | daemon | `{node, results[]}`; one admin request per item. Progress lines on stderr |
| `admin <NODE> raw '<JSON>'` | daemon | arbitrary `meshtastic.AdminMessage` as proto3 JSON |
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

Wait options for `send`, `nodeinfo`, `admin`: `--wait S` (per transmission, default 60), `--retries N` (retransmissions of the same packet id if nothing heard, default 1), `--hop-limit N`.

## Admin items

`owner`, `metadata`, `channel:<0-7>`, `config:<name>`, `module:<name>`; groups `channels`, `configs`, `modules`, `all`.

Config names: device position power network display lora bluetooth security sessionkey deviceui.
Module names: mqtt serial extnotif storeforward rangetest telemetry cannedmsg audio remotehardware neighborinfo ambientlighting detectionsensor paxcounter statusmessage trafficmanagement tak meshbeacon.

Admin requests are PKC-encrypted with `node.private_key`; the target must list our public key in `security.admin_key`. On NAK `PKI_UNKNOWN_PUBKEY` the CLI sends our NodeInfo and retries once (`key_exchange` in result). Firmware 2.7.26 required this.

Responses include `session_passkey`; set operations must echo it within 300 s (not yet exposed).

## Redaction

Config `redact_fields` (default `["private_key","wifi_psk"]`): decoded payload fields with these names, at any nesting level, are replaced with `<redacted>` before storage (`rx_frames.data_json`, `requests.result_json`) and output. Matching is by field name only. Firmware returns the node private key to authorized admins in `get_config_response.security`.

- Set: `config --set 'redact_fields=["private_key","wifi_psk","fixed_pin"]'`. Restart daemon to apply.
- Not covered: `rx_frames.raw` (encrypted frame; decryptable with `node.private_key`), rows stored before a field was added.
- Admin writes do not yet exist; handling of redacted values in writes is unspecified.

## Send result

```json
{"packet_id":3447972213,"to":"!59a5f896","port":"TEXT_MESSAGE_APP","encryption":"pkc",
 "transmissions":[{"tx_frame_id":1,"tx_time":...,"attempt":0,"len":36,"airtime_ms":443.4}],
 "acked":true,"nak":null,"acks":[<frame event>],"responses":[<frame event>],
 "relays_heard":[{"relay_node":212,"hop_limit":2,"rssi":-30,"snr":11.25,"rx_time":...,"frame_id":...}],
 "complete":true}
```

- `acks`: ROUTING_APP frames with `request_id` = packet_id. `nak` = `error_reason` if not NONE.
- `responses`: first non-Routing frame with `request_id` = packet_id.
- `relays_heard`: our packet heard rebroadcast (implicit ACK, relay coverage).
- Broadcasts complete after `--wait`.

## Transmit policy

- CSMA: TX deferred while a preamble/header is being received; random 0–400 ms initial backoff.
- Duty cycle: TX refused when own airtime in the last hour would exceed `node.duty_cycle_limit` (default 0.10). Disable for lab use: `config --set node.duty_cycle_enforce=false`.
- Power: `radio.tx_power_dbm` (default 14, max 22).
- This node never rebroadcasts. `node.role` is only advertised.

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
| `{"cmd":"send","to":N,"port":"NAME","payload_b64":"..","want_ack":b,"want_response":b,"pkc":b\|null,"channel":i,"hop_limit":n\|null,"wait_s":f,"retries":n}` | send result |

One daemon per stick; a second daemon fails to bind the API port.

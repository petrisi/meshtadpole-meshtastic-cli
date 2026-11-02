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
| `request <TO> <KIND>` | daemon | request result (see Requests) |
| `nodeinfo <TO>` | daemon | send our User (incl. public key) over the channel with want_response |
| `admin <NODE> get <ITEM>...` | daemon | `{node, results[]}`; one admin request per item. Progress lines on stderr |
| `admin <NODE> raw '<JSON>'` | daemon | arbitrary `meshtastic.AdminMessage` as proto3 JSON |
| `admin <NODE> set <ITEM> '<CHANGES>' [--apply] [--allow-disruptive]` | daemon | read-modify-write result (see Admin writes) |
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

Responses include `session_passkey`; set operations must echo it within 300 s. `admin raw` with a non-get message and no `session_passkey` first reads `config:sessionkey` to obtain one.

## Redaction

Config `redact_fields` (default `["private_key","wifi_psk"]`): decoded payload fields with these names, at any nesting level, are replaced with `<redacted>` before storage (`rx_frames.data_json`, `requests.result_json`) and output. Matching is by field name only. Firmware returns the node private key to authorized admins in `get_config_response.security`.

- Set: `config --set 'redact_fields=["private_key","wifi_psk","fixed_pin"]'`. Restart daemon to apply.
- Not covered: `rx_frames.raw` (encrypted frame; decryptable with `node.private_key`), rows stored before a field was added.
- Writes never take secret values from the caller; see Admin writes.

## Admin writes

`admin <NODE> set <ITEM> '<CHANGES>' [--apply] [--allow-disruptive] [--verify-delay S]`

Firmware replaces a whole config section on set. Sequence (daemon `admin_set`):

1. Fresh get of ITEM; unredacted value and `session_passkey` held in daemon memory only.
2. CHANGES merged: objects merge recursively, arrays and scalars replace, `null` resets to default.
3. Diff computed (proto3 defaults and absent fields are equal). No diff → nothing sent.
4. Guards:
   - CHANGES containing `<redacted>` → error.
   - `config:security`: merged `private_key` must be 32 bytes (2.7.x generates a new keypair otherwise); our public key must remain in `admin_key`.
   - `config:security`, `config:lora`, `channel:0` → refused without `--allow-disruptive`.
5. Without `--apply`: dry run, returns before/after/diff.
6. `set_*` with passkey, want_ack + want_response, single transmission, 20 s wait. NAK → stop (`set_nak`). Then wait `--verify-delay` and re-read; changed paths compared. `applied` = `verified`.

Result fields: `node`, `set_field`, `variant`, `disruptive`, `before`, `after`, `diff[{path,before,after}]`, `changed`, `applied`, `dry_run`, `refused`, `set_packet_id`, `set_nak`, `verified`, `verify_mismatch`, `verify_error`, `identity_preserved` (security only: public and private key unchanged). Secret values appear as `<redacted>`.

Many config writes make the node reboot; increase `--verify-delay` (e.g. 30) accordingly.

`set_owner` (2.7.26) copies only non-empty `long_name`/`short_name`, `is_licensed`, `is_unmessagable`; names cannot be cleared.

### Firmware reply semantics for admin writes (2.7.26, verified)

| Sent with | Accepted | Rejected (e.g. bad passkey) |
|---|---|---|
| want_ack | ACK | ACK (admin error discarded) |
| want_ack + want_response | silence | NAK with reason (`ADMIN_BAD_SESSION_KEY`, `NOT_AUTHORIZED`, ...) |

ACK means received and decrypted, not applied. Only a read-back proves a write.

`admin raw` non-get messages use want_response (result `ok` = no NAK; delivery unconfirmed). `--ack-only` requests an ACK instead (`ok` = ACK; admin errors not reported).

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

## Requests

`request <TO> telemetry[:VARIANT] | position | traceroute | neighbors | nodeinfo`

Telemetry variants: `device` (default), `local_stats`, `environment`, `power`, `air_quality`, `health`, `host`.

| Kind | Port | Encryption | Notes |
|---|---|---|---|
| telemetry | TELEMETRY_APP | PKC if key known, want_ack | variant selected by which Telemetry field the request carries |
| position | POSITION_APP | channel | firmware replies at most once per 3 min per node |
| traceroute | TRACEROUTE_APP | channel | relays append themselves and SNR; firmware never uses PKC |
| neighbors | NEIGHBORINFO_APP | channel | only if NeighborInfo module enabled; 1 reply per 3 min |
| nodeinfo | NODEINFO_APP | channel | same as `nodeinfo` command |

Result: `to`, `kind`, `packet_id`, `encryption`, `ok`, `response_port`, `nak`, `key_exchange`, `incidental_responses`, `unanswered_packet_ids`, `transmissions`, `relays_heard`, `response` (payload), `response_rf` (`rssi`, `snr`, `hops_taken`, `hop_start`, `relay_node`, `rx_time`, `frame_id`), `note`.
Traceroute adds `forward` and `back`: `[{from, to, snr_db}]` per hop; `snr_db` measured by the receiving node of that hop (last back hop measured by us). Unknown SNR = null.

- `ok` requires `response_port` = requested port.
- Firmware bug (2.7.26 and current master): with NeighborInfo enabled, `NeighborInfoModule::wantPacket()` returns true for every packet and the module is registered before the telemetry modules (`Modules.cpp`). `MeshModule::callModules` lets the first wanting module answer a want_response request, so TELEMETRY_APP requests are answered with NEIGHBORINFO_APP. NeighborInfo replies at most once per 3 min; within that window the telemetry module answers. The CLI therefore repeats the request once when it gets NEIGHBORINFO_APP to a non-neighbor request; the first reply is kept in `incidental_responses`. Admin, position, nodeinfo and traceroute modules are registered earlier and are not affected.
- The request bytes are fixed by unit tests (`proto::tests`), e.g. device telemetry: Data `08 43 12 02 12 00 18 01`.
- No response and no NAK: the CLI repeats the request once (`unanswered_packet_ids`). If a PKC request is still unanswered, it sends our NodeInfo and tries once more (`key_exchange.reason = "no response after repeat"`; heuristic, the NAK may have been lost). On NAK PKI_UNKNOWN_PUBKEY the NodeInfo exchange happens directly.
- Observed on !a5139fee (RAK4631, NeighborInfo enabled, 4 hops): the first telemetry request after an idle period got NEIGHBORINFO_APP once and no reply twice; a repeat within ~3 min returned TELEMETRY_APP each time. The missing replies are consistent with NeighborInfo replies being lost or suppressed; not confirmed.
- No response and no NAK: request lost, module disabled, or reply throttled.

## Automatic replies (daemon)

| Received (addressed to us) | Daemon sends |
|---|---|
| want_ack, decoded, no request_id/reply_id | ROUTING_APP ACK, hop limit per firmware getHopLimitForResponse |
| want_ack, decoded reply, received direct or with next_hop set | ROUTING_APP ACK, hop limit 0 |
| want_ack, PKC from unknown key | NAK PKI_UNKNOWN_PUBKEY |
| want_ack, no channel / decode error | NAK NO_CHANNEL |
| NODEINFO_APP with want_response | our User (replaces ACK) |
| TRACEROUTE_APP with want_response | RouteDiscovery with our SNR appended to snr_towards (replaces ACK) |

All automatic replies use channel encryption, are rate-limited to one per (from, id) per 5 s, and are logged in `tx_frames`. Without ACKs, firmware retransmits want_ack replies (observed up to 4 copies).

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

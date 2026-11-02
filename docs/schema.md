# Database schema

SQLite, WAL mode. `meta.schema_version` = 3. Full DDL: `tadpole-mt schema`. Times are Unix seconds (REAL, host clock). Node numbers are unsigned 32-bit stored as INTEGER.

## sessions

One row per daemon run. Use to distinguish "node silent" from "not listening".

| Column | Meaning |
|---|---|
| started, ended | daemon start / clean stop (ended NULL = crashed or running) |
| last_alive | heartbeat, updated every 60 s |
| usb_serial, freq_hz, bw_hz, sf, cr, config_json | radio identity and parameters for the session |

## rx_frames

Every reception, including errors and duplicates. Source of truth.

| Column | Meaning |
|---|---|
| session_id, rx_time | |
| raw | full over-the-air frame (header + encrypted payload) |
| len, airtime_ms | frame length; computed time on air |
| rssi, snr, signal_rssi | SX1262 packet status (dBm, dB, dBm) |
| freq_error_hz | estimated carrier offset of the transmitter |
| status | `ok`, `crc_error`, `header_error`, `short`, `no_channel`, `pkc`, `decode_error` |
| pkt_to, pkt_from, pkt_id | header fields |
| hop_limit, hop_start, hops_taken | hops remaining / initial / traversed (NULL if hop_start unset) |
| want_ack, via_mqtt, channel_hash | header flags |
| next_hop, relay_node | last byte of node number (0 = none) |
| channel, portnum, port | matched channel name; `meshtastic.PortNum` |
| data_json | decoded `meshtastic.Data` (proto3 JSON, payload expanded) |
| error | decode error detail |
| dup_of | id of first reception of same (pkt_from, pkt_id) within 1 h; NULL = first |

Status semantics:
- `crc_error`: header fields may be garbage; node table not updated.
- `header_error`: LoRa header failed; only rx_time and instantaneous rssi stored.
- `no_channel`: channel hash matches no configured channel.
- `pkc`: hash 0, unicast, not to/from us (or peer key unknown) — public-key encrypted; not decryptable. PKC frames to/from us decode with `channel` = `PKC`.
- `decode_error`: hash matched but decrypted bytes are not a valid `Data` (hash collision or wrong PSK).

## nodes

Summary per sender, updated from every trusted frame (not `crc_error`).

| Column group | Source |
|---|---|
| first_heard, last_heard, packets_heard, last_frame_id | any frame (duplicates included) |
| last_rssi, last_snr, last_hops_taken, last_relay_node, last_via_mqtt | last frame (may be a relayed copy) |
| last_direct_heard, last_direct_rssi, last_direct_snr | last frame with hops_taken = 0 and not via MQTT |
| long_name, short_name, hw_model, role, public_key, is_licensed, user_json, user_time | NODEINFO_APP |
| latitude, longitude, altitude, position_json, position_time | POSITION_APP |
| battery_level, voltage, channel_utilization, air_util_tx, uptime_seconds, device_metrics_json, device_metrics_time | TELEMETRY_APP device_metrics |

`*_time` columns are local receive times. Reported device time is inside the JSON (`time`).

## radio_stats

One row per 60 s of daemon operation.

| Column | Meaning |
|---|---|
| noise_min/avg/max, noise_samples | instantaneous RSSI sampled ~1/s while idle (dBm). Includes in-progress traffic; `noise_min` is the best floor estimate |
| frames, crc_errors, header_errors | counts in interval |
| rx_airtime_ms | sum of airtime of received frames (local channel occupancy) |

## tx_frames

Every transmission by this node (including retransmissions).

| Column | Meaning |
|---|---|
| tx_time, raw, len, airtime_ms | |
| pkt_to, pkt_id, hop_limit, want_ack, channel_hash | header |
| encryption | `pkc` or `channel` |
| portnum, port, data_json | plaintext `Data` as sent |
| attempt | 0 = first transmission |
| error | radio error, NULL on success |

Our packets heard back via relays appear in `rx_frames` with `pkt_from` = our node num.

## radio_events

| Column | Meaning |
|---|---|
| t, session_id | |
| event | `connected` (detail = USB serial), `disconnected`, `open_failed` (detail = error; logged when the error changes), `rx_restored` (watchdog) |

Gaps in reception: combine `sessions` (daemon running) with `radio_events` (stick connected).

## requests

One row per daemon `send` API call: `request_json` (as received), `result_json` (send result incl. acks, responses, relays). Secrets in decoded payloads are redacted.

## Views

| View | Content |
|---|---|
| v_packets | rx_frames with `from_id`/`to_id` as `!xxxxxxxx`, `rx_utc` |
| v_device_metrics | device telemetry history (first receptions only) |
| v_positions | position history, degrees |
| v_text | text messages |

# meshtadpole-meshtastic-cli

Host-side Meshtastic node for the MeshTadpole SX1262 USB stick. Binary: `tadpole-mt`. Windows, Linux.

Target users: AI agents and engineers operating dense Meshtastic networks. The CLI is an interface to the mesh and to the record of everything heard; analysis is done by the caller.

## Architecture

- Radio: `meshtadpole-radio` (CH341 + SX1262). No firmware on the stick; all protocol logic runs here.
- Daemon (`tadpole-mt daemon`): owns the stick, receives continuously, decrypts/decodes, writes every frame to SQLite (including CRC/header errors, undecodable and duplicate receptions), serves a local NDJSON API.
- CLI: JSON on stdout, errors as `{"error": ...}` on stderr, non-interactive. Live commands use the daemon API; query commands read SQLite directly (WAL, safe while the daemon runs).
- Protocol: Meshtastic firmware 2.6+/2.7+. Channel AES-CTR. Protobuf payloads decoded at runtime from vendored `.proto` descriptors (`proto/UPSTREAM`).

## Build

```
cargo build --release      # target/release/tadpole-mt
```

Requires `../meshtadpole-radio`. Windows: WinUSB bound to `1a86:5512` (Zadig). Linux: udev rule for `1a86:5512`.

## Files

| Item | Default | Override |
|---|---|---|
| Config | `<local data dir>/meshtadpole/meshtastic.json` | `--config`, `TADPOLE_MT_CONFIG` |
| Database | `<local data dir>/meshtadpole/meshtastic.db` | `--db`, `TADPOLE_MT_DB` |
| API | `127.0.0.1:47403` | config `api` |

Local data dir: Windows `%LOCALAPPDATA%`, Linux `$XDG_DATA_HOME` or `~/.local/share`.

No config file = built-in defaults (EdgeFastLow). `tadpole-mt config --write` writes the defaults for editing.

## Default radio (EdgeFastLow, meshpirkanmaa.org)

| Parameter | Value |
|---|---|
| Region | EU_868 (869.4–869.65 MHz) |
| Bandwidth | 62.5 kHz |
| SF / CR | 8 / 4/8 |
| Frequency slot | 1 → 869.43125 MHz |
| Sync word / preamble | 0x2B / 16 |
| Channel | `EdgeFastLow`, PSK `AQ==` (hash 0x55) |

## Documentation

- [docs/cli.md](docs/cli.md) — commands, output, exit codes, API.
- [docs/schema.md](docs/schema.md) — database tables, views, semantics.

## Status

| Feature | State |
|---|---|
| RX, decrypt (channel AES-CTR), decode, store | working |
| Duplicate/rebroadcast tracking, node summary, radio noise stats | working |
| PKC decrypt (DMs to us), AEAD channels | not implemented |
| TX, requests (telemetry/position/traceroute/nodeinfo), routing participation | not implemented |
| Remote admin (PKC admin key) | not implemented |
| Linux | untested |

## License

GPL-3.0-only. Derived from Meshtastic firmware and protobufs (GPL-3.0).

# meshtadpole-meshtastic-cli

Host-side Meshtastic node for the MeshTadpole SX1262 USB stick. Binary: `tadpole-mt`. Windows, Linux.

Target users: AI agents and engineers operating dense Meshtastic networks. The CLI is an interface to the mesh; analysis is done by the caller.

## Architecture

- Radio: `meshtadpole-radio` (CH341 + SX1262). No firmware on the stick.
- Daemon: owns the stick and node identity, participates in the mesh (default non-rebroadcasting role), stores every received frame (raw, RF metadata, decrypted/decoded payload, decode failures) in SQLite.
- CLI: JSON/NDJSON on stdout, stable exit codes, non-interactive. Talks to the daemon over a local socket; read queries may access SQLite directly.
- Protocol: Meshtastic firmware 2.6+/2.7+ compatible. Channel AES-CTR, PKC (x25519 + AES-CCM), managed flooding, protobufs, admin via PKC admin keys.

## Target network defaults (EdgeFastLow)

| Parameter | Value |
|---|---|
| Region | EU_868 |
| Modem preset | disabled (custom) |
| Bandwidth | 62 kHz |
| Spreading factor | 8 |
| Coding rate | 4/8 (RX accepts any) |
| Frequency slot | 1 → 869.4313 MHz |
| Channel | `EdgeFastLow`, PSK `AQ==` |
| Hop limit | 3 |

Source: https://meshpirkanmaa.org

## Status

Skeleton.

## License

GPL-3.0-only. Derived from Meshtastic firmware and protobufs (GPL-3.0).

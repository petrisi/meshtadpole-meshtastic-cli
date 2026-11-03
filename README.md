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

Requires `../meshtadpole-radio`. Windows: WinUSB bound to `1a86:5512` (Zadig). Linux: build deps `build-essential pkg-config libudev-dev`; root works as is (kernel `spi_ch341` auto-detached); non-root needs a udev rule, see meshtadpole-radio README.

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

## Install

Requirements: Rust >= 1.85 (rustup), git.

Dependency `meshtadpole-radio` comes from `https://github.com/petrisi/meshtadpole-radio` (currently private). For a clean clone, cargo needs git credentials: `gh auth login`, `gh auth setup-git`, and `[net] git-fetch-with-cli = true` in `~/.cargo/config.toml` (or env `CARGO_NET_GIT_FETCH_WITH_CLI=true`).

Development with both repos checked out side by side: put this in `.cargo/config.toml` of the common parent directory (outside the repos) to build against the local checkout:

```
[patch."https://github.com/petrisi/meshtadpole-radio"]
meshtadpole-radio = { path = "meshtadpole-radio" }
```

Linux (tested Ubuntu 26.04, kernel 7.0):

```
sudo apt install build-essential pkg-config libudev-dev
cd meshtadpole-meshtastic-cli && cargo build --release
sudo install -m 755 target/release/tadpole-mt /usr/local/bin/
```

USB access: root works as is (the kernel `spi_ch341` driver is detached automatically). Non-root: udev rule in the `meshtadpole-radio` README.

Windows (tested Windows 10):

```
cd meshtadpole-meshtastic-cli
cargo build --release
mkdir %LOCALAPPDATA%\Programs\meshtadpole
copy target\release\tadpole-mt.exe %LOCALAPPDATA%\Programs\meshtadpole\
```

Add that directory to PATH. The stick needs the WinUSB driver (Zadig, device `1a86:5512`); the vendor CH341PAR driver is not used.

Then: `tadpole-mt config --write`, edit or `--set` the config, start `tadpole-mt daemon` (see "Running as a service").

## Running as a service

The daemon reconnects to the stick on its own (backoff 1-30 s) and starts without it. It exits on database errors and when the radio loop is blocked for 120 s (`radio_events` `stalled`, exit code 70); run it under a service manager that restarts it.

Linux (systemd), `/etc/systemd/system/tadpole-mt.service`:

```
[Unit]
Description=Meshtastic node (tadpole-mt)
After=network.target

[Service]
ExecStart=/usr/local/bin/tadpole-mt daemon
Restart=always
RestartSec=5
User=root

[Install]
WantedBy=multi-user.target
```

Non-root: add the udev rule from `meshtadpole-radio` README and set `User=`. Config/DB then live in that user's `~/.local/share/meshtadpole/`.

Windows: Task Scheduler task "At startup", action `tadpole-mt.exe daemon`, settings "If the task fails, restart every 1 minute"; or a service wrapper (e.g. NSSM).


## Status

| Feature | State |
|---|---|
| RX, decrypt (channel AES-CTR), decode, store | working |
| Duplicate/rebroadcast tracking, node summary, radio noise stats | working |
| PKC encrypt/decrypt (DMs, admin to/from us) | working |
| TX: text (channel/PKC), NodeInfo exchange, CSMA, duty-cycle guard | working |
| Remote admin read (owner, metadata, channels, configs, modules) | working (2.7.26) |
| Remote admin set: read-modify-write, dry run, guards, verify | working (2.7.26: config, security, module, owner, channel) |
| Requests: telemetry, position, traceroute, neighborinfo | working |
| ACK/NAK of packets to us; NodeInfo and traceroute responders; NO_RESPONSE for others | working |
| Bulk admin targets (lists, @groups), ignore/unignore | working |
| Daemon: start without stick, reconnect, RX watchdog | working (unplug/replug tested on Windows) |
| Stuck-receiver recovery (light AGC reset), radio loop watchdog (exit 70), `status.loop_age_s` | working (shared meshtadpole-radio) |
| Fire-and-forget admin writes (`--no-wait`), passkey cache | working |
| AEAD channels, XEdDSA signing, rebroadcasting | not implemented |
| Linux (Ubuntu 26.04.1, kernel 7.0) | working: build, tests, RX, TX, PKC, admin, reconnect |

## Node identity

Top-level `redact_fields`: payload field names masked in DB and output (default `private_key`, `wifi_psk`). See docs/cli.md.

`node` section of the config: `num`, `long_name`, `short_name`, `role` (advertised only), `hw_model`, `private_key` (base64 X25519, stored only in the local config file, redacted in DB and CLI output), `hop_limit`, `duty_cycle_limit`, `duty_cycle_enforce`.

## License

GPL-3.0-only. Derived from Meshtastic firmware and protobufs (GPL-3.0).

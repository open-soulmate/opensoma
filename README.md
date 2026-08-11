# OpenSoma — 分身引擎

**Deploy Everywhere, Collect Everything.**

OpenSoma is a headless daemon that acts as your digital twin's sensory layer. It collects data from diverse sources, normalizes it, and streams it to the Soul agent via gRPC — running silently on any machine, any OS, any network.

## Architecture

```
┌─────────────────────────────────────────────────┐
│                   OpenSoma                       │
│                                                  │
│  ┌──────────┐  ┌───────────┐  ┌──────────────┐  │
│  │Collectors│→ │Processors │→ │  Sync Engine │  │
│  │ (file,   │  │(normalize,│  │(sled cache,  │  │
│  │  stdin…) │  │  dedup)   │  │ upload, retry)│ │
│  └──────────┘  └───────────┘  └──────┬───────┘  │
│                                       │          │
│  ┌──────────┐  ┌───────────┐  ┌──────▼───────┐  │
│  │Connectors│  │ Heartbeat │  │  gRPC Client │  │
│  │(feishu,  │  │ (30s ping)│  │ (→ Soul API) │  │
│  │ dingtalk,│  └───────────┘  └──────────────┘  │
│  │ wecom)   │                                    │
│  └──────────┘                                    │
└─────────────────────────────────────────────────┘
```

## Quick Start

```bash
# Copy and edit config
cp config.example.toml config.toml
vim config.toml

# Run
cargo run --release

# Or with Docker
docker build -t opensoma .
docker run -v /path/to/config.toml:/etc/opensoma/config.toml opensoma
```

## Configuration

See [`config.example.toml`](config.example.toml) for all options. Key sections:

| Section       | Description                          |
|---------------|--------------------------------------|
| `[daemon]`    | Node ID, log level, data directory   |
| `[soul]`      | Soul gRPC endpoint + heartbeat interval |
| `[collector]` | Watched directories, file patterns   |
| `[connector]` | Feishu / DingTalk / WeCom credentials |
| `[sync]`      | Upload batch size, retry policy      |

## Modules

| Module       | Responsibility                              |
|--------------|---------------------------------------------|
| `collector`  | File system watcher (notify) — detects new/changed files |
| `connector`  | IM platform integrations — Feishu, DingTalk, WeCom APIs  |
| `processor`  | Normalize formats + deduplicate entries      |
| `sync`       | Incremental upload with local sled cache + offline retry  |
| `grpc`       | Tonic client for Soul Agent API              |
| `heartbeat`  | Periodic liveness signal to Soul             |
| `config`     | TOML config with hot-reload via notify       |

## Build

```bash
# Debug
cargo build

# Release (optimized)
cargo build --release

# Docker (multi-stage, <10MB runtime image)
docker build -t opensoma .
```

## License

MIT

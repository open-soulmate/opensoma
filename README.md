# OpenSoma — 分身引擎

**Deploy Everywhere, Collect Everything.**

OpenSoma is a headless daemon that acts as your digital twin's sensory layer. It collects data from diverse sources — files, IM platforms, RSS feeds, email, and more — normalizes it, and streams it to the OpenSoul agent via HTTP API. No UI, no bloat — just a silent process running on any machine, any OS, any network.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                     OpenSoma Node                    │
│                                                      │
│  ┌────────────┐   ┌───────────┐   ┌──────────────┐  │
│  │ Collectors │──▶│ Processors│──▶│  Sync Engine │  │
│  │ (file,     │   │(normalize,│   │(sled cache,  │  │
│  │  obsidian) │   │  dedup)   │   │ upload, retry)│  │
│  └────────────┘   └───────────┘   └──────┬───────┘  │
│                                          │           │
│  ┌────────────┐   ┌───────────┐   ┌──────▼───────┐  │
│  │ Connectors │   │ Heartbeat │   │  gRPC Client │  │
│  │(feishu,    │   │ (30s ping)│   │ (─▶ Soul API)│  │
│  │ dingtalk,  │   └───────────┘   └──────────────┘  │
│  │ wecom, rss,│                                      │
│  │ email,     │   ┌───────────┐                      │
│  │ notion,    │   │  Sense    │  (multimodal plugins)│
│  │ git,webhook│   │(OCR, ASR, │                      │
│  └────────────┘   │ image,vid)│                      │
│                   └───────────┘                      │
└─────────────────────────────────────────────────────┘
                          │
                     HTTP (REST)
                          │
                  ┌───────▼───────┐
                  │   Soul Agent  │
                  └───────────────┘
```

**Key design principles:**
- **Headless** — no UI, no web server overhead. Runs as a systemd service, Docker container, or bare process.
- **Distributed** — deploy multiple nodes on different machines; each node has a unique `node_id` and connects independently to the Soul agent.
- **Offline-first** — local sled cache buffers events when the network is down; automatic retry on reconnection.
- **Hot-reload** — edit `config.toml` while running; changes apply without restart.

## Connectors

| Connector    | Source Type              | Mode         | Description                                      |
|-------------|--------------------------|--------------|--------------------------------------------------|
| **File**     | Local filesystem         | Watch        | Monitors directories for new/changed files       |
| **Feishu**   | Feishu (Lark) API        | Webhook + Poll | Receives messages, docs, and approval events   |
| **DingTalk** | DingTalk Open API        | Poll         | Fetches messages, approvals, and robot events    |
| **WeCom**    | WeCom (Enterprise WeChat)| Poll         | Collects messages and application events         |
| **RSS**      | Any RSS/Atom feed        | Poll         | Periodically fetches and parses feed entries     |
| **Email**    | IMAP mailbox             | Poll         | Reads emails from one or more IMAP accounts      |
| **Notion**   | Notion API               | Poll         | Syncs database pages from a Notion workspace     |
| **Git**      | Git repository           | Poll         | Watches a repo for new commits and diffs         |
| **Obsidian** | Obsidian vault           | Watch        | Monitors vault files for changes (notes, etc.)   |
| **Webhook**  | HTTP POST                | Listen       | Generic webhook receiver with HMAC verification  |
| **GitHub**   | GitHub REST API          | Poll         | Syncs issues, PRs, and releases from repositories |
| **Slack**    | Slack API                | Poll         | Collects messages and thread replies from channels |

## Quick Start

### Prerequisites

- Rust 1.75+ (or Docker)
- A running OpenSoul agent with HTTP endpoint

### Run with Cargo

```bash
# Clone the repo
git clone https://github.com/your-org/opensoma.git
cd opensoma

# Copy and edit config
cp config.example.toml config.toml
# Edit config.toml — at minimum, set [soul] endpoint

# Build and run
cargo run --release
```

### Run with Docker

```bash
# Build the image (~12MB)
docker build -t opensoma .

# Run with config mounted
docker run -d \
  --name opensoma \
  -v /path/to/config.toml:/etc/opensoma/config.toml \
  opensoma
```

### CLI Options

```
opensoma [OPTIONS]

Options:
  -c, --config <PATH>  Path to config file [default: config.toml]
  -h, --help           Print help
  -V, --version        Print version
```

## Configuration

Copy `config.example.toml` to `config.toml` and edit. All options are documented with inline comments.

### Sections Overview

| Section         | Description                                        |
|----------------|----------------------------------------------------|
| `[daemon]`     | Node identity, log level, data directory            |
| `[soul]`       | OpenSoul HTTP endpoint, heartbeat and timeout settings  |
| `[collector]`  | File system watch directories and patterns          |
| `[connector.*]`| Platform-specific credentials and polling intervals |
| `[processor]`  | Normalization, deduplication, and event size limits  |
| `[sync]`       | Upload batch size, retry policy, cache limits       |

### Environment Variable Overrides

Any config value can be overridden via environment variables using the pattern:

```
OPENSOMA_<SECTION>_<FIELD>
```

Examples:

```bash
OPENSOMA_DAEMON_NODE_ID=soma-node-002
OPENSOMA_SOUL_ENDPOINT=http://localhost:8090
OPENSOMA_CONNECTOR_FEISHU_APP_ID=cli_xxxxx
OPENSOMA_CONNECTOR_DINGTALK_APP_SECRET=secret123
```

## Sense Plugins

Sense plugins are multimodal parsers that extract structured data from media files. They run inline during the processing pipeline.

| Plugin   | Input Types                | Description                              |
|----------|----------------------------|------------------------------------------|
| **OCR**  | Images (PNG, JPG), PDFs    | Extracts text from images and documents  |
| **ASR**  | Audio (WAV, MP3, OGG)      | Transcribes speech to text               |
| **Image**| Images (PNG, JPG, WEBP)    | Extracts metadata, descriptions, EXIF    |
| **Video**| Video (MP4, MOV, WEBM)     | Extracts keyframes, subtitles, metadata  |

Sense plugins implement the `SensePlugin` trait:

```rust
#[async_trait]
pub trait SensePlugin: Send + Sync {
    async fn parse(&self, data: &[u8]) -> anyhow::Result<SenseResult>;
    fn name(&self) -> &str;
}
```

To add a new plugin, create a module under `src/plugins/sense/` and register it in `mod.rs`.

## Modules

| Module       | Responsibility                                          |
|-------------|--------------------------------------------------------|
| `collector`  | File system watcher + process/network/clipboard monitors |
| `connector`  | IM platform integrations — Feishu, DingTalk, WeCom, RSS, Email, Notion, Git, Obsidian, Webhook, GitHub |
| `processor`  | Normalize → Classify → Enrich → Dedup pipeline         |
| `sync`       | Incremental upload with local sled cache + offline retry |
| `grpc`       | Tonic client for Soul Agent API                         |
| `heartbeat`  | Periodic liveness signal to Soul                        |
| `config`     | TOML config with hot-reload via notify                  |
| `plugins`    | Sense plugins for multimodal content parsing            |

## Build

```bash
# Debug build
cargo build

# Release build (optimized, stripped)
cargo build --release

# Run tests
cargo test

# Docker (multi-stage, ~12MB runtime image)
docker build -t opensoma .
```

### Release Profile

The release profile is optimized for minimal binary size:

```toml
[profile.release]
opt-level = "z"      # Optimize for size
lto = true           # Link-time optimization
codegen-units = 1    # Single codegen unit for better optimization
strip = true         # Strip debug symbols
panic = "abort"      # Abort on panic (no unwinding)
```

## HTTP API Integration

OpenSoma communicates with OpenSoul via HTTP REST API:

| RPC             | Direction       | Description                        |
|----------------|----------------|------------------------------------|
| `Heartbeat`     | Bidirectional  | Node liveness ping/pong            |
| `UploadEvents`  | Soma ─▶ Soul   | Batch upload of collected events   |
| `StreamEvents`  | Soma ─▶ Soul   | Real-time single-event streaming (SSE) |

## Project Structure

```
opensoma/
├── Cargo.toml
├── build.rs              # Protobuf code generation
├── config.example.toml   # Annotated config template
├── Dockerfile            # Multi-stage build
├── proto/
│   └── soul.proto        # gRPC service definition
└── src/
    ├── main.rs           # Entry point, subsystem wiring
    ├── config.rs         # TOML config + hot-reload
    ├── heartbeat.rs      # Liveness signal
    ├── status_server.rs  # HTTP monitoring endpoint
    ├── collector/
    │   ├── mod.rs
    │   ├── file.rs       # File system watcher
    │   ├── process.rs    # Process monitor (sysinfo)
    │   ├── network.rs    # Network connection monitor
    │   └── clipboard.rs  # Clipboard change monitor
    ├── connector/
    │   ├── mod.rs        # Connector trait + retry macro
    │   ├── feishu.rs
    │   ├── dingtalk.rs
    │   ├── wecom.rs
    │   ├── rss.rs
    │   ├── email.rs      # IMAP fetching
    │   ├── notion.rs
    │   ├── git.rs
    │   ├── obsidian.rs
    │   ├── webhook.rs    # HMAC-verified HTTP receiver
    │   └── github.rs     # Issues, PRs, releases
    ├── grpc/             # HTTP client for Soul API
    ├── plugins/
    │   └── sense/
    │       ├── mod.rs    # SensePlugin trait
    │       ├── ocr.rs
    │       ├── asr.rs
    │       ├── image.rs
    │       └── video.rs
    ├── processor/
    │   ├── mod.rs        # Pipeline orchestrator
    │   ├── normalize.rs  # Timestamp normalization
    │   ├── classify.rs   # Content type + urgency classification
    │   ├── enrich.rs     # Entity extraction + keywords + summary
    │   └── dedup.rs      # Content-hash deduplication
    └── sync/
        ├── mod.rs        # Sync engine with batch upload
        ├── cache.rs      # Sled local cache
        ├── upload.rs     # HTTP upload to Soul
        └── conflict.rs   # Conflict detection + resolution
```

## License

MIT

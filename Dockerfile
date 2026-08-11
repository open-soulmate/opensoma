# ---- Builder Stage ----
FROM rust:1.83-bookworm AS builder

WORKDIR /build
COPY Cargo.toml Cargo.lock* ./
COPY build.rs ./
COPY proto/ ./proto/
COPY src/ ./src/

RUN cargo build --release && strip /build/target/release/opensoma

# ---- Runtime Stage ----
FROM debian:bookworm-slim AS runtime

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd -r opensoma && \
    useradd -r -g opensoma -d /var/lib/opensoma opensoma && \
    mkdir -p /var/lib/opensoma /etc/opensoma && \
    chown -R opensoma:opensoma /var/lib/opensoma

COPY --from=builder /build/target/release/opensoma /usr/local/bin/opensoma

USER opensoma
WORKDIR /var/lib/opensoma

ENTRYPOINT ["opensoma"]
CMD ["--config", "/etc/opensoma/config.toml"]

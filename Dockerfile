# ---- Builder Stage ----
FROM rust:1.83-bookworm AS builder

WORKDIR /build

# Cache dependency builds by copying manifests first
COPY Cargo.toml Cargo.lock* ./
COPY build.rs ./
COPY proto/ ./proto/

# Create a dummy main.rs to build dependencies only
RUN mkdir src && echo "fn main() {}" > src/main.rs && \
    cargo build --release && \
    rm -rf src

# Now copy the real source and rebuild (only the application code changes)
COPY src/ ./src/
RUN touch src/main.rs && \
    cargo build --release && \
    strip /build/target/release/opensoma

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

EXPOSE 9800

ENTRYPOINT ["opensoma"]
CMD ["--config", "/etc/opensoma/config.toml"]

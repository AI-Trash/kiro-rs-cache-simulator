# syntax=docker/dockerfile:1

FROM rust:1.90-bookworm AS builder
WORKDIR /app
RUN apt-get update \
    && apt-get install -y --no-install-recommends mold sccache \
    && rm -rf /var/lib/apt/lists/*
ENV RUSTC_WRAPPER=sccache \
    SCCACHE_DIR=/sccache \
    RUSTFLAGS="-C link-arg=-fuse-ld=mold"
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    --mount=type=cache,target=/sccache \
    cargo build --release --locked \
    && cp target/release/kiro-rs-cache-simulator /tmp/kiro-rs-cache-simulator

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /tmp/kiro-rs-cache-simulator /usr/local/bin/kiro-rs-cache-simulator
EXPOSE 8990
ENTRYPOINT ["/usr/local/bin/kiro-rs-cache-simulator"]

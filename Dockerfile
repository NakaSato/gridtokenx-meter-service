# syntax=docker/dockerfile:1.7
# gridtokenx-meter-service — multi-stage Rust build (modular monolith workspace).
FROM rust:1.91-bookworm AS builder
# rdkafka (+ its zstd-sys) needs a C/C++ toolchain + cmake to build librdkafka.
# apt archives live in BuildKit caches shared with the sibling service images, so
# the .debs are downloaded once rather than per-image whenever this layer
# invalidates. docker-clean must go, or apt deletes what we are caching; the
# lists/ rm is likewise dropped since the mount keeps them out of the layer.
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked <<EOT
    set -eux
    rm -f /etc/apt/apt.conf.d/docker-clean
    echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache
    apt-get update
    apt-get install -y --no-install-recommends build-essential cmake clang pkg-config
EOT
WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
COPY crates ./crates
COPY bin ./bin
# Cache mounts: cargo registry + target persist across builds in BuildKit's
# cache (incremental recompile). Copy the binary out before the RUN ends —
# the mount is not visible to a later COPY.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/app/target \
    cargo build --release --bin meter-service && \
    strip target/release/meter-service && \
    cp target/release/meter-service /app/meter-service-bin

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/meter-service-bin /usr/local/bin/meter-service
EXPOSE 8080
ENV METER_SERVICE_PORT=8080
HEALTHCHECK --interval=30s --timeout=3s --retries=3 \
    CMD curl -fsS http://localhost:8080/health || exit 1
CMD ["meter-service"]

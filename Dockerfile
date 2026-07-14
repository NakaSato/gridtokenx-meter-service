# syntax=docker/dockerfile:1.7
# gridtokenx-meter-service — multi-stage Rust build (modular monolith workspace).
FROM rust:1.89-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
COPY crates ./crates
COPY bin ./bin
# Cache mounts: cargo registry + target persist across builds in BuildKit's
# cache (incremental recompile). Copy the binary out before the RUN ends —
# the mount is not visible to a later COPY.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
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

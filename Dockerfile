# gridtokenx-meter-service — multi-stage Rust build (modular monolith workspace).
FROM rust:1-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
COPY crates ./crates
COPY bin ./bin
RUN cargo build --release --bin meter-service

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/meter-service /usr/local/bin/meter-service
EXPOSE 8080
ENV METER_SERVICE_PORT=8080
HEALTHCHECK --interval=30s --timeout=3s --retries=3 \
    CMD curl -fsS http://localhost:8080/health || exit 1
CMD ["meter-service"]

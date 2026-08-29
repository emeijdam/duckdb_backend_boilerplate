# CROSV governance API (Rust/Axum + bundled DuckDB).
# Multi-stage: compile, then a slim runtime. DuckDB is vendored by the crate, so
# the build stage needs a C/C++ toolchain; the runtime needs only libstdc++.

FROM rust:1-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends build-essential cmake \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cargo build --release --bin crosv_api_server

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libstdc++6 \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /src/target/release/crosv_api_server /app/crosv_api_server
COPY config /app/config
# Bind 0.0.0.0 inside the container; data + DuckDB file live under /app/data
# (a mounted volume). CROSV_DATA_DIR is read by init.sql's base_path macro.
ENV APP_SERVER__HOST=0.0.0.0 \
    APP_DATABASE__FILENAME=/app/data/data.db \
    CROSV_DATA_DIR=/app/data
EXPOSE 3000
CMD ["/app/crosv_api_server"]

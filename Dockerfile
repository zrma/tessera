# Tessera development Dockerfile
# - Builds the Rust workspace
# - Runs tessera-worker and tessera-gateway together for local dev

FROM rust:1-bookworm

# System deps commonly needed for Rust dev and future gRPC usage
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
     ca-certificates curl git pkg-config build-essential \
     protobuf-compiler \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy workspace
COPY . .

# Prime build cache (optional during image build)
RUN cargo build --workspace --locked || true

# Default env for in-container networking and logs
ENV TESSERA_GW_ADDR=0.0.0.0:4000 \
    TESSERA_WORKER_ADDR=0.0.0.0:5001 \
    RUST_LOG=info \
    CARGO_TERM_COLOR=always

EXPOSE 4000 5001

# Run both services and keep the container in foreground
CMD ["bash", "-lc", "\
set -euo pipefail; \
cargo build --workspace; \
cargo run -p tessera-worker & \
cargo run -p tessera-gateway & \
wait -n \
"]


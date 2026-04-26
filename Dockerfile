# syntax=docker/dockerfile:1

# Tessera sample runtime image.
# Builds all workspace binaries once, then ships a small runtime layer that can
# run gateway, worker, orchestrator, client, or simulator by overriding CMD.

FROM rust:1.89-bookworm AS builder

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
     ca-certificates pkg-config build-essential protobuf-compiler \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY proto ./proto
COPY crates ./crates
COPY xtask ./xtask

RUN cargo build --workspace --bins --release --locked

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates curl \
  && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/tessera-client /usr/local/bin/tessera-client
COPY --from=builder /app/target/release/tessera-gateway /usr/local/bin/tessera-gateway
COPY --from=builder /app/target/release/tessera-orch /usr/local/bin/tessera-orch
COPY --from=builder /app/target/release/tessera-sim /usr/local/bin/tessera-sim
COPY --from=builder /app/target/release/tessera-worker /usr/local/bin/tessera-worker

ENV RUST_LOG=info

EXPOSE 4000 4100 5001 5100 6000 6100

USER 65532:65532
CMD ["tessera-gateway"]

FROM rust:1.94-slim-bookworm AS builder

RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    pkg-config \
    libssl-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

COPY Cargo.toml Cargo.lock ./
COPY .cargo ./.cargo
COPY vendor ./vendor

COPY crates ./crates

RUN mkdir src && echo "fn main() {}" > src/main.rs

RUN cargo build --release

COPY src ./src
COPY protos ./protos
COPY build.rs ./

RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /usr/src/app/target/release/degenerate /app/degenerate

CMD ["./degenerate"]
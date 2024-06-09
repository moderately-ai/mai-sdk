FROM rust:1.78 as builder
RUN rustup target add aarch64-unknown-linux-musl
RUN apt-get update && apt-get upgrade && apt-get install -y musl-tools pkg-config libssl-dev
WORKDIR /usr/src/mai-sdk
COPY . .
RUN cargo install --path mai-sdk-node --target=aarch64-unknown-linux-musl

FROM debian:bullseye-slim
COPY --from=builder /usr/local/cargo/bin/mai-sdk-node /usr/local/bin/mai-sdk-node
CMD ["mai-sdk-node"]
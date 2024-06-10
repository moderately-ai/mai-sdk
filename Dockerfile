FROM rust:1.78 as builder
RUN rustup target add aarch64-unknown-linux-musl
RUN apt-get update && apt-get upgrade && apt-get install -y musl-tools pkg-config libssl-dev
WORKDIR /usr/src/mai-sdk
COPY . .
RUN cargo install --path mai-sdk-node --target=aarch64-unknown-linux-musl

FROM debian:bullseye-slim

# setup tini for proper process and signal handlikng
ENV TINI_VERSION v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--"]

# add the binary to the image
COPY --from=builder /usr/local/cargo/bin/mai-sdk-node /usr/local/bin/mai-sdk-node

# default environment configurations
ENV RUST_LOG=info
ENV RUST_BACKTRACE=1
ENV LOG_MODE=stdout
ENV GOSSIP_LISTEN_ADDRS="/ip4/0.0.0.0/tcp/3000"

# expose the default port, enable health checks
EXPOSE 3000

# run the binary
CMD ["mai-sdk-node"]
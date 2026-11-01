# Built from the repo root (compose: context ".", dockerfile gateway/Dockerfile): the binary
# embeds src/lua/*.lua and reads deploy/meter.toml at startup.
FROM rust:1.85-slim-bookworm AS build

WORKDIR /build/gateway
# Dependencies first so code edits don't rebuild them.
COPY gateway/Cargo.toml gateway/Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs \
    && cargo build --release --locked \
    && rm -rf src

COPY src/lua /build/src/lua
COPY deploy/meter.toml /build/deploy/meter.toml
COPY gateway/src ./src
COPY gateway/tests ./tests
# Same gate as the api image: unit tests (incl. Python parity vectors) must pass to ship.
RUN cargo test --release --locked \
    && touch src/main.rs && cargo build --release --locked


FROM debian:bookworm-slim

COPY --from=build /build/gateway/target/release/gateway /usr/local/bin/gateway
COPY deploy/meter.toml /etc/gateway/meter.toml
ENV METER_CONFIG=/etc/gateway/meter.toml LISTEN=0.0.0.0:80

EXPOSE 80
CMD ["gateway"]

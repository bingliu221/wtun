# syntax=docker/dockerfile:1

# ==============================================================
# Builder: musl / Alpine
# ==============================================================
FROM docker.io/library/rust:alpine AS builder-alpine

RUN apk add --no-cache musl-dev

COPY . /wtun/
WORKDIR /wtun

RUN cargo build --release --bin wtun


# ==============================================================
# Builder: glibc / Debian
# ==============================================================
FROM docker.io/library/rust:slim-bookworm AS builder-glibc

COPY . /wtun/
WORKDIR /wtun

RUN cargo build --release --bin wtun


# ==============================================================
# Runtime: Alpine (musl) — 默认 target
# ==============================================================
FROM docker.io/library/alpine AS runtime-alpine

RUN apk add --no-cache ca-certificates

COPY --from=builder-alpine /wtun/target/release/wtun /usr/local/bin/wtun

ENTRYPOINT ["wtun"]


# ==============================================================
# Runtime: Debian slim (glibc)
# ==============================================================
FROM docker.io/library/debian:bookworm-slim AS runtime-glibc

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder-glibc /wtun/target/release/wtun /usr/local/bin/wtun

ENTRYPOINT ["wtun"]

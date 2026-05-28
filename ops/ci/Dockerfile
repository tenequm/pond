# syntax=docker/dockerfile:1
#
# Pond cross-compile image: cargo-zigbuild base + pond's system build deps +
# kache as RUSTC_WRAPPER. The toolchain (and aarch64-apple-darwin target) is
# pinned by ../../rust-toolchain.toml; `rustup show` reads it and installs.
FROM ghcr.io/rust-cross/cargo-zigbuild

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
        cmake \
        protobuf-compiler \
        pkg-config \
 && rm -rf /var/lib/apt/lists/*

ENV PROTOC=/usr/bin/protoc

COPY rust-toolchain.toml ./
RUN rustup show

ARG KACHE_VERSION=v0.3.1
RUN curl -fsSL "https://github.com/kunobi-ninja/kache/releases/download/${KACHE_VERSION}/kache-x86_64-unknown-linux-musl.tar.gz" \
    | tar -xz -C /tmp \
 && install -m 0755 "$(find /tmp -name kache -type f | head -1)" /usr/local/bin/kache \
 && rm -rf /tmp/kache*

ENV RUSTC_WRAPPER=kache

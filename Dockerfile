# syntax=docker/dockerfile:1
FROM rust:1.95-bookworm AS builder

WORKDIR /app

RUN set -eux && \
    apt-get -y update && \
    apt-get install -y --no-install-recommends \
        libssl-dev make cmake graphviz clang libclang-dev llvm \
        git pkg-config curl time rhash ca-certificates zstd xz-utils \
        python3 python3-pip unzip gnupg protobuf-compiler && \
    apt-get autoremove -y && apt-get clean && rm -rf /var/lib/apt/lists/*

# Pin zig < 0.16. cargo-zigbuild's zig linker emits a duplicate libobjc.A.dylib
# LC_LOAD_DYLIB (Apple's ld coalesces it; zig doesn't), and macOS 26 dyld aborts
# on duplicate linked dylibs - but only when the binary records LC_BUILD_VERSION
# sdk >= 26. zig bakes that sdk field in by version, ignoring SDKROOT entirely:
# 0.16 records 26.x (dyld aborts), 0.15.2 records 15.5 (dyld tolerates the
# duplicate, binary runs). Root cause is zig over-emitting per-library load
# commands that newer dyld rejects (ziglang/zig#24349); the LC_RPATH variant of
# this hit macOS 26 + zig 0.16 in ziglang/zig#25311 and was fixed there for
# RPATH only - the LC_LOAD_DYLIB/libobjc variant still ships broken in 0.16. Do
# NOT bump to 0.16+ until that's fixed upstream.
RUN pip3 install --no-cache-dir --break-system-packages ziglang==0.15.2
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    cargo install --locked cargo-zigbuild

# SDKROOT supplies the macOS framework/lib stubs the link step needs (candle's
# metal feature pulls Metal/Foundation). It does NOT set the recorded
# LC_BUILD_VERSION sdk - the zig version above owns that. Any recent SDK links;
# 15.5 is pinned for reproducibility.
ARG MACOSX_SDK_VERSION=15.5
ARG MACOSX_SDK_SHA256=c15cf0f3f17d714d1aa5a642da8e118db53d79429eb015771ba816aa7c6c1cbd
RUN curl -fsSL -o /tmp/sdk.tar.xz \
      "https://github.com/joseluisq/macosx-sdks/releases/download/${MACOSX_SDK_VERSION}/MacOSX${MACOSX_SDK_VERSION}.sdk.tar.xz" && \
    echo "${MACOSX_SDK_SHA256}  /tmp/sdk.tar.xz" | sha256sum -c - && \
    mkdir -p /opt/sdks && tar -xJf /tmp/sdk.tar.xz -C /opt/sdks && \
    rm /tmp/sdk.tar.xz
ENV SDKROOT=/opt/sdks/MacOSX${MACOSX_SDK_VERSION}.sdk

ARG KACHE_VERSION=v0.3.1
RUN curl -fsSL "https://github.com/kunobi-ninja/kache/releases/download/${KACHE_VERSION}/kache-x86_64-unknown-linux-musl.tar.gz" \
      | tar -xz -C /tmp && \
    install -m 0755 "$(find /tmp -name kache -type f | head -1)" /usr/local/bin/kache && \
    rm -rf /tmp/kache*

# No KACHE_REMOTE_TYPE env var exists - this two-line file is required to activate s3.
RUN mkdir -p /root/.config/kache && \
    printf '[cache.remote]\ntype = "s3"\n' > /root/.config/kache/config.toml

# `profile = "minimal"` + targets-in-toml does not reliably fetch rust-std for non-host targets.
COPY rust-toolchain.toml /app/rust-toolchain.toml
RUN rustup show && \
    rustup target add aarch64-apple-darwin \
                      x86_64-pc-windows-gnu \
                      aarch64-unknown-linux-gnu \
                      x86_64-unknown-linux-gnu

ENV PROTOC=/usr/bin/protoc \
    CARGO_TERM_COLOR=always \
    RUSTC_WRAPPER=kache \
    KACHE_S3_BUCKET=ttq \
    KACHE_S3_ENDPOINT=https://nbg1.your-objectstorage.com \
    KACHE_S3_REGION=nbg1 \
    KACHE_S3_PREFIX=kache/pond

COPY . .

# Cargo unifies features across all targets in one invocation, leaking
# cfg-gated deps across platforms. macOS and Windows each build alone: macOS pulls
# candle's macOS-only `metal`, and the two linux targets enable xet-data's
# `cfg(not(windows))` sha2 `asm` feature, which drags sha2-asm (a Windows
# compile_error!) into the Windows build. The two linux targets gate nothing
# against each other, so they batch together.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/app/target,sharing=locked \
    --mount=type=cache,target=/root/.cache/kache,sharing=locked \
    --mount=type=secret,id=kache_s3_access_key,env=KACHE_S3_ACCESS_KEY \
    --mount=type=secret,id=kache_s3_secret_key,env=KACHE_S3_SECRET_KEY \
    bash -ec '\
      kache sync --pull && \
      cargo zigbuild --locked --profile dist \
        --target aarch64-unknown-linux-gnu \
        --target x86_64-unknown-linux-gnu && \
      cargo zigbuild --locked --profile dist --target x86_64-pc-windows-gnu && \
      cargo zigbuild --locked --profile dist --target aarch64-apple-darwin && \
      kache sync --push && \
      mkdir -p /app/out && \
      cp target/aarch64-apple-darwin/dist/pond        /app/out/pond-aarch64-apple-darwin && \
      cp target/x86_64-pc-windows-gnu/dist/pond.exe   /app/out/pond-x86_64-pc-windows-gnu.exe && \
      cp target/aarch64-unknown-linux-gnu/dist/pond   /app/out/pond-aarch64-unknown-linux-gnu && \
      cp target/x86_64-unknown-linux-gnu/dist/pond    /app/out/pond-x86_64-unknown-linux-gnu \
    '

FROM scratch AS output
COPY --from=builder /app/out/ /

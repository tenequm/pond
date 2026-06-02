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

# cargo-zigbuild's zig linker emits a duplicate libobjc.A.dylib LC_LOAD_DYLIB
# (Apple's ld coalesces it; zig doesn't), which macOS 26 dyld rejects - but only
# when the binary records LC_BUILD_VERSION sdk >= 26, which zig bakes in by
# version regardless of SDKROOT. We keep zig 0.16 (downgrading to a version that
# records sdk < 26 breaks the aws-lc-sys link - cargo-zigbuild#377) and instead
# rewrite the recorded sdk to < 26 + re-sign after linking (the macos post-link
# step in the build below). Same bug class as ziglang/zig#24349 / #25311 (the
# LC_RPATH variant, fixed upstream; the libobjc/LC_LOAD_DYLIB variant is not).
RUN pip3 install --no-cache-dir --break-system-packages ziglang==0.16.0
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

# rcodesign (apple-codesign): re-sign the darwin binary on Linux after the
# post-link sdk rewrite below; arm64 macOS rejects a binary whose signature the
# byte-patch invalidated, so we must ad-hoc re-sign. vtool/codesign are macOS-only.
ARG RCODESIGN_VERSION=0.29.0
RUN curl -fsSL "https://github.com/indygreg/apple-platform-rs/releases/download/apple-codesign%2F${RCODESIGN_VERSION}/apple-codesign-${RCODESIGN_VERSION}-x86_64-unknown-linux-musl.tar.gz" \
      | tar -xz -C /tmp && \
    install -m 0755 "$(find /tmp -name rcodesign -type f | head -1)" /usr/local/bin/rcodesign && \
    rm -rf /tmp/apple-codesign*

# Rewrite a macOS binary's LC_BUILD_VERSION.sdk (and legacy LC_VERSION_MIN_MACOSX)
# to 15.0 so macOS 26 dyld tolerates zig's duplicate libobjc load command. Pure
# Python (Linux has no vtool); handles thin + fat Mach-O, edits in place.
RUN cat > /usr/local/bin/patch-macos-sdk.py <<'PY'
import sys, struct
TARGET = (15 << 16)  # 15.0.0, encoded X<<16 | Y<<8 | Z
MH_MAGIC_64 = 0xfeedfacf
FAT_MAGIC, FAT_CIGAM = 0xcafebabe, 0xbebafeca
LC_BUILD_VERSION, LC_VERSION_MIN_MACOSX = 0x32, 0x24

def patch_thin(buf, base):
    end = '<' if struct.unpack_from('<I', buf, base)[0] == MH_MAGIC_64 else '>'
    ncmds = struct.unpack_from(end + 'I', buf, base + 16)[0]
    off = base + 32  # sizeof(mach_header_64)
    for _ in range(ncmds):
        cmd, cmdsize = struct.unpack_from(end + 'II', buf, off)
        if cmd == LC_BUILD_VERSION:
            struct.pack_into(end + 'I', buf, off + 16, TARGET)  # after cmd,size,platform,minos
        elif cmd == LC_VERSION_MIN_MACOSX:
            struct.pack_into(end + 'I', buf, off + 12, TARGET)  # after cmd,size,version
        off += cmdsize

data = bytearray(open(sys.argv[1], 'rb').read())
if struct.unpack_from('>I', data, 0)[0] in (FAT_MAGIC, FAT_CIGAM):
    for i in range(struct.unpack_from('>I', data, 4)[0]):
        patch_thin(data, struct.unpack_from('>I', data, 8 + i * 20 + 8)[0])
else:
    patch_thin(data, 0)
open(sys.argv[1], 'wb').write(data)
PY

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
      cargo zigbuild --locked --profile dist --target aarch64-apple-darwin && \
      cargo zigbuild --locked --profile dist \
        --target aarch64-unknown-linux-gnu \
        --target x86_64-unknown-linux-gnu && \
      cargo zigbuild --locked --profile dist --target x86_64-pc-windows-gnu && \
      kache sync --push \
    '

# Package step, separate from the compile above so a darwin patch/sign failure
# re-runs in seconds instead of recompiling. Reads the binaries back from the
# same target/ cache mount. rcodesign needs --config-file /dev/null: with no
# explicit config it auto-discovers a profile and aborts ("UnknownField version").
RUN --mount=type=cache,target=/app/target,sharing=locked \
    bash -ec '\
      mkdir -p /app/out && \
      cp target/aarch64-apple-darwin/dist/pond        /app/out/pond-aarch64-apple-darwin && \
      python3 /usr/local/bin/patch-macos-sdk.py /app/out/pond-aarch64-apple-darwin && \
      rcodesign --config-file /dev/null sign /app/out/pond-aarch64-apple-darwin && \
      chmod +x /app/out/pond-aarch64-apple-darwin && \
      cp target/x86_64-pc-windows-gnu/dist/pond.exe   /app/out/pond-x86_64-pc-windows-gnu.exe && \
      cp target/aarch64-unknown-linux-gnu/dist/pond   /app/out/pond-aarch64-unknown-linux-gnu && \
      cp target/x86_64-unknown-linux-gnu/dist/pond    /app/out/pond-x86_64-unknown-linux-gnu \
    '

FROM scratch AS output
COPY --from=builder /app/out/ /

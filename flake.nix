{
  description = "pond - lossless session storage and search for AI agent clients";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

    # The devShell resolves against THIS input, not `nixpkgs`, so a routine
    # `nixpkgs` bump (which only feeds `packages.pond`) moves no compiler store
    # path: cargo does not rerun build scripts and moon/kache keys do not miss.
    # Bump it deliberately and expect one cold build when you do.
    nixpkgs-toolchain.url = "github:NixOS/nixpkgs/nixos-unstable";

    # Vendored rustup manifests, so `fromRustupToolchainFile` stays a pure eval -
    # no import-from-derivation, no network. fenix was rejected: its
    # `fromToolchainFile` is impure or IFD, and it drops unknown components.
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs-toolchain";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      nixpkgs-toolchain,
      rust-overlay,
    }:
    let
      # Prebuilt binaries exist only for these three; there is no
      # x86_64-darwin build, so it is deliberately absent.
      systems = [
        "aarch64-darwin"
        "x86_64-linux"
        "aarch64-linux"
      ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});

      # Every tool version in one place, in plain `name = "x.y.z";` form on
      # purpose: a reader without Nix can extract it by text, which is how the
      # Windows leg will read these once it stops carrying its own copies (plan
      # phase 5). The flake-check job keeps that text form honest today - it
      # fails when the literals and `nix eval --json .#lib.toolVersions`
      # disagree. Rust is not repeated here - it is read from
      # rust-toolchain.toml, which stays the single Rust pin.
      #
      # The four that come from nixpkgs-toolchain (zig, cargoZigbuild,
      # rcodesign, gh) are asserted against the package's own `version` below,
      # so a nixpkgs bump that moves one fails evaluation instead of making
      # this table lie.
      toolVersions = {
        rust = (builtins.fromTOML (builtins.readFile ./rust-toolchain.toml)).toolchain.channel;
        zig = "0.16.0";
        cargoZigbuild = "0.23.4";
        rcodesign = "0.29.0";
        gh = "2.100.0";
        macosSdk = "15.5";
        moon = "2.5.4";
        protoc = "36.1";
        uv = "0.12.13";
        node = "24.21.0";
        npm = "11.19.1";
        kache = "0.21.0";
      };

      v = toolVersions;

      # Release archives, per platform. Hashes from `nix store prefetch-file <url>`.
      sources = {
        moon = {
          x86_64-linux = {
            url = "https://github.com/moonrepo/moon/releases/download/v${v.moon}/moon_cli-x86_64-unknown-linux-gnu.tar.xz";
            hash = "sha256-uqbwzaj+nXUT/+uykIz9lSiIxmZZt5XaAqFO8hVutcc=";
          };
          aarch64-linux = {
            url = "https://github.com/moonrepo/moon/releases/download/v${v.moon}/moon_cli-aarch64-unknown-linux-gnu.tar.xz";
            hash = "sha256-Up4z5tyBKw3oJA+mNpg6Z2UFzCZtNGM3H/+tk6EfVMY=";
          };
          aarch64-darwin = {
            url = "https://github.com/moonrepo/moon/releases/download/v${v.moon}/moon_cli-aarch64-apple-darwin.tar.xz";
            hash = "sha256-xOD0H0P4BTO+QSCReFjKaOHWUGM3XeAIiII0LkU2aGc=";
          };
        };
        protoc = {
          x86_64-linux = {
            url = "https://github.com/protocolbuffers/protobuf/releases/download/v${v.protoc}/protoc-${v.protoc}-linux-x86_64.zip";
            hash = "sha256-xLxnLZ1JIU3Iyv3OrfTfkhgtbKjj7GWlay195WAmabQ=";
          };
          aarch64-linux = {
            url = "https://github.com/protocolbuffers/protobuf/releases/download/v${v.protoc}/protoc-${v.protoc}-linux-aarch_64.zip";
            hash = "sha256-I3pohW7fG9KLYgS93QWWwc9G0pi8KcYgASVAsuRMc+c=";
          };
          aarch64-darwin = {
            url = "https://github.com/protocolbuffers/protobuf/releases/download/v${v.protoc}/protoc-${v.protoc}-osx-aarch_64.zip";
            hash = "sha256-3lbVev4wxdGRsR0k/5PdQCVyjX+0O3c4hrLTYT4L27I=";
          };
        };
        uv = {
          x86_64-linux = {
            url = "https://github.com/astral-sh/uv/releases/download/${v.uv}/uv-x86_64-unknown-linux-gnu.tar.gz";
            hash = "sha256-dFdlo7bjYK12dDWZrlxC6SeMft+Lv/n8dtBb8mI6BN0=";
          };
          aarch64-linux = {
            url = "https://github.com/astral-sh/uv/releases/download/${v.uv}/uv-aarch64-unknown-linux-gnu.tar.gz";
            hash = "sha256-LqpdlPXbezoaCSFWuUIEWeQqsCF9kX/nSodjCc75tek=";
          };
          aarch64-darwin = {
            url = "https://github.com/astral-sh/uv/releases/download/${v.uv}/uv-aarch64-apple-darwin.tar.gz";
            hash = "sha256-fm3bkxaswA8ilsgv9NmZd4cO40svDdyulETXFNuTZO0=";
          };
        };
        node = {
          x86_64-linux = {
            url = "https://nodejs.org/dist/v${v.node}/node-v${v.node}-linux-x64.tar.xz";
            hash = "sha256-/Y5Z1aURUQ9qKYr7VI8Yx9KxvkBNi0on2U++SfVsstY=";
          };
          aarch64-linux = {
            url = "https://nodejs.org/dist/v${v.node}/node-v${v.node}-linux-arm64.tar.xz";
            hash = "sha256-atEyXtvbVknDebdaI3FHpmbJXU+a6NNA/vLRV10omtI=";
          };
          aarch64-darwin = {
            url = "https://nodejs.org/dist/v${v.node}/node-v${v.node}-darwin-arm64.tar.xz";
            hash = "sha256-YjnUz5LYZEh+yM02FQOPe2fn9Yt3shzS8J6p+9aAZf4=";
          };
        };
        kache = {
          x86_64-linux = {
            url = "https://github.com/kunobi-ninja/kache/releases/download/v${v.kache}/kache-x86_64-unknown-linux-musl.tar.gz";
            hash = "sha256-Y1S7dkFL5o+PkGIGyHwTlOW3n968s6nTHiJU3frcKCA=";
          };
          aarch64-linux = {
            url = "https://github.com/kunobi-ninja/kache/releases/download/v${v.kache}/kache-aarch64-unknown-linux-musl.tar.gz";
            hash = "sha256-fIsOcYsEDybDWkNe7h298urUKvhSmxO+fKzJf1vI3sQ=";
          };
          aarch64-darwin = {
            url = "https://github.com/kunobi-ninja/kache/releases/download/v${v.kache}/kache-aarch64-apple-darwin.tar.gz";
            hash = "sha256-GUaUMOTH8ai8xWHCjBX9SH0ytR68Yski1x7n3iHwYPA=";
          };
        };
        # npm is one tarball for every platform. node bundles an older npm than
        # .moon/toolchains.yml pins, so the pinned one is unpacked over it.
        npm = {
          url = "https://registry.npmjs.org/npm/-/npm-${v.npm}.tgz";
          hash = "sha256-n1i/8BYEyxsUAI/vFNzrFNg2pJIl5FxsLjfeO+PnB/A=";
        };
        # Stub SDK for the Linux -> darwin cross link (candle metal pulls
        # Metal/Foundation). Platform-independent content.
        macosSdk = {
          url = "https://github.com/joseluisq/macosx-sdks/releases/download/${v.macosSdk}/MacOSX${v.macosSdk}.sdk.tar.xz";
          hash = "sha256-wVzw8/F9cU0apaZC2o4RjbU9eUKesBV3G6gWqnxsHL0=";
        };
      };

      # The devShell is built from `nixpkgs-toolchain` and must NOT reference
      # `self` or the source tree: CI keys a `nix print-dev-env` profile on the
      # hash of flake.nix, flake.lock and rust-toolchain.toml, and a source
      # reference would make every commit a fresh derivation (plan 2609-16 2.2).
      mkDevShell =
        system:
        let
          pkgs = import nixpkgs-toolchain {
            inherit system;
            overlays = [ rust-overlay.overlays.default ];
          };
          inherit (pkgs) lib stdenv;

          pinned =
            want: pkg:
            if pkg.version == want then
              pkg
            else
              throw "nixpkgs-toolchain has ${pkg.pname} ${pkg.version}, toolVersions pins ${want}: bump the pin with the input";

          src = tool: pkgs.fetchurl (sources.${tool}.${system} or sources.${tool});

          prebuilt =
            args:
            pkgs.stdenvNoCC.mkDerivation (
              {
                dontConfigure = true;
                dontBuild = true;
                # Release binaries are already stripped, and stripping a signed
                # darwin binary invalidates its signature.
                dontStrip = true;
                # Additive rather than `//`-overridable: a caller passing its
                # own nativeBuildInputs would otherwise drop autoPatchelfHook
                # and ship an ELF that only fails at first exec.
                nativeBuildInputs =
                  lib.optionals stdenv.hostPlatform.isLinux [ pkgs.autoPatchelfHook ]
                  ++ lib.optional (lib.hasSuffix ".zip" args.src.name) pkgs.unzip
                  ++ (args.nativeBuildInputs or [ ]);
                buildInputs =
                  lib.optionals stdenv.hostPlatform.isLinux [
                    stdenv.cc.cc.lib
                    pkgs.zlib
                  ]
                  ++ (args.buildInputs or [ ]);
              }
              // removeAttrs args [
                "nativeBuildInputs"
                "buildInputs"
              ]
            );

          rustToolchain = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;

          # cargo-zigbuild copies this def in at build time to work around zig
          # having no -lsynchronization (ziglang/zig#14919), and that write into
          # a read-only store path fails silently (.ok()). Bake it in instead.
          # The lib tree is symlink-copied (~220 MB of real files stay shared)
          # and zig is pointed at the copy through ZIG_LIB_DIR.
          zig =
            let
              base = pinned v.zig pkgs.zig_0_16;
            in
            pkgs.runCommand "zig-${v.zig}-pond" { nativeBuildInputs = [ pkgs.makeWrapper ]; } ''
              mkdir -p $out/bin $out/lib
              cp -as ${base}/lib/zig $out/lib/zig
              find $out/lib/zig -type d -exec chmod u+w {} +
              cp ${base}/lib/zig/libc/mingw/lib-common/api-ms-win-core-synch-l1-2-0.def \
                 $out/lib/zig/libc/mingw/lib-common/synchronization.def
              makeWrapper ${base}/bin/zig $out/bin/zig --set-default ZIG_LIB_DIR $out/lib/zig
            '';

          moon = prebuilt {
            pname = "moon";
            version = v.moon;
            src = src "moon";
            installPhase = "install -Dm755 moon $out/bin/moon";
          };

          # nixpkgs' protobuf is deliberately not used: protoc's exact version
          # is a pin the bootstrap actions mirror by hand, and nixpkgs' would
          # float with the input instead of staying that pin.
          protoc = prebuilt {
            pname = "protoc";
            version = v.protoc;
            src = src "protoc";
            sourceRoot = ".";
            installPhase = ''
              install -Dm755 bin/protoc $out/bin/protoc
              mkdir -p $out/include
              cp -r include/* $out/include/
            '';
          };

          uv = prebuilt {
            pname = "uv";
            version = v.uv;
            src = src "uv";
            installPhase = ''
              install -Dm755 uv $out/bin/uv
              install -Dm755 uvx $out/bin/uvx
            '';
          };

          kache = prebuilt {
            pname = "kache";
            version = v.kache;
            src = src "kache";
            sourceRoot = ".";
            installPhase = "install -Dm755 kache $out/bin/kache";
          };

          nodejs = prebuilt {
            pname = "nodejs";
            version = v.node;
            src = src "node";
            npmSrc = src "npm";
            installPhase = ''
              mkdir -p $out
              cp -a bin include lib share $out/
              chmod -R u+w $out/lib/node_modules
              rm -rf $out/lib/node_modules/npm
              mkdir -p $out/lib/node_modules/npm
              tar -xzf $npmSrc --strip-components=1 -C $out/lib/node_modules/npm
              ln -sf ../lib/node_modules/npm/bin/npm-cli.js $out/bin/npm
              ln -sf ../lib/node_modules/npm/bin/npx-cli.js $out/bin/npx
            '';
          };

          macosSdk = prebuilt {
            pname = "macosx-sdk";
            version = v.macosSdk;
            src = src "macosSdk";
            sourceRoot = ".";
            dontFixup = true;
            installPhase = ''
              mkdir -p $out
              cp -a MacOSX${v.macosSdk}.sdk $out/
            '';
          };
        in
        pkgs.mkShell (
          {
            name = "pond-dev";

            packages = [
              rustToolchain
              zig
              (pinned v.cargoZigbuild pkgs.cargo-zigbuild)
              (pinned v.rcodesign pkgs.rcodesign)
              (pinned v.gh pkgs.gh)
              moon
              protoc
              uv
              nodejs
              kache
              pkgs.cmake
              pkgs.pkg-config
              # ops/scripts/*.sh and the dist build's patch-macos-sdk.py.
              pkgs.python3
            ]
            # The SDK is the cross-link stub set; on darwin the native SDK that
            # comes with the stdenv clang wrapper is the right one.
            ++ lib.optional stdenv.hostPlatform.isLinux macosSdk;

            # bindgen consumers need libclang at runtime. The C compiler itself
            # stays the stdenv default (gcc on Linux, clang on darwin), which is
            # what the runner image and macos-verify use today - putting a
            # second clang on PATH would silently change who builds cc-crate code.
            LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";

            # moon's rust plugin prepends $CARGO_HOME/bin to PATH
            # (toolchains/rust/src/tier2.rs), so a rustup proxy left in the
            # default ~/.cargo would shadow this shell's rustc. Default to a
            # proxy-free home; an explicit CARGO_HOME (CI's /ci-cache/cargo) wins.
            shellHook = ''
              export CARGO_HOME="''${CARGO_HOME:-$HOME/.cargo-pond}"
              if [ -e "$CARGO_HOME/bin/rustc" ] || [ -e "$CARGO_HOME/bin/cargo" ]; then
                echo "warning: $CARGO_HOME/bin holds rustup proxies - they shadow this shell's rustc under moon" >&2
              fi
            '';
          }
          // lib.optionalAttrs stdenv.hostPlatform.isLinux {
            SDKROOT = "${macosSdk}/MacOSX${v.macosSdk}.sdk";
          }
        );
    in
    {
      lib = { inherit toolVersions; };

      overlays.default = final: _prev: { pond = final.callPackage ./ops/nix/pond.nix { }; };

      packages = forAllSystems (pkgs: rec {
        pond = pkgs.callPackage ./ops/nix/pond.nix { };
        default = pond;
      });

      apps = forAllSystems (pkgs: rec {
        pond = {
          type = "app";
          program = "${self.packages.${pkgs.system}.pond}/bin/pond";
          meta.description = "Run the pond CLI";
        };
        default = pond;
      });

      # `packages.pond` above unpacks a released binary, so it needs none of
      # this. Building the crate from a fresh clone does, and so does every CI
      # job: this shell is the single source of truth for the toolchain -
      # rust-toolchain.toml's rust, the cross-compile set (zig, cargo-zigbuild,
      # rcodesign, the SDK stubs) and the pinned user-space binaries (moon,
      # protoc, uv, node/npm, kache, gh). rustup is deliberately absent: it
      # would be a second Rust pin resolving against a different source.
      devShells = nixpkgs.lib.genAttrs systems (system: { default = mkDevShell system; });
    };
}

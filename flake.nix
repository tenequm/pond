{
  description = "pond - lossless session storage and search for AI agent clients";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs =
    { self, nixpkgs }:
    let
      # Prebuilt binaries exist only for these three; there is no
      # x86_64-darwin build, so it is deliberately absent.
      systems = [
        "aarch64-darwin"
        "x86_64-linux"
        "aarch64-linux"
      ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});
    in
    {
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
      # this. Building the crate from a fresh clone does: the native toolchain
      # below is the part a new machine is missing, and its absence surfaces as
      # a linker or build-script error deep in a dependency rather than as
      # anything that names pond.
      #
      #   cmake       aws-lc-sys drives a CMake build (build-dependencies.cmake),
      #               as does the protobuf-src that lance's `protoc` feature
      #               vendors.
      #   protobuf    a system protoc. Not the same build as CI's: the bootstrap
      #               pins protoc 35.1 explicitly, this tracks nixpkgs. Both
      #               satisfy the build; neither is load-bearing for output.
      #   pkg-config  the pkg-config crate is in the tree via libsqlite3-sys.
      #
      # The Rust toolchain is deliberately NOT pinned here: rust-toolchain.toml
      # already pins it and rustup honors that on both a dev machine and CI. A
      # second pin in this flake would be a second source of truth that drifts.
      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell {
          packages = with pkgs; [
            cmake
            protobuf
            pkg-config
          ];
        };
      });
    };
}

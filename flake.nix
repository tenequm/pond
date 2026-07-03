{
  description = "pond - lossless session storage and hybrid search for AI agent clients";

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
      overlays.default = final: _prev: { pond = final.callPackage ./nix/pond.nix { }; };

      packages = forAllSystems (pkgs: rec {
        pond = pkgs.callPackage ./nix/pond.nix { };
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
    };
}

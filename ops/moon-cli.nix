{ stdenvNoCC, fetchurl }:
let
  inherit (stdenvNoCC.hostPlatform) system;
  # The version comes from the .prototools pin at the repo root - the single
  # place a moon bump is typed. Only the hashes below are per-version data;
  # a bump whose hashes were not updated fails loudly at fetch, never
  # silently. New hashes come from the release's *.tar.xz.sha256 assets.
  version = (builtins.fromTOML (builtins.readFile ../.prototools)).moon;
  # musl on Linux: the static build runs on NixOS without ELF patching.
  # Darwin Mach-O needs no patching either, so no autoPatchelfHook anywhere.
  targetMap = {
    x86_64-linux = "x86_64-unknown-linux-musl";
    aarch64-linux = "aarch64-unknown-linux-musl";
    aarch64-darwin = "aarch64-apple-darwin";
  };
  target = targetMap.${system};
  shaMaps = {
    "2.5.4" = {
      x86_64-linux = "d289e8c0cdb30d080445a38c7f2635b7c2f2c29618abe7c2ef48ca29525ba926";
      aarch64-linux = "644c4c08975f35943bf4fbdfede7ee9c7cc98bc76ba7e8f0e8443c3029a1e525";
      aarch64-darwin = "c4e0f41f43f80533be4120917858ca68e1d65063375de0088882342e45366867";
    };
  };
  shaMap =
    shaMaps.${version} or (throw
      "ops/moon-cli.nix has no hashes for moon ${version}; add an entry from the v${version} release's .sha256 assets");
in
stdenvNoCC.mkDerivation {
  pname = "moon";
  inherit version;

  src = fetchurl {
    url = "https://github.com/moonrepo/moon/releases/download/v${version}/moon_cli-${target}.tar.xz";
    sha256 = shaMap.${system};
  };

  sourceRoot = "moon_cli-${target}";

  installPhase = ''
    runHook preInstall
    install -Dm755 moon $out/bin/moon
    runHook postInstall
  '';
}

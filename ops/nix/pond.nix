{ lib, fetchurl, stdenv, autoPatchelfHook, installShellFiles, zlib }:
let
  inherit (stdenv) hostPlatform;
  inherit (hostPlatform) system;
  version = "0.17.3";
  base = "https://github.com/tenequm/pond/releases/download/v${version}";
  shaMap = {
    x86_64-linux = "c0f54473e97352e2cd45f3cc893c8f94284d4171686f3debe62e4abb0db87965";
    aarch64-linux = "7021e2268348abdd44a8909ce160b823c452339ba7a6bb0a12f67daf0d1cc56b";
    aarch64-darwin = "ba19eb863333eaf70e8b584ce6a20f5dcf5c9f9940513b3bab3c8e1d3041cee3";
  };
  urlMap = {
    x86_64-linux = "${base}/pond-x86_64-unknown-linux-gnu.tar.xz";
    aarch64-linux = "${base}/pond-aarch64-unknown-linux-gnu.tar.xz";
    aarch64-darwin = "${base}/pond-aarch64-apple-darwin.tar.xz";
  };
in
stdenv.mkDerivation {
  pname = "pond";
  inherit version;

  src = fetchurl {
    url = urlMap.${system};
    sha256 = shaMap.${system};
  };

  sourceRoot = ".";

  # Prebuilt glibc ELF won't run on NixOS until its interpreter and RPATH
  # are rewritten to Nix-store paths; darwin Mach-O needs no patching.
  nativeBuildInputs = [ installShellFiles ] ++ lib.optionals hostPlatform.isLinux [ autoPatchelfHook ];

  # The released Linux build is CPU-only candle + vendored onig + rustls,
  # so the sole dynamic deps beyond glibc are libgcc_s/libstdc++.
  buildInputs = lib.optionals hostPlatform.isLinux [ stdenv.cc.cc.lib zlib ];

  # Completions ship pre-generated in the tarball: the binary can't
  # run here (autoPatchelfHook rewrites the interpreter later, in
  # fixupPhase).
  installPhase = ''
    runHook preInstall
    install -Dm755 pond $out/bin/pond
    installShellCompletion --bash completions/pond.bash --zsh completions/_pond --fish completions/pond.fish
    runHook postInstall
  '';

  meta = {
    description = "Lossless storage, full-text search, and optional semantic search for sessions from any AI agent client";
    homepage = "https://pond.locker/";
    changelog = "https://github.com/tenequm/pond/releases/tag/v${version}";
    license = lib.licenses.asl20;
    mainProgram = "pond";
    sourceProvenance = with lib.sourceTypes; [ binaryNativeCode ];
    platforms = [ "aarch64-darwin" "aarch64-linux" "x86_64-linux" ];
    maintainers = with lib.maintainers; [ ];
  };
}

{ lib, fetchurl, stdenv, autoPatchelfHook, installShellFiles, zlib }:
let
  inherit (stdenv) hostPlatform;
  inherit (hostPlatform) system;
  version = "0.17.0";
  base = "https://github.com/tenequm/pond/releases/download/v${version}";
  shaMap = {
    x86_64-linux = "a08181ecc19130bf5d42e3a7a9ff256b920d12a15f09cde6a274b572b9c5969b";
    aarch64-linux = "ec45d3867e91efd88034226e2c7c8b6ec1138e0b73cd3711d1d9f10616af0855";
    aarch64-darwin = "8b187f7bc921a6fec4724fe6ed93ba86def9efa928b0e8556dbf99b1b8cebe87";
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

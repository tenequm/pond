# Install

Linux and macOS are supported; Windows is not in v1 scope.

Package Managers (macOS and Linux):

```sh
brew install tenequm/tap/pond                       # Homebrew
nix profile add github:tenequm/nur-packages#pond    # Nix
cargo install pond-db                               # crates.io (installs the `pond` command)
```

Build from source:

```sh
git clone https://github.com/tenequm/pond.git
cd pond
cargo install --path .
```

For CUDA acceleration on Linux:

```sh
cargo install --path . --features cuda
```

On macOS the Metal backend is selected automatically; on other systems the CPU fallback runs without extra features.

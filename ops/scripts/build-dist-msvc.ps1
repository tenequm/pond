# The shipped msvc zip, built natively on a Windows runner and driven by the
# `repo:build-dist-msvc` moon task. In a file rather than inline in moon.yml for
# the same reason as ops/scripts/build-dist.sh: moon fingerprints an inline
# `script:` verbatim, so a reworded comment invalidated a full thin-LTO build.
# As a declared file input, only a real change does.
#
# Completions come from the just-built binary, the same way the Linux tarballs
# get theirs. `Out-Null` and `Set-Content` both consume the whole stream, so
# neither trips the EPIPE that a short-circuiting matcher like
# `Select-String -Quiet` hands the still-writing binary - which is why the
# emptiness check reads the file back instead of piping into it.
$ErrorActionPreference = 'Stop'
# 'Stop' alone governs cmdlets only - a native command that exits non-zero is
# not an error to PowerShell, and this variable is what makes it one (pwsh 7.3+,
# default $false). Without it a failed `cargo build` falls through to the
# packaging below, which is the one way this task could ship a zip built from
# something other than this commit. Today every later step would still throw on
# the missing exe, but that is a property of a runner whose target/ starts
# empty, not of this script.
$PSNativeCommandUseErrorActionPreference = $true
$env:POND_BUILD_COMMIT = (git rev-parse --short HEAD)
# --features windows-launcher builds pondw.exe (gated in Cargo.toml).
cargo build --locked --profile dist --target x86_64-pc-windows-msvc --features windows-launcher

$exe = "target\x86_64-pc-windows-msvc\dist\pond.exe"
& $exe --version
& $exe --help | Out-Null

# rm first: moon can hydrate a previously cached dist/ into the tree.
Remove-Item -Recurse -Force stage -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force -Path stage\completions | Out-Null
Copy-Item $exe stage\pond.exe
# The windowless launcher the scheduled task Execs; `pond schedule start`
# refuses to register without it.
Copy-Item target\x86_64-pc-windows-msvc\dist\pondw.exe stage\pondw.exe
& $exe completions powershell | Set-Content -Encoding utf8 stage\completions\_pond.ps1
& $exe completions bash       | Set-Content -Encoding utf8 stage\completions\pond.bash
& $exe completions zsh        | Set-Content -Encoding utf8 stage\completions\_pond
& $exe completions fish       | Set-Content -Encoding utf8 stage\completions\pond.fish
if (-not (Select-String -Path stage\completions\_pond.ps1 -Pattern 'pond' -Quiet)) { throw 'empty completions' }

New-Item -ItemType Directory -Force -Path dist | Out-Null
Compress-Archive -Path stage\pond.exe,stage\pondw.exe,stage\completions -DestinationPath dist\pond-x86_64-pc-windows-msvc.zip -Force
Remove-Item -Recurse -Force stage

use std::error::Error;

// Windows application manifest. `new_manifest()` already defaults to the three
// settings pond wants - UTF-8 active code page, per-monitor DPI awareness, and
// asInvoker (no UAC elevation heuristics) - plus longPathAware, which is cheap
// insurance for the C dependencies and spawned children on machines that also
// set the LongPathsEnabled registry key. Rust's own std has never been
// MAX_PATH-bound; it converts to `\\?\` verbatim paths internally.
//
// Gated on CARGO_CFG_WINDOWS (the target), not the host: embed_manifest is a
// no-op anywhere else.
fn main() -> Result<(), Box<dyn Error>> {
    if std::env::var_os("CARGO_CFG_WINDOWS").is_some() {
        embed_manifest::embed_manifest(embed_manifest::new_manifest("Pond.Pond"))?;
    }
    println!("cargo:rerun-if-changed=build.rs");
    Ok(())
}

# Error Handling

Rust has no exceptions. Every fallible function returns `Result<T, E>`, every "maybe absent" value is `Option<T>`, and the `?` operator stitches them together with one character. This file covers the patterns you will use every day.

## `Result<T, E>` and `?`

```rust
use std::fs;
use std::io;

fn read_config() -> Result<String, io::Error> {
    let s = fs::read_to_string("config.toml")?;  // returns early on Err
    Ok(s)
}
```

`?` does three things:
1. If the value is `Ok(v)`, unwrap and continue with `v`.
2. If `Err(e)`, return early from the function with `Err(e.into())`.
3. The `into()` calls `From` to convert the error type if needed (this is why `From` impls between error types are the foundation of error-handling ergonomics).

`?` works on `Option<T>` too: returns early with `None`.

## `Option<T>`

`Option<T>` is `Some(value)` or `None`. It replaces null. Common combinators:

```rust
let n: Option<i32> = Some(5);

n.unwrap();                       // panics on None - prototypes only
n.expect("must be set");         // panics with message - prototypes only
n.unwrap_or(0);                  // default value
n.unwrap_or_else(|| compute());  // default from a closure
n.map(|x| x * 2);                // Some(10), None stays None
n.and_then(|x| checked_div(x, 0)); // chain another Option-returning op
n.ok_or("missing");              // turn Option into Result
```

## `panic!`, `unwrap`, `expect` - When Each Is Appropriate

| Usage | Acceptable | Why |
|---|---|---|
| `panic!("bug: ...")` | Truly unreachable code, broken invariants | Bug indicator, not a control flow tool |
| `unwrap()` | Tests, prototypes, `examples/`, throwaway scripts | Crashes the program with a stack trace |
| `expect("msg")` | Same as `unwrap`, with a message documenting the assumption | Better than `unwrap` because the message helps debugging |
| `?` | Production code | Propagates the error upward |
| `.unwrap_or(default)` | When a default makes sense | Recovery without panic |

**Rule of thumb for app code**: outside of `main()` (which can panic), use `?` everywhere. `unwrap` and `expect` should be flagged in code review unless paired with a comment explaining why the case is impossible.

## `anyhow` for Application Code

Use `anyhow` in binaries, scripts, and any code where you do not need callers to react to specific error variants. It gives you a single `anyhow::Error` type that anything implementing `std::error::Error` can become.

```rust
use anyhow::{Context, Result, bail};

fn load_user(id: u64) -> Result<User> {
    let bytes = std::fs::read(format!("users/{id}.json"))
        .with_context(|| format!("reading user {id}"))?;
    let user: User = serde_json::from_slice(&bytes)
        .context("parsing user JSON")?;
    if !user.email.contains('@') {
        bail!("user {id} has no @ in email");
    }
    Ok(user)
}
```

Key features:
- **`Result<T>`**: type alias for `Result<T, anyhow::Error>`.
- **`.context(...)`**: attaches a string to an error so the chain reads top-down.
- **`bail!("msg")`**: shorthand for `return Err(anyhow!("msg"))`.
- **`anyhow!("msg")`**: builds an error from a format string.

`anyhow::Error` prints the full chain when displayed with `{:?}`:
```
Error: parsing user JSON

Caused by:
    expected `,` at line 3 column 5
```

## `thiserror` for Library Code

Library APIs should expose typed error enums so callers can match on specific variants. `thiserror` derives the boilerplate.

```rust
use thiserror::Error;

#[derive(Debug, Error)]
pub enum LoadError {
    #[error("user file not found: {path}")]
    NotFound { path: String },

    #[error("io error")]
    Io(#[from] std::io::Error),       // From impl auto-derived

    #[error("invalid JSON")]
    Parse(#[from] serde_json::Error),

    #[error("user {id} has no email")]
    MissingEmail { id: u64 },
}

pub fn load(id: u64) -> Result<User, LoadError> {
    let bytes = std::fs::read(format!("users/{id}.json"))?;
    let user: User = serde_json::from_slice(&bytes)?;
    if user.email.is_empty() {
        return Err(LoadError::MissingEmail { id });
    }
    Ok(user)
}
```

The `#[from]` attribute generates the `From` impl that `?` needs to convert from `io::Error` or `serde_json::Error` into `LoadError`.

**Do not return `Box<dyn std::error::Error>` from a public library API.** It forces callers to downcast to inspect the error. Define the enum.

## Combining `anyhow` and `thiserror`

The standard pattern in a project that has both library and binary crates:

- Library crates define typed errors with `thiserror`.
- Binary crates use `anyhow` and let `?` convert library errors via `From` (which `thiserror` derived for free, since `thiserror` errors implement `std::error::Error` and `anyhow::Error: From<E> where E: std::error::Error`).

This way binaries get ergonomic `?` everywhere, libraries give callers something useful to match on, and no boilerplate is duplicated.

## Custom Error Enums Without `thiserror`

You do not have to use `thiserror`. Plain enums work, you just write the impls yourself:

```rust
use std::fmt;

#[derive(Debug)]
pub enum MyError {
    NotFound,
    Io(std::io::Error),
}

impl fmt::Display for MyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MyError::NotFound => write!(f, "not found"),
            MyError::Io(e) => write!(f, "io: {e}"),
        }
    }
}

impl std::error::Error for MyError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            MyError::NotFound => None,
            MyError::Io(e) => Some(e),
        }
    }
}

impl From<std::io::Error> for MyError {
    fn from(e: std::io::Error) -> Self { MyError::Io(e) }
}
```

Use `thiserror`. The above is what it generates.

## `?` Conversion: How `From` Powers Error Propagation

When `?` returns from a function, it calls `.into()` on the error. `.into()` calls `From::from`. So `?` works whenever there is a `From` impl from the inner error type to the outer error type.

```rust
fn outer() -> Result<(), MyError> {
    let _bytes = std::fs::read("x")?;   // io::Error -> MyError via From impl
    Ok(())
}
```

If you derived `#[from]` on a variant with `thiserror`, this Just Works. If you wrote the enum by hand, write the `From` impl. If you use `anyhow::Error`, all errors that impl `std::error::Error` convert automatically.

## `Result` in `main`

`fn main` can return a `Result`. With `anyhow`:

```rust
fn main() -> anyhow::Result<()> {
    let user = load_user(1)?;
    println!("{user:?}");
    Ok(())
}
```

If `main` returns `Err`, the program exits with a non-zero status and prints the error chain via `Debug`.

## Common Pitfalls

- **Returning `Result<T, String>`**: tempting in early days, but you lose the error chain (`source()`), and you cannot `?`-convert from other error types. Use `anyhow` or `thiserror`.
- **`.unwrap()` because the error type is annoying**: define the conversion with `From` once, then `?` everywhere.
- **Catching every error and logging it**: errors are values; let them propagate to a single point that decides how to log or display them. Your app likely has one or two such points (request handler, CLI entry).
- **Ignoring `Result`**: `let _ = fallible();` discards an error silently. The compiler warns by default; do not silence the warning without a comment.

## Quick Reference

```rust
// Construct
Ok(value)            Err(my_error)
Some(value)          None

// Unwrap / propagate
val?                 // propagate
val.unwrap()         // panic on Err / None
val.unwrap_or(d)     // default
val.unwrap_or_else(|| compute())
val.expect("msg")    // panic with message

// Transform
val.map(|x| ...)              // Result<T,E> -> Result<U,E>
val.map_err(|e| ...)          // Result<T,E> -> Result<T,F>
val.and_then(|x| ...)         // chain Result-returning op
val.or_else(|e| ...)          // recover from Err
val.ok()                      // Result<T,E> -> Option<T>
val.ok_or(my_err)             // Option<T> -> Result<T,E>

// Build (anyhow)
anyhow::anyhow!("msg with {var}")
anyhow::bail!("msg")          // = return Err(anyhow!("msg"))
result.context("doing X")
result.with_context(|| format!("doing X for {id}"))
```

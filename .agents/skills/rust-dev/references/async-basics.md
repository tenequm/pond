# Async Basics

Rust's async is cooperative: `.await` is an explicit yield point. There is no built-in runtime; you pick one. In 2026, that runtime is `tokio` for almost every application. This file covers what you need to write async Rust well from day 1, and the small set of pitfalls that cause most async bugs.

## Mental Model

An `async fn` does not run when called. It returns a `Future`, which is a state machine. A runtime (`tokio`) drives futures by polling them; when a poll hits a point that needs to wait (network I/O, timer, channel receive), the future returns "not ready" and the runtime parks it until the underlying event fires.

```rust
async fn add(a: i32, b: i32) -> i32 { a + b }

let f = add(2, 3);   // f is a Future, nothing has run yet
let n = f.await;     // runtime drives f to completion; n == 5
```

`.await` only works inside `async fn` or `async {}` blocks.

## The Minimum You Need

```rust
// Cargo.toml
// [dependencies]
// tokio = { version = "1", features = ["full"] }

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let body = reqwest::get("https://example.com").await?.text().await?;
    println!("{}", body.len());
    Ok(())
}
```

`#[tokio::main]` is a macro that wraps `main` with the runtime setup. For tests, use `#[tokio::test]`.

```rust
#[tokio::test]
async fn it_works() {
    assert_eq!(add(2, 3).await, 5);
}
```

## `tokio` Features

`tokio = { version = "1", features = ["full"] }` is fine while learning. In production, narrow the feature list:

```toml
tokio = { version = "1", features = ["macros", "rt-multi-thread", "net", "fs", "sync", "time"] }
```

Common ones:
- `macros` - `#[tokio::main]`, `#[tokio::test]`, `tokio::select!`, `tokio::join!`
- `rt-multi-thread` - the default work-stealing runtime
- `rt` - single-threaded runtime (use this in WASM, embedded, or to avoid `Send` bounds)
- `net` - TCP/UDP
- `fs` - async filesystem
- `sync` - `Mutex`, `RwLock`, `mpsc`, `broadcast`, `oneshot`, `Notify`
- `time` - `sleep`, `interval`, `timeout`
- `process` - spawning subprocesses
- `signal` - Ctrl-C, SIGTERM handling

## Spawning Tasks

`tokio::spawn` puts a future on the runtime so it can run concurrently with the current task.

```rust
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() {
    let handle = tokio::spawn(async {
        sleep(Duration::from_secs(1)).await;
        42
    });

    let result = handle.await.unwrap();   // wait for the spawned task
    println!("{result}");
}
```

`spawn` returns a `JoinHandle<T>`. Awaiting it gives you the task's return value (wrapped in `Result` to handle panic).

## `Send`, `Sync`, and `'static` Bounds

Tasks spawned with `tokio::spawn` must be `Send + 'static` because the runtime moves them across threads.

- `Send` means "safe to move to another thread."
- `Sync` means "safe to share by reference (`&T`) across threads."
- `'static` means "owns all its data; does not borrow from the spawning function's stack."

If you see `error: future cannot be sent between threads safely`, you have likely:
- Held a non-`Send` type across an `.await` (e.g., `Rc<T>`, `RefCell<T>`, `std::sync::MutexGuard`).
- Captured a reference instead of moving owned data into the task.

The most common fix: replace `Rc` with `Arc`, `RefCell` with `Mutex` (and check the next pitfall), and `move` the closure body into the task.

```rust
let data = Arc::new(some_data);

tokio::spawn({
    let data = Arc::clone(&data);     // clone Arc, move into task
    async move {
        process(&data).await;
    }
});
```

## The `MutexGuard` Across `.await` Pitfall

Holding a synchronous `std::sync::MutexGuard` across an `.await` is wrong:
1. The guard is not `Send`, so the future is not `Send`, so `tokio::spawn` rejects it.
2. Even when it compiles, you can deadlock: the task yields while still holding the lock, and another task waiting for the lock blocks the runtime thread.

Two fixes, depending on need:

**A. Drop the guard before the await:**
```rust
use std::sync::Mutex;
let state = Arc::new(Mutex::new(Counter::new()));

let snapshot = {
    let g = state.lock().unwrap();
    g.snapshot()       // get the data we need
};                     // guard dropped here
do_async_thing(snapshot).await;
```

**B. Use `tokio::sync::Mutex` (async-aware):**
```rust
use tokio::sync::Mutex;
let state = Arc::new(Mutex::new(Counter::new()));

let mut g = state.lock().await;     // this await is OK
do_async_thing(&mut g).await;       // holding the lock across await is OK
```

`tokio::sync::Mutex` is slower than `std::sync::Mutex`. Default to `std::sync::Mutex` and scope your locks tightly. Reach for `tokio::sync::Mutex` only when you genuinely need to hold a lock across awaits.

## Don't Block the Runtime

Async runtimes assume tasks yield quickly. CPU-bound work (parsing big files, encoding video, expensive computations) starves other tasks. Two escapes:

**`tokio::task::spawn_blocking`** for synchronous, CPU-heavy work:
```rust
let result = tokio::task::spawn_blocking(|| expensive_computation()).await?;
```

**`tokio::task::block_in_place`** when you cannot spawn (e.g., inside a closure that needs to return synchronously):
```rust
tokio::task::block_in_place(|| do_blocking_thing());
```

Never call `std::thread::sleep` in async code. Use `tokio::time::sleep`. Never call blocking I/O (`std::fs::read`, `std::net::TcpStream`) on a runtime thread. Use `tokio::fs`, `tokio::net`, or wrap with `spawn_blocking`.

## Concurrency: `select!`, `join!`, `try_join!`

Run multiple futures concurrently in the same task.

```rust
use tokio::{join, try_join, select};

// Run both, wait for both
let (a, b) = join!(fetch_user(1), fetch_user(2));

// Run both, wait for both, short-circuit on error
let (a, b) = try_join!(fetch_user(1), fetch_user(2))?;

// Run both, take whichever finishes first
select! {
    a = fetch_user(1) => println!("got {a:?}"),
    _ = tokio::time::sleep(Duration::from_secs(5)) => println!("timeout"),
}
```

For dynamic-sized concurrency, use `futures::future::join_all` or `tokio::task::JoinSet`:

```rust
use tokio::task::JoinSet;

let mut set = JoinSet::new();
for id in 0..100 {
    set.spawn(fetch_user(id));
}
while let Some(res) = set.join_next().await {
    println!("{:?}", res?);
}
```

## Cancellation

Dropping a future cancels it. The future stops being polled at its next `.await` point. There is no "cancellation token" by default; structuring code so that dropping is a clean shutdown is the idiom.

For explicit cancellation across many tasks: `tokio_util::sync::CancellationToken`. For timeouts: `tokio::time::timeout`:

```rust
use tokio::time::{timeout, Duration};

match timeout(Duration::from_secs(5), slow_op()).await {
    Ok(Ok(value)) => { /* success */ }
    Ok(Err(e)) => { /* slow_op returned Err */ }
    Err(_) => { /* timed out */ }
}
```

## Channels (`tokio::sync`)

| Channel | Use |
|---|---|
| `mpsc` | Many producers, one consumer (work queue, command bus) |
| `oneshot` | Single send, single receive (request/response) |
| `broadcast` | Many producers, many consumers (each receiver gets every message; lossy if slow) |
| `watch` | Many producers, many consumers (each receiver gets only the latest value) |

```rust
use tokio::sync::mpsc;

let (tx, mut rx) = mpsc::channel::<String>(32);

tokio::spawn(async move {
    while let Some(msg) = rx.recv().await {
        println!("got: {msg}");
    }
});

tx.send("hi".into()).await?;
```

## Async in Traits

Native async functions in traits stabilized in Rust 1.75. They work for object-safety-irrelevant cases:

```rust
trait Greeter {
    async fn greet(&self) -> String;
}
```

Limitations: an `async fn` in a trait cannot be used as `dyn Trait` directly without help. For that, the `async-trait` crate is still common:

```toml
async-trait = "0.1"
```

```rust
#[async_trait::async_trait]
pub trait Greeter {
    async fn greet(&self) -> String;
}
```

`async-trait` boxes the future, which costs an allocation per call but lets you use `dyn Greeter`. For most app code, this is fine. Library authors writing performance-critical traits often avoid `async-trait` and use the native form with `impl Future` returns.

## Common Pitfalls

1. **Forgetting `.await`**: returns a `Future` that does nothing.
2. **Holding a `std::sync::MutexGuard` across `.await`**: see above.
3. **Calling blocking I/O in an async function**: use `spawn_blocking` or the async equivalent (`tokio::fs`).
4. **Spawning tasks that borrow from the parent**: `tokio::spawn` requires `'static`. Move owned data in (often via `Arc::clone`).
5. **`select!` arms with side effects**: when one arm completes, the others are dropped (cancelled). Make sure that is safe for the operations involved.
6. **Forgetting that `async fn` returns immediately**: until you `.await` or `spawn`, no work happens.
7. **One giant `tokio::main` task with no concurrency**: if your `main` is awaiting things sequentially, you may not need async at all. Async pays off when you have concurrent I/O.

## What to Defer

- Manual `Future` impls and `Pin`. Almost no app code needs this.
- `poll_*` methods on lower-level traits (`AsyncRead`, `AsyncWrite`).
- Custom executors. Use `tokio` until you have a proven reason not to.
- `tokio` internals (`tokio-uring`, `LocalSet`, custom schedulers). Only relevant for advanced cases.

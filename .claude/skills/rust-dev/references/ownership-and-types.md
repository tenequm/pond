# Ownership and Types

Deep dive on the rules that govern every Rust program: who owns what, who can read it, who can change it. Also: when to use `String` vs `&str`, `Vec<T>` vs `&[T]`, and the smart pointers (`Box`, `Rc`, `Arc`, `RefCell`, `Mutex`).

## The Three Ownership Rules

1. Every value has exactly one owner (a variable).
2. When the owner goes out of scope, the value is dropped (memory freed, file closed, etc.).
3. There can be either many readers (`&T`) or one writer (`&mut T`), never both at once.

That is the entire system. Lifetimes are bookkeeping that proves rule 3 to the compiler when references cross function or struct boundaries.

## Move, Borrow, Clone

```rust
let a = String::from("hello");

// MOVE: ownership transfers; a is no longer usable
let b = a;
// println!("{a}");  // compile error: value moved

// BORROW: a keeps ownership, c looks at it
let c = &b;          // immutable borrow
println!("{c}");

// MUTABLE BORROW: exclusive write access
let mut s = String::from("hi");
let m = &mut s;
m.push_str("!");

// CLONE: deep copy, both keep their own
let d = b.clone();
```

`Copy` types (integers, bools, char, fixed-size tuples of `Copy`, `&T`) are copied implicitly on assignment instead of moved. They are cheap bitwise duplications. `String`, `Vec<T>`, `Box<T>`, etc., are not `Copy`; they move.

## Borrowing Rules in Practice

```rust
let mut v = vec![1, 2, 3];
let r = &v;
let m = &mut v;       // ERROR: cannot borrow as mutable while r is alive
println!("{r:?}");
```

The fix is to end the immutable borrow before starting the mutable one, often by restructuring the code:

```rust
let mut v = vec![1, 2, 3];
{
    let r = &v;
    println!("{r:?}");
}                      // r dropped here
let m = &mut v;        // now fine
m.push(4);
```

In modern Rust (NLL: non-lexical lifetimes), the compiler often shortens borrow scopes automatically. You usually do not need explicit blocks.

## `String` vs `&str` vs `Cow<str>`

| Type | What it is | Owns? | When to use |
|---|---|---|---|
| `String` | Heap-allocated, growable UTF-8 | Yes | Struct fields, return values, buffers you mutate |
| `&str` | Borrowed view into a UTF-8 string | No | Function parameters; string literals (`"hi"` is `&'static str`) |
| `&'static str` | Borrowed string with program-long lifetime | No | Constants, hardcoded strings |
| `Cow<'_, str>` | Either owned or borrowed | Sometimes | When most calls do not need to allocate but a few do |

```rust
// Good: parameter is &str, accepts both String and &str callers
fn shout(s: &str) -> String {
    s.to_uppercase()
}

shout("hi");
shout(&String::from("hi"));   // &String coerces to &str via deref
```

The newcomer trap: putting `&str` in a struct field. The struct now needs a lifetime parameter, and so does every function and struct that holds it. Use `String` and stop the cascade.

## `Vec<T>` vs `&[T]` vs `[T; N]`

| Type | What it is | When to use |
|---|---|---|
| `Vec<T>` | Heap-allocated, growable array | Field, owned collection, return value |
| `&[T]` | Borrowed slice, view into a contiguous run | Function parameter; accepts `&Vec<T>`, `&[T; N]`, slice |
| `[T; N]` | Fixed-size array, size known at compile time | Small fixed buffers, lookup tables |

```rust
// Generic over input source: works with Vec, array, or slice
fn sum(xs: &[i32]) -> i32 {
    xs.iter().sum()
}

sum(&vec![1, 2, 3]);
sum(&[1, 2, 3]);
sum(&[1, 2, 3][..]);
```

## Smart Pointers

| Pointer | Ownership model | Thread-safe? | Mutability |
|---|---|---|---|
| `Box<T>` | Single owner, heap allocation | n/a | Through `&mut Box<T>` or by moving out |
| `Rc<T>` | Shared owner, ref-counted | NO | Read-only; pair with `RefCell<T>` for interior mutability |
| `Arc<T>` | Shared owner, atomic ref-counted | YES | Read-only; pair with `Mutex<T>` or `RwLock<T>` |
| `RefCell<T>` | Single owner, runtime-checked borrows | NO | Mutable through `.borrow_mut()` (panics if rule 3 violated) |
| `Mutex<T>` | Owner gates access via lock | YES | Mutable through `.lock()` |
| `RwLock<T>` | Many readers or one writer | YES | Many `.read()`, one `.write()` |

### When to pick which

- **Single owner, just needs to be on the heap** (e.g., recursive types, large structs in enums): `Box<T>`.
- **Shared ownership across one thread** (e.g., a tree where multiple parents point to the same child): `Rc<T>`. Reach for it sparingly; it usually signals a graph structure that could be flattened.
- **Shared ownership across threads** (e.g., shared state in a `tokio::spawn`'d task): `Arc<T>`.
- **Need to mutate through a shared pointer**:
  - Single thread: `Rc<RefCell<T>>` (not recommended as default)
  - Multi-thread / async: `Arc<Mutex<T>>` (recommended default for shared mutable state)

### Why default to `Arc<Mutex<T>>` over `Rc<RefCell<T>>`

`Rc` and `RefCell` are not `Send`, so the moment you spawn an async task or thread, you have to refactor. Picking `Arc<Mutex<T>>` from the start avoids that refactor. The performance cost (atomic ops vs non-atomic) is negligible compared to the cost of reorganizing your code.

### Mutex pitfall in async

Holding a `MutexGuard` across an `.await` point can deadlock. Either drop the guard before awaiting, or use `tokio::sync::Mutex` (async-aware) for state that needs to be locked across awaits.

```rust
use std::sync::Mutex;

let state = Arc::new(Mutex::new(Counter::new()));

// BAD: guard held across .await
{
    let guard = state.lock().unwrap();
    do_async_thing().await;        // tasks waiting on this lock are stuck
}

// GOOD: scope the lock tightly
{
    let mut guard = state.lock().unwrap();
    guard.bump();
}                                  // guard dropped here
do_async_thing().await;
```

## Lifetimes (When You Cannot Avoid Them)

Most function lifetimes are inferred (lifetime elision). You only write them when:

1. A function takes multiple input references and returns a reference, and the compiler cannot tell which input the output borrows from.
2. A struct field is a reference (which you should mostly avoid).

```rust
// Elided: compiler infers 'a for both input and output
fn first_word(s: &str) -> &str { /* ... */ }

// Explicit: compiler does not know if the output borrows from x or y
fn longest<'a>(x: &'a str, y: &'a str) -> &'a str {
    if x.len() > y.len() { x } else { y }
}
```

The `'static` lifetime means "lives for the entire program." String literals and most constants are `'static`. Use it sparingly; it is more constraint than asset.

## The Self-Referential Struct Trap

You will at some point want to write something like:

```rust
struct Parsed {
    text: String,
    first_word: &str,   // points into self.text
}
```

This does not work in safe Rust. A struct cannot hold a reference to itself, because moving the struct would invalidate the reference. Workarounds:

1. Store an index or range instead of a reference: `first_word: std::ops::Range<usize>`. Recompute the slice on demand.
2. Use a crate like `ouroboros` or `self_cell` if you really need this pattern.
3. Refactor to keep the parsed pieces and the source string in separate owners.

Most of the time option 1 is the right answer.

## When You Find Yourself Cloning a Lot

`.clone()` is fine when learning. Some cases where it is correct in production:
- You genuinely need two independent copies (e.g., one for a callback, one for the current scope).
- Cloning is cheap (`Arc<T>` clone is one atomic increment).
- The alternative is significantly more complex and the perf does not matter.

When it is wrong:
- You are cloning a large `Vec<T>` per request to "fix" a borrow error. The borrow error is telling you the data ownership is unclear. Step back and look at the design.

## `Cow<T>` (Clone on Write)

`std::borrow::Cow<'a, T>` is "borrowed if possible, owned if necessary." Useful when most callers do not need to mutate or own the value, but some do.

```rust
use std::borrow::Cow;

fn normalize(s: &str) -> Cow<'_, str> {
    if s.contains('\r') {
        Cow::Owned(s.replace('\r', ""))      // allocate only when needed
    } else {
        Cow::Borrowed(s)                     // zero allocation common case
    }
}
```

Most code does not need `Cow`. Reach for it when profiling shows allocation pressure on a hot path.

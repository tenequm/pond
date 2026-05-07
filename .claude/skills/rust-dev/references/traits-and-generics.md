# Traits and Generics

Traits are how Rust does polymorphism. Generics give you static dispatch (zero runtime cost). Trait objects (`dyn Trait`) give you dynamic dispatch (one vtable lookup). Traits are not Java/C# interfaces in the way that matters most: there is no runtime cost by default.

## Defining and Implementing a Trait

```rust
trait Greet {
    fn hello(&self) -> String;

    // Default method body - implementors can override
    fn loud_hello(&self) -> String {
        self.hello().to_uppercase()
    }
}

struct Cat;

impl Greet for Cat {
    fn hello(&self) -> String {
        "meow".into()
    }
}
```

## Three Ways to Use a Trait in a Function

```rust
// 1. Generic with a trait bound (monomorphized, static dispatch)
fn greet1<T: Greet>(x: T) {
    println!("{}", x.hello());
}

// 2. impl Trait sugar (same as above, less verbose, only one type per call site)
fn greet2(x: impl Greet) {
    println!("{}", x.hello());
}

// 3. Trait object (dynamic dispatch, one vtable lookup per call)
fn greet3(x: &dyn Greet) {
    println!("{}", x.hello());
}
```

| Form | Dispatch | Code size | When to use |
|---|---|---|---|
| `<T: Trait>` | Static (monomorphized) | Larger (one copy per type) | The default for library code |
| `impl Trait` | Static (monomorphized) | Larger | Same as above; cleaner for one trait bound |
| `&dyn Trait` / `Box<dyn Trait>` | Dynamic (vtable) | Smaller, one copy | Heterogeneous collections, plugin-like APIs, when you need to store mixed types |

### When you actually need `dyn Trait`

```rust
// Cannot use generics: Vec needs a single concrete type
let animals: Vec<Box<dyn Greet>> = vec![
    Box::new(Cat),
    Box::new(Dog),
];

for a in &animals {
    println!("{}", a.hello());
}
```

If your collection is homogeneous (all the same type), use a generic. If it is heterogeneous, you need `dyn`.

### Object safety

Not every trait can be used as `dyn Trait`. The compiler will tell you when you violate object safety. Common reasons:
- The trait has methods returning `Self` (e.g., `Clone`).
- The trait has generic methods (`fn f<T>(&self, x: T)`).
- The trait has methods without a `&self`/`&mut self`/`self` receiver.

If you hit this, either redesign the trait or use a generic instead.

## Trait Bounds and `where` Clauses

```rust
fn print_all<T>(xs: &[T]) where T: std::fmt::Display {
    for x in xs {
        println!("{x}");
    }
}

// Multiple bounds with +
fn print_and_clone<T: std::fmt::Display + Clone>(x: &T) {
    println!("{x}");
    let _y = x.clone();
}
```

Use `where` when bounds get long; it improves readability:

```rust
fn complex<T, U>(t: T, u: U) -> Vec<T>
where
    T: Clone + std::fmt::Debug,
    U: IntoIterator<Item = T>,
{
    u.into_iter().collect()
}
```

## Common Derive Macros

`#[derive(...)]` auto-implements traits when the implementation is mechanical. The ones you will use constantly:

```rust
#[derive(
    Debug,         // {:?} formatting
    Clone,         // .clone() (deep copy)
    Copy,          // implicit-copy semantics (must also derive Clone; only for plain-data types)
    PartialEq, Eq, // == and !=
    Hash,          // for HashMap/HashSet keys
    PartialOrd, Ord, // < <= > >=
    Default,       // T::default()
)]
pub struct User {
    pub id: u64,
    pub email: String,
}

// With serde:
#[derive(serde::Serialize, serde::Deserialize)]
pub struct Payload { /* ... */ }

// With thiserror:
#[derive(thiserror::Error, Debug)]
pub enum MyError { /* ... */ }
```

Default these on every public struct unless you have a reason not to: `Debug`, `Clone`. Add `PartialEq` and `Eq` if you compare instances. Add `Hash` if you use them as map/set keys. `Copy` only for small types of POD shape (no allocations, no `Drop`).

## `From`, `Into`, `TryFrom`, `TryInto`

These four traits are how Rust handles type conversions. Implement `From` and you get `Into` for free.

```rust
struct UserId(u64);

impl From<u64> for UserId {
    fn from(n: u64) -> Self { UserId(n) }
}

let id: UserId = 42.into();         // uses From above
let id = UserId::from(42);          // same thing

// Fallible version
impl TryFrom<&str> for UserId {
    type Error = std::num::ParseIntError;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Ok(UserId(s.parse()?))
    }
}

let id: UserId = "42".try_into()?;
```

This is also how `?` converts error types: `?` calls `.into()` on the inner error, which uses your `From` impls.

## `Display` and `Debug`

Two ways to format a value as a string. They have different audiences.

```rust
use std::fmt;

struct Money { cents: i64 }

// Debug: developer-facing, "{:?}" or "{:#?}", usually #[derive(Debug)]
impl fmt::Debug for Money {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Money({})", self.cents)
    }
}

// Display: user-facing, "{}", explicit impl
impl fmt::Display for Money {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "${}.{:02}", self.cents / 100, self.cents.abs() % 100)
    }
}
```

Derive `Debug` always. Implement `Display` when there is a single, obvious string representation users should see.

## Blanket Implementations

A trait can be `impl`'d for any type satisfying some bound. The standard library does this constantly:

```rust
// In std: impl<T: Display> ToString for T { ... }
// So every Display type automatically gets .to_string()

let s: String = 42.to_string();       // works because i32: Display
```

Blanket impls are how `Into` exists for free when you write `From`, how iterator combinators work for any `Iterator`, etc.

## The Orphan Rule

You can `impl YourTrait for SomeoneElsesType`, or `impl SomeoneElsesTrait for YourType`, but not both foreign. This is the orphan rule. It exists so that two crates cannot independently implement the same foreign trait for the same foreign type and conflict at link time.

Workaround when you need to implement a foreign trait for a foreign type: wrap the foreign type in your own newtype.

```rust
struct MyVec(Vec<i32>);

impl std::fmt::Display for MyVec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MyVec({:?})", self.0)
    }
}
```

The newtype pattern is also how you add type safety to primitives:

```rust
struct UserId(u64);
struct OrderId(u64);

fn lookup(id: UserId) { /* ... */ }
// lookup(OrderId(5))   // compile error - good
```

## Iterator Trait

`Iterator` is the most-used trait in Rust. It has one required method (`next`) and dozens of provided combinators (`map`, `filter`, `collect`, etc.).

```rust
let total: i32 = (1..=10)
    .filter(|n| n % 2 == 0)
    .map(|n| n * n)
    .sum();
```

You implement `Iterator` for your own types when they are sequences:

```rust
struct Counter { n: u32 }

impl Iterator for Counter {
    type Item = u32;
    fn next(&mut self) -> Option<Self::Item> {
        self.n += 1;
        if self.n <= 5 { Some(self.n) } else { None }
    }
}
```

`for x in collection` desugars to `let mut iter = collection.into_iter(); while let Some(x) = iter.next()`. Three iter methods you must distinguish:

| Method | Yields | When |
|---|---|---|
| `iter()` | `&T` | Read-only iteration |
| `iter_mut()` | `&mut T` | Modify in place |
| `into_iter()` | `T` (consumes the collection) | Move items out |

## Common Standard Library Traits to Know

| Trait | What it represents |
|---|---|
| `Clone` | `.clone()` deep copy |
| `Copy` | Implicit bitwise copy on assignment (must also be `Clone`) |
| `Debug` | `{:?}` formatting |
| `Display` | `{}` formatting (user-facing) |
| `Default` | `T::default()` |
| `From<T>` / `Into<T>` | Conversion between types |
| `PartialEq` / `Eq` | `==` |
| `PartialOrd` / `Ord` | `<`, `<=`, `>`, `>=` |
| `Hash` | Map/set keys |
| `Iterator` | Sequence with `.next()` |
| `IntoIterator` | "Can be turned into an iterator" - powers `for` loops |
| `Drop` | Custom cleanup when value goes out of scope |
| `Deref` / `DerefMut` | Custom `*` and method-call autoderef (for smart-pointer-like types only) |
| `AsRef<T>` / `AsMut<T>` | Cheap reference-to-reference conversion (e.g., `Path: AsRef<OsStr>`) |
| `Borrow<T>` | Like `AsRef`, with stricter equality/hash guarantees (used by `HashMap::get`) |

## Generics: Type, Lifetime, and Const

```rust
fn count<T>(xs: &[T]) -> usize { xs.len() }                    // type generic
fn first<'a>(xs: &'a [i32]) -> &'a i32 { &xs[0] }              // lifetime generic
fn sum<const N: usize>(xs: [i32; N]) -> i32 { xs.iter().sum() } // const generic
```

You will mostly write type generics. Lifetime generics show up when you store references in structs or return references that depend on inputs. Const generics let you parameterize over compile-time values like array sizes; useful for fixed-size buffers and SIMD.

## When to Reach For Generics vs Trait Objects

- **Library code, performance-sensitive paths**: generics. Static dispatch, inlining, no heap allocation.
- **Heterogeneous collections, plugin systems, runtime-determined behavior**: trait objects (`Box<dyn Trait>`).
- **You think you need generics but only have one or two implementors**: just use a concrete type or an enum.

## Anti-Patterns

- **Single-method traits that should be closures.** If a trait has one method and no state, `Fn(...)` or `FnMut(...)` or `FnOnce(...)` may be the better abstraction.
- **`Deref` for inheritance simulation.** `Deref` is for smart-pointer-like wrappers (`Box`, `Rc`, `MutexGuard`) where the wrapper "is-a" pointer to the inner type. It is not a way to "extend" a struct.
- **Excessive generic parameters.** Two or three type parameters is normal. Five or more usually means a design that is too abstract.
- **Trait + always one impl.** A trait with one implementor is just an interface for nothing. Inline it until you have a second use case.

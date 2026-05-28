//! Adapter seam primitives: `Source`, `Extracted<T>`, and the `extract_*`
//! family.
//!
//! These types are the load-bearing contract for pond's adapter ecosystem
//! (CLAUDE.md "Adapter seam"). They make ONE thing impossible to express:
//! synthesizing a value the source data did not carry. The schema fields
//! that hold "did the source say this?" data are typed as
//! `Option<Extracted<T>>`, and the only way to construct an
//! `Extracted<T>` is to extract it from a `Source` via one of the
//! `extract_*` functions in this module. Adapters cannot produce a
//! sentinel ("unknown", "function", "") through any combination of trait
//! methods, conversions, or struct literals - the seal is module-private.
//!
//! Transport-agnostic by design: `Source` is a tiny trait that adapter
//! authors implement for their own row type, regardless of how the row
//! arrived (JSONL file, HTTP response body, WebSocket frame, queue
//! payload, database row). pond ships `impl Source for serde_json::Value`
//! for JSON-flavored adapters; the trait carries no transport assumptions.
//!
//! See spec.md#model-no-synthesis, spec.md#model-schema-honesty, and spec.md#model-lossless-projection for the underlying principles.

use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::Value;

/// A value that was pulled from real source data. The wrapper is opaque to
/// adapter code: there is no public constructor, no `From<T>`, no
/// `Default`, no consuming `into_inner`. Read-only access is via `Deref`
/// so consumers and storage code can use the inner value freely without
/// being able to forge new instances.
///
/// The only path that produces an `Extracted<T>` is the
/// [`wrap`] function below, which is module-private and called solely by
/// the `extract_*` helpers in this file. An adapter that wants to put a
/// value into a schema field has to obtain it from a `Source` via one of
/// those helpers; no other path exists.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Extracted<T>(T);

impl<T> Extracted<T> {
    /// Borrow the inner value.
    #[allow(clippy::should_implement_trait)]
    pub fn as_ref(&self) -> &T {
        &self.0
    }

    /// Test-only constructor. Lets unit-test code build Part / Message
    /// literals directly without routing through a `Source` (which would
    /// force every test to stand up a synthetic JSON row). Compiled out of
    /// release builds via `#[cfg(test)]`, so production adapter code
    /// genuinely cannot reach this even from sibling modules.
    #[cfg(test)]
    pub fn from_test_value(value: T) -> Self {
        wrap(value)
    }

    /// Crate-internal constructor for the storage decode path. Lance
    /// stores values that originally came from a `Source` extraction; on
    /// read-back we need to rewrap them. NOT for adapter use - adapter
    /// code MUST go through `extract_*`. The `pub(crate)` visibility
    /// limits this to pond's own infrastructure (notably the
    /// `message_from_batch` / `part_from_batch` decoders in
    /// `src/sessions.rs`). Code review checks that no adapter module
    /// reaches for this.
    pub(crate) fn from_stored(value: T) -> Self {
        wrap(value)
    }
}

impl<T> std::ops::Deref for Extracted<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}

/// Module-private constructor. The single place in pond's codebase that
/// can wrap a raw `T` into `Extracted<T>`. Visibility is bounded to this
/// module so adapter code (which lives in sibling modules under
/// `src/adapter/`) cannot reach it - that is the seal that makes the
/// "no synthesized values" invariant compile-enforced.
fn wrap<T>(value: T) -> Extracted<T> {
    Extracted(value)
}

// Arrow `Utf8` columns address with an `i32` offset buffer: no stored text
// value may approach `i32::MAX`. The seam caps every extracted value at
// `LEAF_CAP`, truncating an oversized leaf to a head-preserving marker
// (spec.md#adapter-bounded-values).
pub(crate) const LEAF_CAP: usize = 10 * 1024 * 1024;

static TRUNCATED_VALUES: AtomicU64 = AtomicU64::new(0);

pub(crate) fn truncated_values_count() -> u64 {
    TRUNCATED_VALUES.load(Ordering::Relaxed)
}

fn record_truncation(original_bytes: usize) {
    TRUNCATED_VALUES.fetch_add(1, Ordering::Relaxed);
    // Per-occurrence detail at debug; the operator-facing signal is the
    // `truncated_values` count in the sync summary, always visible.
    tracing::debug!(
        original_bytes,
        cap_bytes = LEAF_CAP,
        "value exceeded the seam leaf cap; truncated to a marked sentinel"
    );
}

fn truncation_marker(original_bytes: usize) -> String {
    format!("<pond:truncated {original_bytes} bytes>")
}

/// Truncate an over-cap value to its head-preserving sentinel: a UTF-8 prefix
/// of `head` plus the `<pond:truncated N bytes>` marker, the whole staying
/// within `LEAF_CAP`. `original` is the value's true byte length, which may
/// exceed `head.len()` when the caller streamed and discarded the tail. The
/// sole definition of the truncation shape (spec.md#adapter-bounded-values). Caller
/// guarantees `head.len() > LEAF_CAP`.
pub(crate) fn truncate_to_marker(head: &[u8], original: usize) -> String {
    let marker = truncation_marker(original);
    let mut end = LEAF_CAP.saturating_sub(marker.len());
    while end > 0 && head[end] & 0xC0 == 0x80 {
        end -= 1;
    }
    let mut capped = String::from_utf8_lossy(&head[..end]).into_owned();
    capped.push_str(&marker);
    record_truncation(original);
    capped
}

pub(crate) fn bound_str(s: &mut String) -> bool {
    if s.len() <= LEAF_CAP {
        return false;
    }
    *s = truncate_to_marker(s.as_bytes(), s.len());
    true
}

pub(crate) fn bound_value(value: &mut Value) {
    match value {
        Value::String(s) => {
            bound_str(s);
        }
        Value::Array(items) => items.iter_mut().for_each(bound_value),
        Value::Object(map) => map.values_mut().for_each(bound_value),
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

pub fn extract_raw_record(row: &Value) -> Value {
    let mut bounded = row.clone();
    bound_value(&mut bounded);
    bounded
}

//
// Round-tripping `Extracted<T>` through serde is required because
// `PartKind::Text { text: Option<Extracted<String>> }` (and friends) are
// serialized to/from JSON for the wire protocol and for Lance's
// `variant_data` column. Encoding is just the inner value; decoding
// rewraps the inner value via `wrap`. From an adapter author's
// perspective serde Deserialize is part of pond, not part of the seam
// they get to call from their adapter code - so this does not loosen the
// non-synthesis guarantee for adapters. (Storage decoding happens in
// `src/sessions.rs::part_from_batch`, not in any adapter.)

impl<T: serde::Serialize> serde::Serialize for Extracted<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<'de, T: serde::Deserialize<'de>> serde::Deserialize<'de> for Extracted<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        T::deserialize(deserializer).map(wrap)
    }
}

/// One row of source data, abstracted across transports. Adapter authors
/// implement this for whatever type carries one event from their source:
///
/// - JSON-flavored adapters (claude-code, codex-cli, ...): pond ships
///   `impl Source for serde_json::Value` below, so the adapter just
///   reuses it.
/// - Struct-flavored adapters (nanoclaw API responses, managed-agent
///   stream frames, database rows): the adapter defines a row struct and
///   `impl Source for MyRow` with whatever lookup makes sense for that
///   format.
///
/// The trait is intentionally minimal - just the four primitives
/// `extract_*` needs - so it ports cleanly to non-JSON sources.
pub trait Source {
    /// String field by key. `None` when the field is absent OR present
    /// but not a string. Adapters that need to distinguish "missing" from
    /// "wrong type" should branch on `value_field` first.
    fn str_field(&self, key: &str) -> Option<&str>;

    /// Boolean field by key. `None` for missing-or-wrong-type, same as
    /// `str_field`.
    fn bool_field(&self, key: &str) -> Option<bool>;

    /// Raw structured value by key (when the field is a nested object,
    /// array, or any non-primitive shape pond stores as `Value`). `None`
    /// for missing.
    fn value_field(&self, key: &str) -> Option<&Value>;

    /// Nested source view by key. Lets `extract_*` walk into sub-objects
    /// without the adapter manually unwrapping intermediate layers.
    /// `None` when the key is absent or not an object.
    fn nested(&self, key: &str) -> Option<&dyn Source>;

    /// The source itself viewed as a primitive string, when it IS one
    /// (e.g. `Value::String("hi")`). `None` for object / array / number
    /// shapes. Used by adapters when the "row" they hold is itself the
    /// string they want to extract (claude-code's
    /// `("user", Value::String(text))` shape).
    fn as_str(&self) -> Option<&str> {
        None
    }

    /// Lossless string encoding of the whole source. Returned by
    /// `extract_compact_repr` when an adapter wants to preserve a row's
    /// bytes via a faithful encoding fallback (e.g. an unknown
    /// `assistant_part` subtype that we still want to keep as text).
    /// This is NOT synthesis (the encoding preserves what was there),
    /// but going through the seam means storage-layer code can rely on
    /// every stored value having flowed through a `Source`. For JSON
    /// sources the default impl is compact `serde_json::to_string`.
    fn compact_repr(&self) -> String;
}

//
// These are the ONLY public producers of `Option<Extracted<T>>`. Anything
// else that needs to put a value into a schema field has to go through
// one of these (directly or via composition), which means it has to come
// from a `Source` - i.e. from real source data.

/// Extract a `String` field. `None` when the source did not carry it.
pub fn extract_str(source: &dyn Source, key: &str) -> Option<Extracted<String>> {
    source.str_field(key).map(|s| {
        let mut owned = s.to_owned();
        bound_str(&mut owned);
        wrap(owned)
    })
}

/// Extract a `bool` field. `None` when the source did not carry it.
pub fn extract_bool(source: &dyn Source, key: &str) -> Option<Extracted<bool>> {
    source.bool_field(key).map(wrap)
}

/// Extract a structured `Value` field. `None` when the source did not
/// carry it. Used for fields like `params` / `result` where pond stores
/// the raw JSON value alongside the canonical schema slots.
pub fn extract_value(source: &dyn Source, key: &str) -> Option<Extracted<Value>> {
    source.value_field(key).cloned().map(|mut value| {
        bound_value(&mut value);
        wrap(value)
    })
}

/// The source itself, viewed as a primitive string. `None` when the
/// source is not a primitive string shape. Composes with patterns where
/// the "row" the adapter holds is the string (e.g. claude-code's
/// `("user", Value::String(text))` content shape).
pub fn extract_self_str(source: &dyn Source) -> Option<Extracted<String>> {
    source.as_str().map(|s| {
        let mut owned = s.to_owned();
        bound_str(&mut owned);
        wrap(owned)
    })
}

/// Lossless compact-string encoding of the whole source. Always succeeds
/// (returns an `Extracted<String>`, not an `Option`) because every
/// `Source` knows how to serialize itself. Used for "preserve the row
/// bytes when we don't have a richer canonical shape" cases - this is
/// NOT synthesis: the encoded string is a faithful representation of
/// data the source actually carried.
pub fn extract_compact_repr(source: &dyn Source) -> Extracted<String> {
    let mut repr = source.compact_repr();
    bound_str(&mut repr);
    wrap(repr)
}

//
// The default implementation for JSON-flavored adapters. Sits at the
// seam, not inside a specific adapter, so claude-code, codex-cli, and
// any future JSON-based adapter share one implementation.

impl Source for Value {
    fn str_field(&self, key: &str) -> Option<&str> {
        self.get(key).and_then(Value::as_str)
    }

    fn bool_field(&self, key: &str) -> Option<bool> {
        self.get(key).and_then(Value::as_bool)
    }

    fn value_field(&self, key: &str) -> Option<&Value> {
        self.get(key)
    }

    fn nested(&self, key: &str) -> Option<&dyn Source> {
        self.get(key).map(|v| v as &dyn Source)
    }

    fn as_str(&self) -> Option<&str> {
        Value::as_str(self)
    }

    fn compact_repr(&self) -> String {
        // `to_string` on `Value` produces compact JSON. Falls back to an
        // empty object encoding only if serialization itself fails, which
        // is impossible for `Value` (it's a closed enum that always
        // serializes), but the unwrap_or_default keeps the trait method
        // total.
        serde_json::to_string(self).unwrap_or_else(|_| "{}".to_owned())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;
    use serde_json::json;

    #[test]
    fn extract_str_pulls_present_field_and_wraps() {
        let row = json!({"name": "Edit", "input": {"file": "/tmp/foo"}});
        let extracted = extract_str(&row, "name").expect("name is present");
        assert_eq!(&*extracted, "Edit", "Deref exposes the inner string");
        assert_eq!(extracted.as_ref(), "Edit", "as_ref does too");
    }

    #[test]
    fn extract_str_returns_none_when_field_absent() {
        let row = json!({"name": "Edit"});
        assert!(extract_str(&row, "missing").is_none());
    }

    #[test]
    fn extract_str_returns_none_when_field_is_not_a_string() {
        let row = json!({"name": 42});
        assert!(
            extract_str(&row, "name").is_none(),
            "wrong-type fields surface as absence; adapters that care should branch on value_field first",
        );
    }

    #[test]
    fn extract_bool_and_extract_value_round_trip() {
        let row = json!({
            "is_error": true,
            "input": {"k": "v"},
        });
        let is_error = extract_bool(&row, "is_error").expect("present");
        assert!(*is_error);
        let params = extract_value(&row, "input").expect("present");
        assert_eq!(&*params, &json!({"k": "v"}));
    }

    #[test]
    fn extracted_serde_round_trip_preserves_value() {
        let extracted = extract_str(&json!({"k": "hello"}), "k").unwrap();
        let encoded = serde_json::to_string(&extracted).unwrap();
        assert_eq!(encoded, "\"hello\"");
        let decoded: Extracted<String> = serde_json::from_str(&encoded).unwrap();
        assert_eq!(&*decoded, "hello");
    }

    #[test]
    fn source_impl_for_value_walks_nested_objects() {
        let row = json!({"message": {"role": "user", "content": "hi"}});
        let nested = row.nested("message").expect("message is an object");
        assert_eq!(nested.str_field("role"), Some("user"));
        assert_eq!(nested.str_field("content"), Some("hi"));
        assert!(row.nested("missing").is_none());
    }

    #[test]
    fn bound_value_caps_every_position_and_spares_good_leaves() {
        let oversize = "x".repeat(LEAF_CAP + 100);
        let mut value = json!({
            "first": oversize,
            "good_a": "ok",
            "middle": oversize,
            "good_b": "ok",
            "nested": {"deep": oversize, "kept": "ok"},
            "list": ["ok", oversize, "ok"],
            "last": oversize,
        });
        bound_value(&mut value);

        let marker = format!("{} bytes>", LEAF_CAP + 100);
        let capped = |v: &Value| {
            let text = v.as_str().expect("string leaf");
            text.len() <= LEAF_CAP && text.ends_with(&marker)
        };
        let intact = |v: &Value| v.as_str() == Some("ok");
        for path in [&value["first"], &value["middle"], &value["last"]] {
            assert!(capped(path));
        }
        assert!(capped(&value["nested"]["deep"]));
        assert!(capped(&value["list"][1]));
        assert!(intact(&value["good_a"]));
        assert!(intact(&value["good_b"]));
        assert!(intact(&value["nested"]["kept"]));
        assert!(intact(&value["list"][0]));
        assert!(intact(&value["list"][2]));
    }
}

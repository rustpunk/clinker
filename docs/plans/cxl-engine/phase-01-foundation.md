# Phase 1: Foundation — Workspace + Data Model

**Status:** 🔲 Not Started
**Depends on:** None
**Entry criteria:** Rust toolchain >= 1.85 (edition 2024 support)
**Exit criteria:** `cargo build --workspace` succeeds, `cargo test -p clinker-record` passes 40+ tests, all 6 crates compile

---

## Tasks

### Task 1.1: Workspace Cargo.toml + Directory Structure
**Status:** 🔲 Not Started
**Blocked by:** Entry criteria

**Description:**
Create the root `Cargo.toml` with workspace configuration, shared dependency versions, and
release profile. Create the full directory tree for all 6 crates per §11.1.

**Implementation notes:**
- Use `resolver = "2"` (default in edition 2024 but be explicit).
- All workspace dependencies from §12.1: `serde`, `serde_json`, `serde-saphyr`, `chrono`, `miette`, `clap`, `regex`, `ahash`, `tracing`.
- Release profile from §12.1: `lto = "fat"`, `codegen-units = 1`, `strip = "symbols"`, `panic = "abort"`.
- Each crate gets its own `Cargo.toml` matching §12.2 exactly (dependency inheritance via `{ workspace = true }`).
- Crate paths: `crates/clinker-record`, `crates/cxl`, `crates/cxl-cli`, `crates/clinker-format`, `crates/clinker-core`, `crates/clinker`.
- Binary crates (`clinker`, `cxl-cli`) need `src/main.rs`; library crates need `src/lib.rs`.

**Acceptance criteria:**
- [ ] `cargo build --workspace` succeeds with zero errors
- [ ] `cargo clippy --workspace -- -D warnings` passes
- [ ] All 6 crates listed in workspace members
- [ ] `cargo test --workspace` passes (even if no tests exist yet)

> **Note:** No `#[test]` hard gate for this task — the gate is `cargo build --workspace` exit code 0 (verified by CI or manual run). Task 1.2 unlocks once the workspace compiles.

#### Sub-tasks
- [ ] **[1.1.0]** Create `rust-toolchain.toml` pinning to channel 1.85 with clippy + rustfmt (~5m)
- [ ] **[1.1.1]** Create root `Cargo.toml` with workspace members, shared deps, and release profile (~1h)
- [ ] **[1.1.2]** Scaffold all 6 crate directories with `Cargo.toml` + minimal `src/lib.rs` or `src/main.rs` (~1h)
- [ ] **[1.1.3]** Verify `cargo build --workspace` and `cargo clippy --workspace -- -D warnings` pass (~30m)

#### Code scaffolding

**`rust-toolchain.toml`** (copy verbatim):
```toml
# File: rust-toolchain.toml
[toolchain]
channel = "1.85"
components = ["clippy", "rustfmt"]
```

**Root Cargo.toml** (copy verbatim):
```toml
# File: Cargo.toml
[workspace]
resolver = "2"
members = [
    "crates/clinker-record",
    "crates/cxl",
    "crates/cxl-cli",
    "crates/clinker-format",
    "crates/clinker-core",
    "crates/clinker",
]

[workspace.package]
version = "0.1.0"
edition = "2024"

[workspace.dependencies]
clinker-record = { path = "crates/clinker-record" }
cxl = { path = "crates/cxl" }
clinker-format = { path = "crates/clinker-format" }
clinker-core = { path = "crates/clinker-core" }

serde = { version = "1", features = ["derive"] }
serde_json = "1"
serde-saphyr = "2"
chrono = { version = "0.4", features = ["serde"] }
miette = { version = "7", features = ["fancy"] }
clap = { version = "4", features = ["derive"] }
regex = "1"
ahash = "0.8"
indexmap = "2"
tracing = "0.1"

[workspace.metadata.release]
shared-version = true

[profile.release]
lto = "fat"
codegen-units = 1
strip = "symbols"
panic = "abort"
```

**clinker-record** (the only crate with code in Phase 1):
```toml
# File: crates/clinker-record/Cargo.toml
[package]
name = "clinker-record"
version.workspace = true
edition.workspace = true
description = "Core data types (Value, Schema, Record) for the Clinker ETL engine"

[dependencies]
serde = { workspace = true }
serde_json = { workspace = true }
chrono = { workspace = true }
ahash = { workspace = true }
indexmap = { workspace = true }
```

**cxl** (library stub):
```toml
# File: crates/cxl/Cargo.toml
[package]
name = "cxl"
version.workspace = true
edition.workspace = true
description = "CXL language parser, type checker, and evaluator"

[dependencies]
clinker-record = { workspace = true }
miette = { workspace = true }
chrono = { workspace = true }
regex = { workspace = true }
ahash = { workspace = true }
```

**cxl-cli** (binary stub):
```toml
# File: crates/cxl-cli/Cargo.toml
[package]
name = "cxl-cli"
version.workspace = true
edition.workspace = true
description = "Standalone CXL language validator and REPL"

[dependencies]
cxl = { workspace = true }
clinker-record = { workspace = true }
clap = { workspace = true }
miette = { workspace = true }
serde_json = { workspace = true }
```

**clinker-format** (library stub):
```toml
# File: crates/clinker-format/Cargo.toml
[package]
name = "clinker-format"
version.workspace = true
edition.workspace = true
description = "Streaming CSV, XML, JSON, and fixed-width readers/writers for Clinker"

[dependencies]
clinker-record = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
csv = "1"
quick-xml = "0.37"
chrono = { workspace = true }
miette = { workspace = true }
```

**clinker-core** (library stub):
```toml
# File: crates/clinker-core/Cargo.toml
[package]
name = "clinker-core"
version.workspace = true
edition.workspace = true
description = "Pipeline orchestration, config parsing, and execution engine for Clinker"

[dependencies]
clinker-record = { workspace = true }
cxl = { workspace = true }
clinker-format = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
serde-saphyr = { workspace = true }
rayon = "1"
crossbeam-channel = "0.5"
chrono = { workspace = true }
regex = { workspace = true }
ahash = { workspace = true }
lz4_flex = "0.11"
tempfile = "3"
tracing = { workspace = true }
miette = { workspace = true }
glob = "0.3"
uuid = { version = "1", features = ["v7"] }
ctrlc = "3"
```

**clinker** (binary stub):
```toml
# File: crates/clinker/Cargo.toml
[package]
name = "clinker"
version.workspace = true
edition.workspace = true
description = "CLI for the Clinker ETL engine"

[dependencies]
clinker-core = { workspace = true }
clap = { workspace = true }
tracing = { workspace = true }
tracing-subscriber = { version = "0.3", features = ["env-filter", "fmt", "ansi"] }
miette = { workspace = true }
```

**Minimal source files** (one per crate):
```rust
// File: crates/clinker-record/src/lib.rs
// Placeholder — modules added in Tasks 1.2–1.5
```
```rust
// File: crates/cxl/src/lib.rs
// Stub
```
```rust
// File: crates/cxl-cli/src/main.rs
fn main() {}
```
```rust
// File: crates/clinker-format/src/lib.rs
// Stub
```
```rust
// File: crates/clinker-core/src/lib.rs
// Stub
```
```rust
// File: crates/clinker/src/main.rs
fn main() {}
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| Workspace root | `Cargo.toml` | §12.1 — all shared deps, release profile |
| `clinker-record` | `crates/clinker-record/Cargo.toml` | §12.2 — serde, serde_json, chrono, ahash |
| `cxl` | `crates/cxl/Cargo.toml` | Library crate, depends on clinker-record |
| `cxl-cli` | `crates/cxl-cli/Cargo.toml` | Binary crate |
| `clinker-format` | `crates/clinker-format/Cargo.toml` | Library crate, depends on clinker-record |
| `clinker-core` | `crates/clinker-core/Cargo.toml` | Library crate, depends on clinker-record |
| `clinker` | `crates/clinker/Cargo.toml` | Binary crate |

#### Intra-phase dependencies
- Requires: Rust toolchain >= 1.85
- Unblocks: Task 1.2, Task 1.3, Task 1.4, Task 1.5 (all)

#### Expanded test stubs
```rust
// No #[test] functions for this task — gate is cargo build success.
// Verified via: cargo build --workspace && cargo clippy --workspace -- -D warnings
```

#### Risk / gotcha callouts
> ⚠️ **Edition 2024 minimum:** `edition = "2024"` requires Rust 1.85+. CI runners must have this version or the workspace will not compile at all.

> ⚠️ **Workspace version field:** All crates declare `version.workspace = true` and `edition.workspace = true`. This requires the `[workspace.package]` table in the root `Cargo.toml` (included in the scaffolding above). If omitted, cargo will error with "workspace package metadata not found".

---

### Task 1.2: Value Enum + Type Coercion
**Status:** ⛔ Blocked (waiting on Task 1.1)
**Blocked by:** Task 1.1 — workspace must compile

**Description:**
Implement the `Value` enum in `clinker-record::value` with all 8 variants per §3. Implement
`Display`, `Serialize`/`Deserialize` (serde untagged), `Clone`, custom `PartialEq` (using
`f64::to_bits()`), and `PartialOrd`. Implement type coercion functions in
`clinker-record::coercion`.

**Implementation notes:**
- `Value::Float` uses `f64`. Custom `PartialEq`: compare floats via `f64::to_bits()` so NaN == NaN is true and -0.0 != 0.0. Do NOT derive `PartialEq`.
- For `PartialOrd`, use `f64::total_cmp()` to get deterministic NaN ordering. NaN sorts after all finite values.
- `Box<str>` for strings, not `Arc<str>` (spec §3 design note). 8 bytes saved per string value.
- `#[serde(untagged)]` on the enum. Variant order: Null, Bool, Integer, Float, String, Date, DateTime, Array. Integer before Float so `42` deserializes as Integer (not Float).
- Date/DateTime `Display`: ISO 8601 — `%Y-%m-%d` for Date, `%Y-%m-%dT%H:%M:%S` for DateTime.
- `CoercionError` is a simple enum in clinker-record (no miette dependency):
  ```rust
  #[derive(Debug, Clone)]
  pub enum CoercionError {
      TypeMismatch { from: &'static str, to: &'static str, value: String },
      ParseFailure { input: String, target: &'static str },
  }
  ```
- Coercion module (`coercion.rs`): implement `coerce_to_int()`, `coerce_to_float()`, `coerce_to_string()`, `coerce_to_bool()`, `coerce_to_date()`, `coerce_to_datetime()`. Each has strict (returns `Result<Value, CoercionError>`) and lenient (returns `Option<Value>`) variants.
- Null propagation rule: coercing `Value::Null` always returns `Value::Null` (not an error).
- String→Bool: `"true"/"yes"/"1"/"on"` → true; `"false"/"no"/"0"/"off"` → false; anything else → error.
- String→Numeric: use `str::parse::<i64>()` then `str::parse::<f64>()`. Parse failure → null (lenient) or error (strict).

**Acceptance criteria:**
- [ ] All 8 Value variants constructible and cloneable
- [ ] `Display` produces human-readable output for all variants (ISO 8601 for dates)
- [ ] Serde round-trip: `Value` → JSON string → `Value` is lossless for all variants
- [ ] Custom `PartialEq` via `f64::to_bits()`: NaN == NaN, -0.0 != 0.0
- [ ] `PartialOrd` is reflexive, transitive, and handles NaN deterministically
- [ ] All coercion paths tested (String→Int, String→Float, String→Date, String→DateTime, Int→Float, etc.)
- [ ] Null propagation: coercing Null always returns Null

**Required unit tests (must pass before Task 1.3 unlocks):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_value_display_all_variants` | Display impl for each of the 8 variants | ⛔ Hard gate |
| `test_value_serde_roundtrip` | JSON serialize then deserialize produces equal Value | ⛔ Hard gate |
| `test_value_partial_ord_same_variant` | Ordering within each variant (Int < Int, String < String, etc.) | ⛔ Hard gate |
| `test_value_partial_ord_cross_variant` | Cross-variant comparison returns None | ⛔ Hard gate |
| `test_value_float_nan_ordering` | NaN sorts deterministically (after all finite values) | ⛔ Hard gate |
| `test_coerce_string_to_int_valid` | `"42"` → `Value::Integer(42)` | ⛔ Hard gate |
| `test_coerce_string_to_int_invalid` | `"abc"` → strict error, lenient None | ⛔ Hard gate |
| `test_coerce_null_propagation` | Coercing Null always returns Null | ⛔ Hard gate |
| `test_coerce_string_to_bool` | `"true"/"yes"/"1"` → true, `"false"/"no"/"0"` → false | ⛔ Hard gate |
| `test_coerce_string_to_date` | `"2024-01-15"` → `Value::Date(NaiveDate)` | ⛔ Hard gate |

> ⛔ **Hard gate:** Task 1.3 status remains `Blocked` until all tests above pass.

#### Sub-tasks
- [ ] **[1.2.1]** Define `Value` enum with all 8 variants, `Clone`, `Debug`, `Serialize` (~1h)
- [ ] **[1.2.2]** Implement custom `PartialEq` using `f64::to_bits()` for Float comparison (~1h)
- [ ] **[1.2.3]** Implement `PartialOrd` using `f64::total_cmp()` for Float ordering (~1h)
- [ ] **[1.2.4]** Implement `Display` for all 8 variants (ISO 8601 for Date/DateTime) (~1h)
- [ ] **[1.2.5]** Implement `Value::type_name()` helper returning `&'static str` (~30m)
- [ ] **[1.2.6]** Implement custom `Deserialize` that tries Date/DateTime before String (~2h)
- [ ] **[1.2.7]** Define `CoercionError` enum and implement all 12 coercion functions (6 strict + 6 lenient) (~3h)
- [ ] **[1.2.8]** Write all 10 hard-gate tests + edge-case tests (Float→Int truncation, empty string, overflow) (~2h)

#### Code scaffolding
```rust
// Module: crates/clinker-record/src/value.rs

use chrono::{NaiveDate, NaiveDateTime};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;

#[derive(Debug, Clone, Serialize)]
// Deserialize: manual impl below (tries DateTime→Date→String for JSON strings)
// PartialEq: manual impl below (f64::to_bits() for NaN==NaN)
pub enum Value {
    Null,
    Bool(bool),
    Integer(i64),
    Float(f64),
    String(Box<str>),
    Date(NaiveDate),
    DateTime(NaiveDateTime),
    Array(Vec<Value>),
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Integer(a), Value::Integer(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => a.to_bits() == b.to_bits(),
            (Value::String(a), Value::String(b)) => a == b,
            (Value::Date(a), Value::Date(b)) => a == b,
            (Value::DateTime(a), Value::DateTime(b)) => a == b,
            (Value::Array(a), Value::Array(b)) => a == b,
            _ => false,
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Value::Null, Value::Null) => Some(Ordering::Equal),
            (Value::Bool(a), Value::Bool(b)) => a.partial_cmp(b),
            (Value::Integer(a), Value::Integer(b)) => a.partial_cmp(b),
            (Value::Float(a), Value::Float(b)) => Some(a.total_cmp(b)),
            (Value::String(a), Value::String(b)) => a.partial_cmp(b),
            (Value::Date(a), Value::Date(b)) => a.partial_cmp(b),
            (Value::DateTime(a), Value::DateTime(b)) => a.partial_cmp(b),
            _ => None, // cross-variant comparison is undefined
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "null"),
            Value::Bool(b) => write!(f, "{b}"),
            Value::Integer(n) => write!(f, "{n}"),
            Value::Float(n) => write!(f, "{n}"),
            Value::String(s) => write!(f, "{s}"),
            Value::Date(d) => write!(f, "{}", d.format("%Y-%m-%d")),
            Value::DateTime(dt) => write!(f, "{}", dt.format("%Y-%m-%dT%H:%M:%S")),
            Value::Array(arr) => {
                write!(f, "[")?;
                for (i, v) in arr.iter().enumerate() {
                    if i > 0 { write!(f, ", ")?; }
                    write!(f, "{v}")?;
                }
                write!(f, "]")
            }
        }
    }
}
```

```rust
// Module: crates/clinker-record/src/value.rs (continued — type_name helper)

impl Value {
    /// Returns the CXL type name as a static string.
    /// Used by CoercionError messages and the CXL `.type_name()` built-in.
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Null => "null",
            Value::Bool(_) => "bool",
            Value::Integer(_) => "int",
            Value::Float(_) => "float",
            Value::String(_) => "string",
            Value::Date(_) => "date",
            Value::DateTime(_) => "datetime",
            Value::Array(_) => "array",
        }
    }

    /// Returns true if the value is Null.
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}
```

```rust
// Module: crates/clinker-record/src/value.rs (continued — custom Deserialize)
//
// The custom Deserialize impl is needed because #[serde(untagged)] with chrono's
// NaiveDate would serialize Date as a JSON string "2024-01-15", which then
// deserializes back as Value::String (not Value::Date).
//
// Strategy: implement Deserialize manually. For JSON strings, try parsing as:
//   1. NaiveDateTime via pipeline.date_formats (datetime formats first)
//   2. NaiveDate via pipeline.date_formats (date formats)
//   3. Auto-detect heuristic (ISO 8601, then slash-delimited with date_locale hint)
//   4. Fall back to Value::String
//
// NOTE: The Deserialize impl below uses a FIXED fallback chain (ISO 8601 formats only).
// The full three-layer resolution (field format → pipeline chain → heuristic) is
// implemented in clinker-format's reader layer, NOT in serde Deserialize. The serde
// impl is used for internal serialization (spill files) where ISO 8601 is guaranteed.
// For JSON/CSV/XML input parsing, the format reader applies the full chain BEFORE
// constructing Value, so strings arrive as Value::Date already.
// For JSON numbers, try i64 first (Integer), then f64 (Float).
// For JSON bools, arrays, nulls: straightforward.

use serde::de::{self, Visitor, SeqAccess};

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct ValueVisitor;

        impl<'de> Visitor<'de> for ValueVisitor {
            type Value = Value;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "a CXL value")
            }

            fn visit_bool<E: de::Error>(self, v: bool) -> Result<Value, E> {
                Ok(Value::Bool(v))
            }

            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Value, E> {
                Ok(Value::Integer(v))
            }

            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Value, E> {
                // u64 values that fit in i64 → Integer; otherwise → Float
                if v <= i64::MAX as u64 {
                    Ok(Value::Integer(v as i64))
                } else {
                    Ok(Value::Float(v as f64))
                }
            }

            fn visit_f64<E: de::Error>(self, v: f64) -> Result<Value, E> {
                Ok(Value::Float(v))
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Value, E> {
                // Try DateTime first (more specific), then Date, then String
                if let Ok(dt) = NaiveDateTime::parse_from_str(v, "%Y-%m-%dT%H:%M:%S") {
                    return Ok(Value::DateTime(dt));
                }
                if let Ok(dt) = NaiveDateTime::parse_from_str(v, "%Y-%m-%d %H:%M:%S") {
                    return Ok(Value::DateTime(dt));
                }
                if let Ok(d) = NaiveDate::parse_from_str(v, "%Y-%m-%d") {
                    return Ok(Value::Date(d));
                }
                Ok(Value::String(v.into()))
            }

            fn visit_none<E: de::Error>(self) -> Result<Value, E> {
                Ok(Value::Null)
            }

            fn visit_unit<E: de::Error>(self) -> Result<Value, E> {
                Ok(Value::Null)
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Value, A::Error> {
                let mut arr = Vec::new();
                while let Some(v) = seq.next_element()? {
                    arr.push(v);
                }
                Ok(Value::Array(arr))
            }
        }

        deserializer.deserialize_any(ValueVisitor)
    }
}
```

```rust
// Module: crates/clinker-record/src/coercion.rs

use crate::value::Value;
use chrono::{NaiveDate, NaiveDateTime};
use std::fmt;

#[derive(Debug, Clone)]
pub enum CoercionError {
    TypeMismatch { from: &'static str, to: &'static str, value: String },
    ParseFailure { input: String, target: &'static str },
}

impl fmt::Display for CoercionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CoercionError::TypeMismatch { from, to, value } => {
                write!(f, "cannot coerce {from} to {to}: {value}")
            }
            CoercionError::ParseFailure { input, target } => {
                write!(f, "failed to parse '{input}' as {target}")
            }
        }
    }
}

impl std::error::Error for CoercionError {}

/// Strict coercion: returns Err on failure. Null propagates as Ok(Null).
pub fn coerce_to_int(value: &Value) -> Result<Value, CoercionError> {
    match value {
        Value::Null => Ok(Value::Null),
        Value::Integer(_) => Ok(value.clone()),
        Value::Float(f) => Ok(Value::Integer(*f as i64)),
        Value::Bool(b) => Ok(Value::Integer(if *b { 1 } else { 0 })),
        Value::String(s) => s.parse::<i64>()
            .map(Value::Integer)
            .map_err(|_| CoercionError::ParseFailure {
                input: s.to_string(),
                target: "Integer",
            }),
        other => Err(CoercionError::TypeMismatch {
            from: other.type_name(),
            to: "Integer",
            value: other.to_string(),
        }),
    }
}

/// Lenient coercion: returns None on failure. Null propagates as Some(Null).
pub fn coerce_to_int_lenient(value: &Value) -> Option<Value> {
    coerce_to_int(value).ok()
}

pub fn coerce_to_float(value: &Value) -> Result<Value, CoercionError> {
    match value {
        Value::Null => Ok(Value::Null),
        Value::Float(_) => Ok(value.clone()),
        Value::Integer(n) => Ok(Value::Float(*n as f64)),
        Value::Bool(b) => Ok(Value::Float(if *b { 1.0 } else { 0.0 })),
        Value::String(s) => s.parse::<f64>()
            .map(Value::Float)
            .map_err(|_| CoercionError::ParseFailure {
                input: s.to_string(),
                target: "Float",
            }),
        other => Err(CoercionError::TypeMismatch {
            from: other.type_name(),
            to: "Float",
            value: other.to_string(),
        }),
    }
}

pub fn coerce_to_float_lenient(value: &Value) -> Option<Value> {
    coerce_to_float(value).ok()
}

pub fn coerce_to_string(value: &Value) -> Result<Value, CoercionError> {
    match value {
        Value::Null => Ok(Value::Null),
        other => Ok(Value::String(other.to_string().into_boxed_str())),
    }
}

pub fn coerce_to_string_lenient(value: &Value) -> Option<Value> {
    coerce_to_string(value).ok()
}

pub fn coerce_to_bool(value: &Value) -> Result<Value, CoercionError> {
    match value {
        Value::Null => Ok(Value::Null),
        Value::Bool(_) => Ok(value.clone()),
        Value::Integer(n) => Ok(Value::Bool(*n != 0)),
        Value::String(s) => match s.to_ascii_lowercase().as_str() {
            "true" | "yes" | "1" | "on" => Ok(Value::Bool(true)),
            "false" | "no" | "0" | "off" => Ok(Value::Bool(false)),
            _ => Err(CoercionError::ParseFailure {
                input: s.to_string(),
                target: "Bool",
            }),
        },
        other => Err(CoercionError::TypeMismatch {
            from: other.type_name(),
            to: "Bool",
            value: other.to_string(),
        }),
    }
}

pub fn coerce_to_bool_lenient(value: &Value) -> Option<Value> {
    coerce_to_bool(value).ok()
}

/// Default date format chain — US-oriented. EU formats require explicit pipeline.date_formats config.
pub const DEFAULT_DATE_FORMATS: &[&str] = &[
    "%Y-%m-%d",     // ISO 8601: 2024-01-15
    "%m/%d/%Y",     // US slash: 01/15/2024
    "%Y/%m/%d",     // Alt ISO: 2024/01/15
    "%m-%d-%Y",     // US dash: 01-15-2024
];

/// Default datetime format chain — US-oriented.
pub const DEFAULT_DATETIME_FORMATS: &[&str] = &[
    "%Y-%m-%dT%H:%M:%S",  // ISO 8601: 2024-01-15T10:30:00
    "%Y-%m-%d %H:%M:%S",  // Space sep: 2024-01-15 10:30:00
    "%m/%d/%Y %H:%M:%S",  // US: 01/15/2024 10:30:00
    "%m/%d/%Y %H:%M",     // US no sec: 01/15/2024 10:30
];

/// Strict date coercion with configurable format chain.
/// Pass &[] to use DEFAULT_DATE_FORMATS. Pass &["%m/%d/%Y"] for a single explicit format.
pub fn coerce_to_date(value: &Value, formats: &[&str]) -> Result<Value, CoercionError> {
    match value {
        Value::Null => Ok(Value::Null),
        Value::Date(_) => Ok(value.clone()),
        Value::DateTime(dt) => Ok(Value::Date(dt.date())),
        Value::String(s) => {
            let chain = if formats.is_empty() { DEFAULT_DATE_FORMATS } else { formats };
            for fmt in chain {
                if let Ok(d) = NaiveDate::parse_from_str(s, fmt) {
                    return Ok(Value::Date(d));
                }
            }
            Err(CoercionError::ParseFailure {
                input: s.to_string(),
                target: "Date",
            })
        }
        other => Err(CoercionError::TypeMismatch {
            from: other.type_name(),
            to: "Date",
            value: other.to_string(),
        }),
    }
}

pub fn coerce_to_date_lenient(value: &Value, formats: &[&str]) -> Option<Value> {
    coerce_to_date(value, formats).ok()
}

/// Strict datetime coercion with configurable format chain.
/// Date values promoted to DateTime at midnight.
pub fn coerce_to_datetime(value: &Value, formats: &[&str]) -> Result<Value, CoercionError> {
    match value {
        Value::Null => Ok(Value::Null),
        Value::DateTime(_) => Ok(value.clone()),
        Value::Date(d) => Ok(Value::DateTime(d.and_hms_opt(0, 0, 0).unwrap())),
        Value::String(s) => {
            let chain = if formats.is_empty() { DEFAULT_DATETIME_FORMATS } else { formats };
            for fmt in chain {
                if let Ok(dt) = NaiveDateTime::parse_from_str(s, fmt) {
                    return Ok(Value::DateTime(dt));
                }
            }
            Err(CoercionError::ParseFailure {
                input: s.to_string(),
                target: "DateTime",
            })
        }
        other => Err(CoercionError::TypeMismatch {
            from: other.type_name(),
            to: "DateTime",
            value: other.to_string(),
        }),
    }
}

pub fn coerce_to_datetime_lenient(value: &Value, formats: &[&str]) -> Option<Value> {
    coerce_to_datetime(value, formats).ok()
}
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| `Value` | `crates/clinker-record/src/value.rs` | 8-variant enum, custom PartialEq/PartialOrd |
| `CoercionError` | `crates/clinker-record/src/coercion.rs` | Simple enum, no miette dep |
| `coerce_to_*` | `crates/clinker-record/src/coercion.rs` | 6 strict + 6 lenient functions |

#### Intra-phase dependencies
- Requires: Task 1.1 (workspace compiles)
- Unblocks: Task 1.3 (Schema/Record needs Value)

#### Expanded test stubs
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    #[test]
    fn test_value_display_all_variants() {
        assert_eq!(Value::Null.to_string(), "null");
        assert_eq!(Value::Bool(true).to_string(), "true");
        assert_eq!(Value::Integer(42).to_string(), "42");
        assert_eq!(Value::Float(3.14).to_string(), "3.14");
        assert_eq!(Value::String("hello".into()).to_string(), "hello");
        let d = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        assert_eq!(Value::Date(d).to_string(), "2024-01-15");
        let dt = d.and_hms_opt(10, 30, 0).unwrap();
        assert_eq!(Value::DateTime(dt).to_string(), "2024-01-15T10:30:00");
        let arr = Value::Array(vec![Value::Integer(1), Value::Integer(2)]);
        assert_eq!(arr.to_string(), "[1, 2]");
    }

    #[test]
    fn test_value_serde_roundtrip() {
        let cases = vec![
            Value::Null,
            Value::Bool(false),
            Value::Integer(42),
            Value::Float(3.14),
            Value::String("test".into()),
            Value::Array(vec![Value::Integer(1), Value::Bool(true)]),
        ];
        for original in cases {
            let json = serde_json::to_string(&original).unwrap();
            let recovered: Value = serde_json::from_str(&json).unwrap();
            assert_eq!(original, recovered);
        }
    }

    #[test]
    fn test_value_partial_ord_same_variant() {
        assert!(Value::Integer(1) < Value::Integer(2));
        assert!(Value::Float(1.0) < Value::Float(2.0));
        assert!(Value::String("a".into()) < Value::String("b".into()));
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let d2 = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
        assert!(Value::Date(d1) < Value::Date(d2));
    }

    #[test]
    fn test_value_partial_ord_cross_variant() {
        assert_eq!(Value::Integer(1).partial_cmp(&Value::String("1".into())), None);
        assert_eq!(Value::Bool(true).partial_cmp(&Value::Integer(1)), None);
        assert_eq!(Value::Null.partial_cmp(&Value::Integer(0)), None);
    }

    #[test]
    fn test_value_float_nan_ordering() {
        let nan = Value::Float(f64::NAN);
        let inf = Value::Float(f64::INFINITY);
        let normal = Value::Float(1.0);
        // NaN sorts after all finite values with total_cmp
        assert!(nan > normal);
        assert!(nan > inf);
    }

    #[test]
    fn test_value_float_partial_eq_nan() {
        // Custom PartialEq: NaN == NaN (via to_bits)
        let nan1 = Value::Float(f64::NAN);
        let nan2 = Value::Float(f64::NAN);
        assert_eq!(nan1, nan2);

        // Custom PartialEq: -0.0 != 0.0 (different bits)
        let neg_zero = Value::Float(-0.0_f64);
        let pos_zero = Value::Float(0.0_f64);
        assert_ne!(neg_zero, pos_zero);
    }
}
```

```rust
#[cfg(test)]
mod coercion_tests {
    use super::*;
    use crate::value::Value;
    use chrono::NaiveDate;

    #[test]
    fn test_coerce_string_to_int_valid() {
        let v = Value::String("42".into());
        let result = coerce_to_int(&v).unwrap();
        assert_eq!(result, Value::Integer(42));
    }

    #[test]
    fn test_coerce_string_to_int_invalid() {
        let v = Value::String("abc".into());
        assert!(coerce_to_int(&v).is_err());
        assert!(coerce_to_int_lenient(&v).is_none());
    }

    #[test]
    fn test_coerce_null_propagation() {
        assert_eq!(coerce_to_int(&Value::Null).unwrap(), Value::Null);
        assert_eq!(coerce_to_float(&Value::Null).unwrap(), Value::Null);
        assert_eq!(coerce_to_string(&Value::Null).unwrap(), Value::Null);
        assert_eq!(coerce_to_bool(&Value::Null).unwrap(), Value::Null);
        assert_eq!(coerce_to_date(&Value::Null, &[]).unwrap(), Value::Null);
        assert_eq!(coerce_to_datetime(&Value::Null, &[]).unwrap(), Value::Null);
    }

    #[test]
    fn test_coerce_string_to_bool() {
        for s in ["true", "yes", "1", "on"] {
            let v = Value::String(s.into());
            assert_eq!(coerce_to_bool(&v).unwrap(), Value::Bool(true));
        }
        for s in ["false", "no", "0", "off"] {
            let v = Value::String(s.into());
            assert_eq!(coerce_to_bool(&v).unwrap(), Value::Bool(false));
        }
        let v = Value::String("maybe".into());
        assert!(coerce_to_bool(&v).is_err());
    }

    #[test]
    fn test_coerce_string_to_date() {
        // ISO format — matches first in default chain
        let v = Value::String("2024-01-15".into());
        let expected = Value::Date(NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());
        assert_eq!(coerce_to_date(&v, &[]).unwrap(), expected);

        // US format — matches second in default chain
        let v = Value::String("01/15/2024".into());
        assert_eq!(coerce_to_date(&v, &[]).unwrap(), expected);

        // Explicit format — bypasses chain
        let v = Value::String("15-01-2024".into());
        assert_eq!(coerce_to_date(&v, &["%d-%m-%Y"]).unwrap(), expected);
    }

    #[test]
    fn test_coerce_string_to_datetime() {
        let v = Value::String("2024-01-15T10:30:00".into());
        let d = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let expected = Value::DateTime(d.and_hms_opt(10, 30, 0).unwrap());
        assert_eq!(coerce_to_datetime(&v, &[]).unwrap(), expected);

        // US datetime format
        let v = Value::String("01/15/2024 10:30:00".into());
        assert_eq!(coerce_to_datetime(&v, &[]).unwrap(), expected);
    }

    // --- Edge-case tests (beyond the 10 hard-gate minimum) ---

    #[test]
    fn test_coerce_float_to_int_truncation() {
        // Float→Int uses truncation toward zero (Rust `as i64`), not rounding
        assert_eq!(coerce_to_int(&Value::Float(1.9)).unwrap(), Value::Integer(1));
        assert_eq!(coerce_to_int(&Value::Float(-1.9)).unwrap(), Value::Integer(-1));
        assert_eq!(coerce_to_int(&Value::Float(0.5)).unwrap(), Value::Integer(0));
    }

    #[test]
    fn test_coerce_empty_string() {
        // Empty string → Int: parse failure
        assert!(coerce_to_int(&Value::String("".into())).is_err());
        assert_eq!(coerce_to_int_lenient(&Value::String("".into())), None);
        // Empty string → Bool: parse failure (not false)
        assert!(coerce_to_bool(&Value::String("".into())).is_err());
        // Empty string → Date: parse failure
        assert!(coerce_to_date(&Value::String("".into()), &[]).is_err());
    }

    #[test]
    fn test_coerce_string_to_int_overflow() {
        // String exceeding i64 range → parse failure
        let v = Value::String("99999999999999999999".into());
        assert!(coerce_to_int(&v).is_err());
        assert!(coerce_to_int_lenient(&v).is_none());
    }

    #[test]
    fn test_coerce_string_to_int_float_string() {
        // "1.5" → Int: this is a string, not a float. str::parse::<i64>("1.5") fails.
        // This is strict intentional — user should use .to_float().to_int() if they want truncation.
        assert!(coerce_to_int(&Value::String("1.5".into())).is_err());
    }

    #[test]
    fn test_value_serde_roundtrip_with_dates() {
        // Custom Deserialize: "2024-01-15" → Value::Date, not Value::String
        let date = Value::Date(NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());
        let json = serde_json::to_string(&date).unwrap();
        assert_eq!(json, r#""2024-01-15""#);
        let recovered: Value = serde_json::from_str(&json).unwrap();
        assert_eq!(recovered, date); // Date, not String

        // DateTime round-trip
        let d = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let dt = Value::DateTime(d.and_hms_opt(10, 30, 0).unwrap());
        let json = serde_json::to_string(&dt).unwrap();
        let recovered: Value = serde_json::from_str(&json).unwrap();
        assert_eq!(recovered, dt); // DateTime, not String
    }

    #[test]
    fn test_value_serde_plain_string_not_date() {
        // "hello" must NOT be parsed as a Date
        let json = r#""hello""#;
        let v: Value = serde_json::from_str(json).unwrap();
        assert!(matches!(v, Value::String(_)));
    }

    #[test]
    fn test_value_serde_integer_not_float() {
        // JSON number 42 (no decimal) → Value::Integer, not Value::Float
        let v: Value = serde_json::from_str("42").unwrap();
        assert!(matches!(v, Value::Integer(42)));
        // JSON number 42.0 (with decimal) → Value::Float
        let v: Value = serde_json::from_str("42.0").unwrap();
        assert!(matches!(v, Value::Float(_)));
    }

    #[test]
    fn test_value_type_name() {
        assert_eq!(Value::Null.type_name(), "null");
        assert_eq!(Value::Bool(true).type_name(), "bool");
        assert_eq!(Value::Integer(1).type_name(), "int");
        assert_eq!(Value::Float(1.0).type_name(), "float");
        assert_eq!(Value::String("x".into()).type_name(), "string");
    }
}
```

#### Risk / gotcha callouts
> ⚠️ **Custom Deserialize + Serialize mismatch:** The `Serialize` derive uses `#[serde(untagged)]`
> which serializes Date as `"2024-01-15"` (a JSON string). The custom `Deserialize` impl
> tries to parse date-like strings back as Date. This works for ISO 8601 formats but will
> false-positive on strings that happen to look like dates (e.g., a field containing
> `"2024-01-15"` as a text value). In practice this is acceptable — CXL users who want a
> string that looks like a date can use `schema_overrides` to pin the field type to String.

> ⚠️ **Float→Int truncation edge cases:** `f64::MAX as i64` is undefined behavior in Rust
> (saturating cast since Rust 1.45, but the result is `i64::MAX`). Very large floats
> coerced to Int will silently saturate. Document this in the spec as a known limitation
> for values outside `±2^53`.

> ⚠️ **PartialEq vs Hash consistency:** Custom `PartialEq` means if `Value` ever implements `Hash`, it must use `f64::to_bits()` too. Plan for this now even if Hash is not yet needed.

---

### Task 1.3: Schema + Record
**Status:** ⛔ Blocked (waiting on Task 1.2)
**Blocked by:** Task 1.2 — Value and coercion must be working

**Description:**
Implement `Schema` in `clinker-record::schema` and `Record` in `clinker-record::record` per §3.
Schema provides O(1) field name→index lookup via ahash. Record is schema-indexed with an
overflow HashMap for CXL-emitted fields.

**Implementation notes:**
- `Schema` constructor: `Schema::new(columns: Vec<Box<str>>)` builds the index HashMap.
- `ahash::RandomState` is used for fast hashing but is **NOT relied on for ordering**. Output field ordering comes exclusively from `Schema.columns` (the `Vec<Box<str>>`), never from HashMap iteration. `with_seeds(1, 2, 3, 4)` is used for consistency within a single build but cross-platform determinism is NOT guaranteed by ahash (confirmed: different hashes on x86_64 vs ARM64, different hashes across ahash versions).
- Schema exposes: `columns(&self) -> &[Box<str>]`, `column_count(&self) -> usize`, `index(&self, name: &str) -> Option<usize>`, `column_name(&self, idx: usize) -> Option<&str>`.
- `Record::new`: uses `debug_assert_eq!` on length mismatch. In release: pad with Null if short, truncate if long.
- `Record::get(&self, name: &str) -> Option<&Value>` — schema lookup then `values[idx]`.
- `Record::set(&mut self, name: &str, value: Value)` — schema lookup then `values[idx] = value`.
- `Record::set_overflow(&mut self, name: Box<str>, value: Value)` — lazily initializes the `Option<Box<HashMap>>`.
- Overflow field names must never collide with schema field names — `set_overflow` panics (debug) or silently redirects to `set` if the name is in the schema.
- `Record` must be `Send + Sync`. Verify with `static_assertions::assert_impl_all!(Record: Send, Sync)` or a compile-time test.

**Acceptance criteria:**
- [ ] `Schema::new` + `Schema::index` provides O(1) lookup
- [ ] Schema exposes `columns()`, `column_count()`, `index()`, `column_name()`
- [ ] `Record::new` debug-asserts on length mismatch; pads/truncates in release
- [ ] `Record::get` returns correct value by name
- [ ] `Record::set` updates schema-indexed fields in-place
- [ ] `Record::set_overflow` creates overflow HashMap on first use
- [ ] Overflow field with same name as schema field redirects to `set`
- [ ] `Record` is `Send + Sync`

**Required unit tests (must pass before Task 1.4 unlocks):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_schema_index_lookup` | Name→index O(1) for 5 columns | ⛔ Hard gate |
| `test_schema_unknown_field_returns_none` | Missing field name → None | ⛔ Hard gate |
| `test_record_get_set_by_name` | Round-trip: set then get returns same Value | ⛔ Hard gate |
| `test_record_overflow_lazy_init` | Overflow is None until first overflow write | ⛔ Hard gate |
| `test_record_overflow_no_shadow` | Schema field name in overflow redirects to schema slot | ⛔ Hard gate |
| `test_record_send_sync` | Compile-time assertion that Record is Send + Sync | ⛔ Hard gate |

> ⛔ **Hard gate:** Task 1.4 status remains `Blocked` until all tests above pass.

#### Sub-tasks
- [ ] **[1.3.1]** Implement `Schema` struct with `new()`, `columns()`, `column_count()`, `index()`, `column_name()`, `contains()` (~2h)
- [ ] **[1.3.2]** Implement `Record` struct with `new()`, `get()`, `set()`, `set_overflow()`, `schema()`, length mismatch handling (~2h)
- [ ] **[1.3.3]** Implement `Record::values()`, `Record::overflow_fields()`, `Record::iter_all_fields()` for output projection (~1h)
- [ ] **[1.3.4]** Write all 6 hard-gate tests + edge-case tests (length mismatch, iteration order, empty overflow) (~1.5h)

#### Code scaffolding
```rust
// Module: crates/clinker-record/src/schema.rs

use ahash::RandomState;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Schema {
    columns: Vec<Box<str>>,
    index: HashMap<Box<str>, usize, RandomState>,
}

impl Schema {
    pub fn new(columns: Vec<Box<str>>) -> Self {
        let hasher = RandomState::with_seeds(1, 2, 3, 4);
        let mut index = HashMap::with_capacity_and_hasher(columns.len(), hasher);
        for (i, name) in columns.iter().enumerate() {
            index.insert(name.clone(), i);
        }
        Self { columns, index }
    }

    /// All column names in insertion order (determines output field ordering).
    pub fn columns(&self) -> &[Box<str>] {
        &self.columns
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// O(1) name → positional index lookup. Returns None if field not in schema.
    pub fn index(&self, name: &str) -> Option<usize> {
        self.index.get(name).copied()
    }

    /// Positional index → name. Returns None if index out of bounds.
    pub fn column_name(&self, idx: usize) -> Option<&str> {
        self.columns.get(idx).map(|s| &**s)
    }

    /// Returns true if the schema contains a field with this name.
    pub fn contains(&self, name: &str) -> bool {
        self.index.contains_key(name)
    }
}
```

```rust
// Module: crates/clinker-record/src/record.rs

use crate::schema::Schema;
use crate::value::Value;
use indexmap::IndexMap;
use std::sync::Arc;

/// Schema-indexed record with optional overflow for CXL-emitted unknown fields.
/// Overflow uses IndexMap to preserve emit statement order in output.
#[derive(Debug, Clone)]
pub struct Record {
    schema: Arc<Schema>,
    values: Vec<Value>,
    overflow: Option<Box<IndexMap<Box<str>, Value>>>,
}

impl Record {
    pub fn new(schema: Arc<Schema>, mut values: Vec<Value>) -> Self {
        debug_assert_eq!(
            schema.column_count(),
            values.len(),
            "Record::new: schema has {} columns but got {} values",
            schema.column_count(),
            values.len(),
        );
        // Release-mode safety: pad with Null or truncate
        let expected = schema.column_count();
        if values.len() < expected {
            values.resize(expected, Value::Null);
        } else if values.len() > expected {
            values.truncate(expected);
        }
        Self { schema, values, overflow: None }
    }

    pub fn get(&self, name: &str) -> Option<&Value> {
        if let Some(idx) = self.schema.index(name) {
            return self.values.get(idx);
        }
        self.overflow.as_ref().and_then(|m| m.get(name))
    }

    pub fn set(&mut self, name: &str, value: Value) -> bool {
        if let Some(idx) = self.schema.index(name) {
            self.values[idx] = value;
            return true;
        }
        false
    }

    pub fn set_overflow(&mut self, name: Box<str>, value: Value) {
        // If name exists in schema, redirect to schema slot
        if let Some(idx) = self.schema.index(&name) {
            debug_assert!(
                false,
                "set_overflow called with schema field '{name}'; redirecting to schema slot"
            );
            self.values[idx] = value;
            return;
        }
        let map = self.overflow.get_or_insert_with(|| {
            Box::new(IndexMap::new())
        });
        map.insert(name, value);
    }

    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    /// Raw access to the schema-indexed values slice (positional).
    /// Used by Arena construction (Phase 5) to extract indexed fields.
    pub fn values(&self) -> &[Value] {
        &self.values
    }

    /// Iterator over overflow fields only. Returns None if no overflow exists.
    pub fn overflow_fields(&self) -> Option<impl Iterator<Item = (&str, &Value)>> {
        self.overflow.as_ref().map(|m| m.iter().map(|(k, v)| (k.as_ref(), v)))
    }

    /// Iterator over ALL fields: schema fields first (in schema order), then overflow.
    /// Used by Phase 3 projection for output serialization.
    /// Schema fields come first so that output column order matches schema order.
    /// Overflow fields (CXL-emitted unknown fields) append after schema fields.
    pub fn iter_all_fields(&self) -> impl Iterator<Item = (&str, &Value)> {
        let schema_fields = self.schema.columns().iter().enumerate().map(|(i, name)| {
            (name.as_ref(), &self.values[i])
        });
        let overflow_fields = self.overflow.as_ref()
            .into_iter()
            .flat_map(|m| m.iter().map(|(k, v)| (k.as_ref(), v)));
        schema_fields.chain(overflow_fields)
    }

    /// Number of schema fields (not counting overflow).
    pub fn field_count(&self) -> usize {
        self.schema.column_count()
    }

    /// Total number of fields including overflow.
    pub fn total_field_count(&self) -> usize {
        self.schema.column_count() + self.overflow.as_ref().map_or(0, |m| m.len())
    }
}
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| `Schema` | `crates/clinker-record/src/schema.rs` | O(1) lookup via ahash with fixed seeds |
| `Record` | `crates/clinker-record/src/record.rs` | Schema-indexed + lazy overflow HashMap |

#### Intra-phase dependencies
- Requires: Task 1.2 (Value enum)
- Unblocks: Task 1.4 (MinimalRecord, Provenance, Counters)

#### Expanded test stubs
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Value;
    use std::sync::Arc;

    fn test_schema() -> Arc<Schema> {
        let cols: Vec<Box<str>> = vec![
            "id".into(), "name".into(), "age".into(), "email".into(), "active".into(),
        ];
        Arc::new(Schema::new(cols))
    }

    #[test]
    fn test_schema_index_lookup() {
        let schema = test_schema();
        assert_eq!(schema.index("id"), Some(0));
        assert_eq!(schema.index("name"), Some(1));
        assert_eq!(schema.index("age"), Some(2));
        assert_eq!(schema.index("email"), Some(3));
        assert_eq!(schema.index("active"), Some(4));
        assert_eq!(schema.column_count(), 5);
        assert_eq!(schema.column_name(0), Some("id"));
        assert_eq!(schema.columns().len(), 5);
    }

    #[test]
    fn test_schema_unknown_field_returns_none() {
        let schema = test_schema();
        assert_eq!(schema.index("nonexistent"), None);
        assert_eq!(schema.column_name(99), None);
    }

    #[test]
    fn test_record_get_set_by_name() {
        let schema = test_schema();
        let values = vec![
            Value::Integer(1),
            Value::String("Alice".into()),
            Value::Integer(30),
            Value::String("alice@example.com".into()),
            Value::Bool(true),
        ];
        let mut record = Record::new(schema, values);

        assert_eq!(record.get("name"), Some(&Value::String("Alice".into())));

        record.set("name", Value::String("Bob".into()));
        assert_eq!(record.get("name"), Some(&Value::String("Bob".into())));
    }

    #[test]
    fn test_record_overflow_lazy_init() {
        let schema = test_schema();
        let values = vec![Value::Null; 5];
        let mut record = Record::new(schema, values);

        // Overflow is None initially
        assert!(record.overflow.is_none());

        record.set_overflow("extra_field".into(), Value::Integer(99));
        assert!(record.overflow.is_some());
        assert_eq!(record.get("extra_field"), Some(&Value::Integer(99)));
    }

    #[test]
    fn test_record_overflow_no_shadow() {
        let schema = test_schema();
        let values = vec![Value::Null; 5];
        let mut record = Record::new(schema, values);

        // Setting a schema field name via set_overflow should redirect
        record.set_overflow("name".into(), Value::String("redirected".into()));
        assert_eq!(record.get("name"), Some(&Value::String("redirected".into())));
        // Overflow should still be None (or not contain "name")
        assert!(
            record.overflow.is_none()
                || !record.overflow.as_ref().unwrap().contains_key("name")
        );
    }

    #[test]
    fn test_record_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Record>();
    }

    // --- Edge-case tests ---

    #[test]
    fn test_record_new_length_mismatch_pad() {
        // Fewer values than schema columns → pad with Null in release
        let schema = test_schema(); // 5 columns
        let values = vec![Value::Integer(1), Value::String("Alice".into())]; // only 2
        let record = Record::new(schema, values);
        assert_eq!(record.get("id"), Some(&Value::Integer(1)));
        assert_eq!(record.get("name"), Some(&Value::String("Alice".into())));
        assert_eq!(record.get("age"), Some(&Value::Null)); // padded
    }

    #[test]
    fn test_record_iter_all_fields_ordering() {
        let schema = test_schema(); // id, name, age, email, active
        let values = vec![
            Value::Integer(1),
            Value::String("Alice".into()),
            Value::Integer(30),
            Value::String("alice@test.com".into()),
            Value::Bool(true),
        ];
        let mut record = Record::new(schema, values);
        record.set_overflow("extra".into(), Value::String("bonus".into()));

        let fields: Vec<(&str, &Value)> = record.iter_all_fields().collect();
        // Schema fields first in schema order, then overflow
        assert_eq!(fields[0].0, "id");
        assert_eq!(fields[1].0, "name");
        assert_eq!(fields[4].0, "active");
        assert_eq!(fields[5].0, "extra"); // overflow appended after schema
        assert_eq!(fields.len(), 6);
    }

    #[test]
    fn test_record_values_raw_access() {
        let schema = test_schema();
        let values = vec![Value::Integer(1); 5];
        let record = Record::new(schema, values);
        assert_eq!(record.values().len(), 5);
        assert_eq!(record.values()[0], Value::Integer(1));
    }

    #[test]
    fn test_record_total_field_count() {
        let schema = test_schema();
        let values = vec![Value::Null; 5];
        let mut record = Record::new(schema, values);
        assert_eq!(record.total_field_count(), 5);
        record.set_overflow("extra1".into(), Value::Integer(1));
        record.set_overflow("extra2".into(), Value::Integer(2));
        assert_eq!(record.total_field_count(), 7);
        assert_eq!(record.field_count(), 5); // schema only
    }

    #[test]
    fn test_schema_contains() {
        let schema = test_schema();
        assert!(schema.contains("id"));
        assert!(schema.contains("name"));
        assert!(!schema.contains("nonexistent"));
    }

    #[test]
    fn test_schema_duplicate_column_names() {
        // Duplicate headers: last index wins in HashMap. Documents the behavior.
        let cols: Vec<Box<str>> = vec!["a".into(), "b".into(), "a".into()];
        let schema = Schema::new(cols);
        assert_eq!(schema.column_count(), 3); // Vec has 3 entries
        assert_eq!(schema.index("a"), Some(2)); // last occurrence wins
    }

    #[test]
    fn test_schema_empty() {
        let schema = Schema::new(vec![]);
        assert_eq!(schema.column_count(), 0);
        assert_eq!(schema.index("anything"), None);
        assert_eq!(schema.column_name(0), None);
        assert!(schema.columns().is_empty());
    }

    #[test]
    fn test_record_set_unknown_field_returns_false() {
        let schema = test_schema();
        let values = vec![Value::Null; 5];
        let mut record = Record::new(schema, values);
        assert!(!record.set("nonexistent", Value::Integer(1)));
    }

    #[test]
    fn test_value_array_partial_ord_returns_none() {
        // Array vs Array comparison is undefined — returns None
        let a = Value::Array(vec![Value::Integer(1)]);
        let b = Value::Array(vec![Value::Integer(2)]);
        assert_eq!(a.partial_cmp(&b), None);
    }

    #[test]
    fn test_record_overflow_preserves_emit_order() {
        // Overflow uses IndexMap — insertion order must match output order
        let schema = test_schema();
        let values = vec![Value::Null; 5];
        let mut record = Record::new(schema, values);

        // Emit in specific order: Zulu, Alpha, Mike
        record.set_overflow("Zulu".into(), Value::Integer(3));
        record.set_overflow("Alpha".into(), Value::Integer(1));
        record.set_overflow("Mike".into(), Value::Integer(2));

        // Overflow iteration must preserve insertion order, not alphabetical
        let overflow: Vec<_> = record.overflow_fields().unwrap().collect();
        assert_eq!(overflow[0].0, "Zulu");   // first emitted
        assert_eq!(overflow[1].0, "Alpha");  // second emitted
        assert_eq!(overflow[2].0, "Mike");   // third emitted
    }
}
```

#### Risk / gotcha callouts
> ⚠️ **ahash HashMap::from_iter ignores custom hasher:** When collecting into a `HashMap<K, V, RandomState>`, the `FromIterator` impl uses `Default::default()` for the hasher, not the fixed-seed one. You must explicitly create the map with `HashMap::with_hasher()` and insert items manually.

> ⚠️ **debug_assert in set_overflow:** The `debug_assert!(false, ...)` in `set_overflow` for schema field redirect will panic in debug builds. This is intentional — it catches misuse during development but silently redirects in release. Tests that exercise this path must use `#[cfg_attr(debug_assertions, should_panic)]` or test only the release behavior.

---

### Task 1.4: MinimalRecord + RecordProvenance + PipelineCounters
**Status:** ⛔ Blocked (waiting on Task 1.3)
**Blocked by:** Task 1.3 — Schema/Record must be working

**Description:**
Implement `MinimalRecord` in `clinker-record::minimal`, `RecordProvenance` in
`clinker-record::provenance`, and `PipelineCounters`. These are simpler structs
that complete the data model.

**Implementation notes:**
- `MinimalRecord` is intentionally thin: just `fields: Vec<Value>`. No schema, no overflow.
  Position stability is guaranteed by the Arena that wraps it (see Phase 5).
- `RecordProvenance`: `source_file: Arc<str>`, `source_row: u64`, `source_count: u64`,
  `source_batch: Arc<str>`, `ingestion_timestamp: chrono::NaiveDateTime`. All fields pub.
- `PipelineCounters`: `total_count: u64`, `ok_count: u64`, `dlq_count: u64`. All fields pub.
  This struct lives in `clinker-record` (not `clinker-core`) so the `cxl` evaluator can
  reference it without depending on `clinker-core`.
- Both structs must be `Send + Sync` and `Clone`.

**Acceptance criteria:**
- [ ] `MinimalRecord` constructible with a `Vec<Value>`, indexable by position
- [ ] `RecordProvenance` constructible with all fields, `Arc<str>` sharing verified
- [ ] `PipelineCounters` constructible, all fields default to 0
- [ ] All three structs are `Send + Sync + Clone`

**Required unit tests (must pass before Task 1.5 unlocks):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_minimal_record_positional_access` | `fields[0]`, `fields[1]` return correct values | ⛔ Hard gate |
| `test_provenance_arc_sharing` | Two provenances from same file share `Arc<str>` (ptr equality) | ⛔ Hard gate |
| `test_pipeline_counters_default` | Default counters are all zero | ⛔ Hard gate |
| `test_all_structs_send_sync` | Compile-time Send + Sync assertion for all three | ⛔ Hard gate |

> ⛔ **Hard gate:** Task 1.5 status remains `Blocked` until all tests above pass.

#### Sub-tasks
- [ ] **[1.4.1]** Implement `MinimalRecord` with `new()`, `get()`, `len()`, `is_empty()` (~1h)
- [ ] **[1.4.2]** Implement `RecordProvenance` with `new()` constructor and all pub fields (~1h)
- [ ] **[1.4.3]** Implement `PipelineCounters` with `Default`, `increment_ok()`, `increment_dlq()`, `snapshot()` (~1h)
- [ ] **[1.4.4]** Write all 4 hard-gate tests + edge-case tests (provenance row independence, counter increments) (~1h)

#### Code scaffolding
```rust
// Module: crates/clinker-record/src/minimal.rs

use crate::value::Value;

/// A schema-free record: positional access only.
/// Position stability is guaranteed by the Arena that wraps it (Phase 5).
#[derive(Debug, Clone)]
pub struct MinimalRecord {
    pub fields: Vec<Value>,
}

impl MinimalRecord {
    pub fn new(fields: Vec<Value>) -> Self {
        Self { fields }
    }

    pub fn get(&self, index: usize) -> Option<&Value> {
        self.fields.get(index)
    }

    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }
}
```

```rust
// Module: crates/clinker-record/src/provenance.rs

use chrono::NaiveDateTime;
use std::sync::Arc;

/// Tracks where a record came from in the source data.
/// Arc<str> fields are shared across all records from the same file/run.
#[derive(Debug, Clone)]
pub struct RecordProvenance {
    pub source_file: Arc<str>,
    pub source_row: u64,
    pub source_count: u64,
    pub source_batch: Arc<str>,
    pub ingestion_timestamp: NaiveDateTime,
}

impl RecordProvenance {
    /// Create provenance for a specific row in a source file.
    /// `source_file` and `source_batch` are shared via Arc — call Arc::from("path")
    /// once per file/run and clone the Arc for each row (cheap refcount bump).
    pub fn new(
        source_file: Arc<str>,
        source_row: u64,
        source_count: u64,
        source_batch: Arc<str>,
        ingestion_timestamp: NaiveDateTime,
    ) -> Self {
        Self { source_file, source_row, source_count, source_batch, ingestion_timestamp }
    }

    /// Create a provenance factory for a specific source file + batch.
    /// Returns a closure that produces provenances with incrementing row numbers.
    /// Used during Phase 1 streaming to avoid repeated Arc::clone boilerplate.
    pub fn factory(
        source_file: Arc<str>,
        source_batch: Arc<str>,
        ingestion_timestamp: NaiveDateTime,
    ) -> impl FnMut(u64) -> Self {
        move |source_row| Self {
            source_file: Arc::clone(&source_file),
            source_row,
            source_count: 0, // set after Phase 1 completes (Arena size)
            source_batch: Arc::clone(&source_batch),
            ingestion_timestamp,
        }
    }
}
```

```rust
// Module: crates/clinker-record/src/counters.rs

/// Pipeline-wide counters for total/ok/dlq record counts.
/// Lives in clinker-record so cxl evaluator can reference it
/// without depending on clinker-core.
///
/// Updated at chunk boundaries during Phase 2 — all records within a chunk
/// see the same counter values (snapshotted at chunk start).
#[derive(Debug, Clone, Default)]
pub struct PipelineCounters {
    pub total_count: u64,
    pub ok_count: u64,
    pub dlq_count: u64,
}

impl PipelineCounters {
    /// Increment ok_count by n (typically chunk_size - chunk_errors after a chunk completes).
    pub fn increment_ok(&mut self, n: u64) {
        self.ok_count += n;
    }

    /// Increment dlq_count by n (typically chunk_errors after a chunk completes).
    pub fn increment_dlq(&mut self, n: u64) {
        self.dlq_count += n;
    }

    /// Snapshot the current counters for sharing with a chunk's parallel evaluation.
    /// The snapshot is immutable — records within the chunk all see the same values.
    pub fn snapshot(&self) -> Self {
        self.clone()
    }
}
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| `MinimalRecord` | `crates/clinker-record/src/minimal.rs` | Schema-free, positional only |
| `RecordProvenance` | `crates/clinker-record/src/provenance.rs` | Arc<str> sharing for file/batch |
| `PipelineCounters` | `crates/clinker-record/src/counters.rs` | Default all zeros |

#### Intra-phase dependencies
- Requires: Task 1.3 (not structurally, but gate ordering)
- Unblocks: Task 1.5 (lib.rs re-exports)

#### Expanded test stubs
```rust
#[cfg(test)]
mod minimal_tests {
    use super::*;
    use crate::value::Value;

    #[test]
    fn test_minimal_record_positional_access() {
        let rec = MinimalRecord::new(vec![
            Value::Integer(1),
            Value::String("Alice".into()),
            Value::Bool(true),
        ]);
        assert_eq!(rec.get(0), Some(&Value::Integer(1)));
        assert_eq!(rec.get(1), Some(&Value::String("Alice".into())));
        assert_eq!(rec.get(2), Some(&Value::Bool(true)));
        assert_eq!(rec.get(3), None);
        assert_eq!(rec.len(), 3);
    }
}
```

```rust
#[cfg(test)]
mod provenance_tests {
    use super::*;
    use chrono::NaiveDate;
    use std::sync::Arc;

    #[test]
    fn test_provenance_arc_sharing() {
        let file: Arc<str> = Arc::from("data/input.csv");
        let batch: Arc<str> = Arc::from("batch-001");
        let ts = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();

        let p1 = RecordProvenance {
            source_file: Arc::clone(&file),
            source_row: 0,
            source_count: 1,
            source_batch: Arc::clone(&batch),
            ingestion_timestamp: ts,
        };
        let p2 = RecordProvenance {
            source_file: Arc::clone(&file),
            source_row: 1,
            source_count: 1,
            source_batch: Arc::clone(&batch),
            ingestion_timestamp: ts,
        };

        // Verify Arc pointer equality (same allocation)
        assert!(Arc::ptr_eq(&p1.source_file, &p2.source_file));
        assert!(Arc::ptr_eq(&p1.source_batch, &p2.source_batch));
    }
}
```

```rust
#[cfg(test)]
mod counters_tests {
    use super::*;

    #[test]
    fn test_pipeline_counters_default() {
        let c = PipelineCounters::default();
        assert_eq!(c.total_count, 0);
        assert_eq!(c.ok_count, 0);
        assert_eq!(c.dlq_count, 0);
    }

    #[test]
    fn test_all_structs_send_sync() {
        fn assert_send_sync_clone<T: Send + Sync + Clone>() {}
        assert_send_sync_clone::<super::super::minimal::MinimalRecord>();
        assert_send_sync_clone::<super::super::provenance::RecordProvenance>();
        assert_send_sync_clone::<PipelineCounters>();
    }

    // --- Edge-case tests ---

    #[test]
    fn test_provenance_row_independence() {
        // Two provenances from same file share Arc<str> but have different rows
        let file: Arc<str> = Arc::from("data/input.csv");
        let batch: Arc<str> = Arc::from("batch-001");
        let ts = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()
            .and_hms_opt(0, 0, 0).unwrap();

        let p1 = RecordProvenance::new(Arc::clone(&file), 1, 100, Arc::clone(&batch), ts);
        let p2 = RecordProvenance::new(Arc::clone(&file), 2, 100, Arc::clone(&batch), ts);

        assert_eq!(p1.source_row, 1);
        assert_eq!(p2.source_row, 2);
        assert!(Arc::ptr_eq(&p1.source_file, &p2.source_file));
    }

    #[test]
    fn test_provenance_factory() {
        let file: Arc<str> = Arc::from("data/input.csv");
        let batch: Arc<str> = Arc::from("batch-001");
        let ts = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()
            .and_hms_opt(0, 0, 0).unwrap();

        let mut make_prov = RecordProvenance::factory(file.clone(), batch.clone(), ts);
        let p1 = make_prov(1);
        let p2 = make_prov(2);

        assert_eq!(p1.source_row, 1);
        assert_eq!(p2.source_row, 2);
        assert!(Arc::ptr_eq(&p1.source_file, &p2.source_file));
        assert_eq!(p1.source_count, 0); // not yet set (set after Phase 1)
    }

    #[test]
    fn test_pipeline_counters_increment() {
        let mut c = PipelineCounters::default();
        c.total_count = 1000;
        c.increment_ok(100);
        c.increment_dlq(5);
        assert_eq!(c.ok_count, 100);
        assert_eq!(c.dlq_count, 5);

        // Snapshot is a clone — mutations don't affect it
        let snap = c.snapshot();
        c.increment_ok(200);
        assert_eq!(snap.ok_count, 100); // unchanged
        assert_eq!(c.ok_count, 300);    // updated
    }

    #[test]
    fn test_minimal_record_empty() {
        let rec = MinimalRecord::new(vec![]);
        assert!(rec.is_empty());
        assert_eq!(rec.len(), 0);
        assert_eq!(rec.get(0), None);
    }
}
```

#### Risk / gotcha callouts
> ⚠️ **MinimalRecord field visibility:** `fields` is `pub` for direct `fields[i]` access in the spec, but prefer the `get()` method for bounds-checked access. Document both patterns.

> ⚠️ **PipelineCounters atomicity:** These are plain `u64` fields, not atomics. In Phase 5+, the main thread updates counters between chunks (single-threaded) and then snapshots for the next chunk's parallel evaluation (read-only). No atomics needed as long as this pattern is maintained. If concurrent chunk dispatch is added in v2, switch to `AtomicU64`.

> ⚠️ **RecordProvenance.source_count = 0 initially:** The factory sets `source_count` to 0 because the total isn't known until Phase 1 completes. A post-Phase-1 pass must set `source_count` on all provenances. If forgotten, CXL expressions using `_source_count` will see 0. Add an assertion in Phase 5's executor.

---

### Task 1.5: Stub Crates + lib.rs Re-exports
**Status:** ⛔ Blocked (waiting on Task 1.4)
**Blocked by:** Task 1.4 — all clinker-record types must be working

**Description:**
Finalize `clinker-record/src/lib.rs` with proper re-exports. Ensure the 5 remaining
stub crates (`cxl`, `cxl-cli`, `clinker-format`, `clinker-core`, `clinker`) compile
with their declared dependencies on `clinker-record`.

**Implementation notes:**
- `clinker-record/src/lib.rs` should `pub mod value; pub mod schema; pub mod record; pub mod minimal; pub mod provenance; pub mod coercion; pub mod counters;` and re-export key types at the crate root.
- Each stub crate should have a minimal `lib.rs` or `main.rs` that `use clinker_record::*;` to verify the dependency link compiles.
- `clinker` and `cxl-cli` are binary crates — `main.rs` with `fn main() {}`.
- Run `cargo build --workspace` and `cargo clippy --workspace -- -D warnings` to verify.

**Acceptance criteria:**
- [ ] `clinker_record::Value`, `clinker_record::Schema`, `clinker_record::Record` importable from any dependent crate
- [ ] `cargo build --workspace` succeeds
- [ ] `cargo clippy --workspace -- -D warnings` passes
- [ ] `cargo test -p clinker-record` runs all 40+ tests and passes

#### Sub-tasks
- [ ] **[1.5.1]** Write `clinker-record/src/lib.rs` with all `pub mod` declarations and root re-exports (~1h)
- [ ] **[1.5.2]** Update stub crates to import `clinker_record` types (verifies dependency link) (~1h)
- [ ] **[1.5.3]** Run full workspace build, clippy, and test suite; fix any issues (~1h)

#### Code scaffolding
```rust
// Module: crates/clinker-record/src/lib.rs

pub mod value;
pub mod schema;
pub mod record;
pub mod minimal;
pub mod provenance;
pub mod coercion;
pub mod counters;

// Re-export primary types at crate root for ergonomic imports
pub use value::Value;
pub use schema::Schema;
pub use record::Record;
pub use minimal::MinimalRecord;
pub use provenance::RecordProvenance;
pub use counters::PipelineCounters;
pub use coercion::{CoercionError, coerce_to_int, coerce_to_float, coerce_to_string,
    coerce_to_bool, coerce_to_date, coerce_to_datetime,
    DEFAULT_DATE_FORMATS, DEFAULT_DATETIME_FORMATS};
```

```rust
// Module: crates/cxl/src/lib.rs
// Stub — verifies clinker-record dependency compiles
use clinker_record::{Value, Schema, Record};

// Silence unused import warnings in stub
#[allow(unused_imports)]
use clinker_record::PipelineCounters;
```

```rust
// Module: crates/clinker-format/src/lib.rs
use clinker_record::{Value, Record, Schema};

#[allow(unused_imports)]
use clinker_record::MinimalRecord;
```

```rust
// Module: crates/clinker-core/src/lib.rs
use clinker_record::{Value, Record, Schema, PipelineCounters};

#[allow(unused_imports)]
use clinker_record::RecordProvenance;
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| `lib.rs` | `crates/clinker-record/src/lib.rs` | All pub mod + root re-exports |
| `cxl` stub | `crates/cxl/src/lib.rs` | Verifies dep link |
| `clinker-format` stub | `crates/clinker-format/src/lib.rs` | Verifies dep link |
| `clinker-core` stub | `crates/clinker-core/src/lib.rs` | Verifies dep link |
| `cxl-cli` stub | `crates/cxl-cli/src/main.rs` | Binary, `fn main() {}` |
| `clinker` stub | `crates/clinker/src/main.rs` | Binary, `fn main() {}` |

#### Intra-phase dependencies
- Requires: Task 1.4 (all types implemented)
- Unblocks: Phase 2 (CXL parser, format crate)

#### Expanded test stubs
```rust
// No new #[test] functions for this task.
// Validation is: cargo build --workspace && cargo test -p clinker-record
// All 40+ tests from Tasks 1.2–1.4 must pass.
```

#### Risk / gotcha callouts
> ⚠️ **Unused import warnings:** Stub crates that `use clinker_record::*` will trigger `unused_import` warnings under `clippy -D warnings`. Use targeted imports with `#[allow(unused_imports)]` or reference the types in a trivial way.

> ⚠️ **Re-export naming collisions:** If `coercion.rs` exports free functions like `coerce_to_int`, re-exporting them at the crate root puts them in the same namespace as struct constructors. Use explicit `pub use` for each symbol rather than glob re-exports.

---

## Intra-phase Dependency Graph

```
  ┌─────────┐
  │ Task 1.1│  Workspace + directory structure
  └────┬────┘
       │ cargo build --workspace succeeds
       v
  ┌─────────┐
  │ Task 1.2│  Value enum + coercion
  └────┬────┘
       │ 10 hard-gate tests pass
       v
  ┌─────────┐
  │ Task 1.3│  Schema + Record
  └────┬────┘
       │ 6 hard-gate tests pass
       v
  ┌─────────┐
  │ Task 1.4│  MinimalRecord + Provenance + Counters
  └────┬────┘
       │ 4 hard-gate tests pass
       v
  ┌─────────┐
  │ Task 1.5│  Stub crates + lib.rs re-exports
  └─────────┘
       │ cargo build --workspace + 40+ tests pass
       v
    Phase 2
```

**Critical path:** 1.1 → 1.2 → 1.3 → 1.4 → 1.5 (strictly sequential, no parallelism within this phase)

## Assumptions Table

| # | Assumption | Impact if wrong | Mitigation |
|---|-----------|-----------------|------------|
| A1 | Rust 1.85+ is available in CI and dev | `edition = "2024"` fails to parse | Pin toolchain in `rust-toolchain.toml` |
| A2 | ~~RESOLVED: Custom Deserialize impl handles integer-before-float and date string detection~~ | — | — |
| A3 | ~~RESOLVED: Custom Deserialize impl tries DateTime→Date→String for JSON strings~~ | — | — |
| A4 | ~~RESOLVED: ahash does NOT guarantee cross-platform determinism (confirmed). Output ordering uses Schema.columns Vec, never HashMap iteration. Overflow uses IndexMap for emit-order determinism.~~ | — | — |
| A5 | `PipelineCounters` is only accessed single-threaded in Phase 1 | Data races if shared across threads | Wrap in `Mutex` or switch to `AtomicU64` in Phase 5 |
| A6 | `debug_assert_eq!` on Record::new length mismatch is acceptable | Silent pad/truncate hides bugs in release | Upgrade to hard error once pipeline is stable |
| A7 | Custom `PartialEq` (NaN==NaN, -0.0!=0.0) matches downstream Hash impl | Hash/Eq contract violation if Hash is derived | Implement Hash manually with `to_bits()` when needed |
| A8 | `CoercionError` does not need miette spans in Phase 1 | Error messages lack source location | Add miette integration in Phase 3+ if needed |
| A9 | Custom Deserialize parsing date-like strings as Date won't false-positive on real data | Text field `"2024-01-15"` becomes Date not String | Use `schema_overrides` to pin type to String when needed |
| A10 | Float→Int truncation toward zero (not rounding) matches user expectation | Users expect `1.9 → 2` | Document: "use `.round()` before `.to_int()` if rounding is desired" |
| A11 | `"1.5".to_int()` should fail (not parse as float then truncate) | Users expect `"1.5" → 1` | Document: "use `.to_float().to_int()` for string→float→int chain" |
| A12 | IndexMap preserves insertion order deterministically | Overflow field output order would be random | IndexMap is explicitly designed for this — guaranteed by crate |
| A13 | US-only default date chain; EU users configure `pipeline.date_formats` explicitly | EU dates like `01/02/2024` misinterpreted as US (Jan 2) | Document in spec: "set date_formats for non-US locales" |

---

## Decisions Log (drill-phase — 2026-03-29)

| # | Decision | Rationale | Affects |
|---|---------|-----------|---------|
| D1 | Parameterized coercion: `coerce_to_date(value, formats: &[&str])` | Three-layer date format chain from spec. clinker-record owns parsing loop, callers provide chain. | Task 1.2 (signatures), Phase 3 (evaluator passes pipeline config), Phase 4 (format reader passes config) |
| D2 | ahash NOT relied on for ordering | Research confirmed ahash produces different hashes on different platforms and versions. HashMap iteration order is non-deterministic. | Task 1.3 (Schema ordering comes from Vec, not HashMap) |
| D3 | `rust-toolchain.toml` pinned to channel 1.85 | Guarantees edition 2024 support across dev and CI | Task 1.1 (new file in scaffolding) |
| D4 | IndexMap for Record overflow | Emit-order output — users expect output columns in the order they wrote `emit` statements | Task 1.3 (Record struct, Cargo.toml), spec §3 |
| D5 | US-only default date chain | EU format `%d/%m/%Y` is ambiguous with US `%m/%d/%Y`. Default chain: ISO, US slash, alt ISO, US dash. EU users set `pipeline.date_formats`. | Task 1.2 (DEFAULT_DATE_FORMATS constant), spec §4 |

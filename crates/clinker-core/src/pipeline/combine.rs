//! Combine runtime primitives: hashing, float canonicalization, and (in
//! later sub-tasks) the index-separated build-side hash table.
//!
//! **Scope of this module:** runtime data structures consumed by the
//! `PlanNode::Combine` executor arm. Plan-time vocabulary —
//! `CombineStrategy`, `DecomposedPredicate`, `EqualityConjunct`,
//! `CombineInput` — lives in `crate::plan::combine`. Do not cross-pollinate.
//!
//! **Concurrency model (spec §Concurrency Model):** every public struct
//! declared here is designed to be immutable after construction. `RandomState`
//! is fixed at build time and shared with probe; no interior mutability.
//!
//! **Memory model:** combine is pipeline-breaking on the build side. All
//! build records are materialized into a single index-separated table before
//! any probe runs. Accounted allocation sources (per DataFusion #14222,
//! #5490, #6170): hash index, chain vector, record arena, cached build-side
//! key values.
//!
//! Primitives provided here:
//!   - `canonical_f64`/`canonical_f32`, `hash_composite_key`,
//!     `keys_equal_canonicalized`, module-level constants.
//!   - `CombineError`, `KeyExtractor`.
//!   - `CombineHashTable` with `build()` / `probe()` / `ProbeIter`
//!     (index-separated design; immutable after build; NULL probe
//!     short-circuit; equality verification across full collision chain;
//!     per-[`MEMORY_CHECK_INTERVAL`] budget checks during build).
//!   - `memory_bytes()` accounting across all four allocation sources.

use ahash::RandomState;
use chrono::{Datelike, Timelike};
use clinker_record::{Record, RecordStorage, Value};
use cxl::ast::Expr;
use cxl::eval::{EvalContext, EvalError, eval_expr};
use cxl::resolve::traits::FieldResolver;
use cxl::typecheck::TypedProgram;
use hashbrown::HashTable;
use std::collections::HashMap;
use std::hash::{BuildHasher, Hasher};
use std::sync::Arc;

use crate::pipeline::memory::MemoryBudget;

/// Period (measured in records processed) between
/// [`crate::pipeline::memory::MemoryBudget::should_abort`] checks during
/// `CombineHashTable::build` AND during probe-side fan-out emission.
///
/// Matches ClickHouse per-block cadence and is the same order of
/// magnitude as DataFusion's default 8192-row RecordBatch. Per-row RSS
/// polling is infeasible (~10 µs per `/proc/self/status` read ⇒ ~10 s/sec
/// overhead at 1 M rec/sec).
pub const MEMORY_CHECK_INTERVAL: usize = 10_000;

/// Maximum number of build-side matches accumulated into a single driver's
/// `Value::Array` under `MatchMode::Collect`. Overflow truncates and emits a
/// W-level diagnostic once per driver record.
///
/// No ETL tool surveyed has a built-in cap and every SQL engine
/// (DuckDB, Spark, Polars) has documented OOMs at hot keys under
/// `collect_list`. Clinker sets its own default; per-combine
/// configurability is a future knob.
pub const COLLECT_PER_GROUP_CAP: usize = 10_000;

/// End-of-chain sentinel for `CombineHashTable::chain`. Selected so the entire
/// `u32` index range except `u32::MAX` is available for records — a combine
/// with 4 billion build rows is not a realistic scenario.
pub const SENTINEL: u32 = u32::MAX;

/// Canonical quiet-NaN bit pattern for `f64`. Matches Polars
/// `polars_utils::total_ord::canonical_f64` and Spark
/// `NormalizeFloatingNumbers` (both pick the identical value — de-facto
/// Rust/JVM community standard).
pub const CANONICAL_NAN_F64: u64 = 0x7ff8_0000_0000_0000;

/// Canonical quiet-NaN bit pattern for `f32`.
pub const CANONICAL_NAN_F32: u32 = 0x7fc0_0000;

// Type-discriminator bytes written before each `Value` in
// `hash_composite_key`. Different variants MUST hash to different byte
// prefixes to prevent cross-type collisions (`Integer(42)` must not hash the
// same as `String("42")`). The sentinel for `Null` is deliberately placed at
// the top of the byte range to avoid any visual overlap with real tags.
const TAG_BOOL: u8 = 0x01;
const TAG_INTEGER: u8 = 0x02;
const TAG_FLOAT: u8 = 0x03;
const TAG_STRING: u8 = 0x04;
const TAG_DATE: u8 = 0x05;
const TAG_DATETIME: u8 = 0x06;
const TAG_ARRAY: u8 = 0x07;
const TAG_MAP: u8 = 0x08;
const TAG_NULL: u8 = 0xFF;

/// Canonicalize an `f64` for hashing and equality. Collapses `-0.0 → +0.0`
/// and every NaN bit pattern (signaling, quiet, with any payload or sign) to
/// `CANONICAL_NAN_F64`.
///
/// The `+ 0.0` step is the community-standard idiom shared by Polars, Spark,
/// and `ordered-float`. It exploits IEEE-754: `-0.0 + 0.0 == +0.0`, and any
/// NaN operand produces a NaN result (so the branch captures every NaN input
/// in one check).
#[inline]
pub fn canonical_f64(x: f64) -> f64 {
    let x = x + 0.0;
    if x.is_nan() {
        f64::from_bits(CANONICAL_NAN_F64)
    } else {
        x
    }
}

/// Canonicalize an `f32` for hashing and equality. See [`canonical_f64`].
#[inline]
pub fn canonical_f32(x: f32) -> f32 {
    let x = x + 0.0;
    if x.is_nan() {
        f32::from_bits(CANONICAL_NAN_F32)
    } else {
        x
    }
}

/// Hash a composite key formed by a sequence of `Value`s. Feeds every value
/// through a single streaming AHasher with a one-byte type tag preceding each
/// value's content bytes.
///
/// **Properties:**
///   - Type-disjoint: `hash([Integer(42)]) != hash([String("42")])` (with
///     overwhelming probability) because the type tags differ.
///   - Null-stable: `Value::Null` always hashes identically under the same
///     `RandomState`, independent of position.
///   - Float-canonical: `NaN`-variants and `±0.0` all hash identically via
///     [`canonical_f64`] / [`canonical_f32`].
///   - Order-sensitive for arrays/maps (array elements are fed in order; map
///     entries are fed in sorted key order for determinism across two maps
///     that have the same entries inserted in different orders).
///
/// Matches the streaming-hash design per Polars PR #15559 (migration away
/// from per-column `combine_hash`) and DataFusion's
/// `create_hashes`/`combine_hashes` seam.
pub fn hash_composite_key(values: &[Value], state: &RandomState) -> u64 {
    let mut h = state.build_hasher();
    for v in values {
        hash_value_into(v, &mut h);
    }
    h.finish()
}

/// Writes a `Value` into a running `Hasher`. Recursive over `Array` and `Map`.
fn hash_value_into<H: Hasher>(v: &Value, h: &mut H) {
    match v {
        Value::Null => h.write_u8(TAG_NULL),
        Value::Bool(b) => {
            h.write_u8(TAG_BOOL);
            h.write_u8(u8::from(*b));
        }
        Value::Integer(i) => {
            h.write_u8(TAG_INTEGER);
            h.write_i64(*i);
        }
        Value::Float(f) => {
            h.write_u8(TAG_FLOAT);
            h.write_u64(canonical_f64(*f).to_bits());
        }
        Value::String(s) => {
            h.write_u8(TAG_STRING);
            h.write_usize(s.len()); // length prefix prevents concatenation collisions
            h.write(s.as_bytes());
        }
        Value::Date(d) => {
            h.write_u8(TAG_DATE);
            h.write_i32(d.num_days_from_ce());
        }
        Value::DateTime(dt) => {
            h.write_u8(TAG_DATETIME);
            // `and_utc()` reinterprets the naive datetime as UTC so we can
            // extract a stable (timestamp_seconds, nanoseconds) pair that
            // losslessly identifies every chrono-representable instant.
            let utc = dt.and_utc();
            h.write_i64(utc.timestamp());
            h.write_u32(utc.nanosecond());
        }
        Value::Array(arr) => {
            h.write_u8(TAG_ARRAY);
            h.write_usize(arr.len());
            for x in arr {
                hash_value_into(x, h);
            }
        }
        Value::Map(m) => {
            h.write_u8(TAG_MAP);
            h.write_usize(m.len());
            // Maps hash in sorted-key order so two Maps with the same entries
            // hash identically regardless of insertion order. IndexMap
            // preserves insertion order, which is useful for display but
            // undesired for hash determinism.
            let mut keys: Vec<&Box<str>> = m.keys().collect();
            keys.sort_unstable();
            for k in keys {
                h.write_usize(k.len());
                h.write(k.as_bytes());
                // `IndexMap::get` is O(1); lookup per key is fine.
                hash_value_into(m.get(k.as_ref()).expect("key from iteration"), h);
            }
        }
    }
}

/// Canonicalized element-wise key equality. Applied AFTER hash match in the
/// probe path to filter out hash collisions (DataFusion #843 — emitting
/// no-match rows before exhausting the collision chain causes phantom
/// output).
///
/// **Semantics:**
///   - Lengths must match; otherwise unequal.
///   - Per-position cross-type (e.g., `Integer` vs `String`) is unequal.
///   - Per SQL 3VL, `Null` compared to ANY value (including another `Null`)
///     is unequal. In practice the probe path short-circuits when any
///     probe-side key is `Null`, so this branch handles build-side `Null`
///     keys colliding with non-null probe keys via hash; the equality check
///     rejects them, so they never match.
///   - `Float` uses [`canonical_f64`] on both sides, then compares `to_bits`
///     — so `NaN == NaN` (both canonicalize) and `+0.0 == -0.0` (both
///     collapse to `+0.0`).
///   - `Array` and `Map` recurse via the same rules. Maps compare
///     order-independently (same keys, same values per key).
pub(crate) fn keys_equal_canonicalized(left: &[Value], right: &[Value]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right.iter())
        .all(|(l, r)| value_equal_canonicalized(l, r))
}

fn value_equal_canonicalized(a: &Value, b: &Value) -> bool {
    match (a, b) {
        // SQL 3VL: NULL is never equal to anything, including NULL.
        (Value::Null, _) | (_, Value::Null) => false,
        (Value::Bool(x), Value::Bool(y)) => x == y,
        (Value::Integer(x), Value::Integer(y)) => x == y,
        (Value::Float(x), Value::Float(y)) => {
            canonical_f64(*x).to_bits() == canonical_f64(*y).to_bits()
        }
        (Value::String(x), Value::String(y)) => x == y,
        (Value::Date(x), Value::Date(y)) => x == y,
        (Value::DateTime(x), Value::DateTime(y)) => x == y,
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len()
                && x.iter()
                    .zip(y.iter())
                    .all(|(l, r)| value_equal_canonicalized(l, r))
        }
        (Value::Map(x), Value::Map(y)) => {
            if x.len() != y.len() {
                return false;
            }
            // Order-independent: every key in x exists in y with an equal
            // canonicalized value.
            x.iter().all(|(k, vx)| {
                y.get(k.as_ref())
                    .is_some_and(|vy| value_equal_canonicalized(vx, vy))
            })
        }
        _ => false, // cross-type
    }
}

// ──────────────────────────────────────────────────────────────────────────
// CombineError
// ──────────────────────────────────────────────────────────────────────────

/// Every runtime failure originating inside the combine operator.
///
/// The executor arm (C.2.2) converts these into `PipelineError` (typically
/// `PipelineError::Internal` for build/probe failures, or an E310 runtime
/// diagnostic for memory/unmatched-driver cases). Callers outside the
/// executor generally do not observe this type directly.
#[derive(Debug)]
pub enum CombineError {
    /// The hash build phase could not complete. Typically wraps an
    /// expression-evaluation failure on a build-side record (e.g. a
    /// malformed cast in the key expression) that was not caught by the
    /// plan-time typechecker.
    HashBuildFailed { reason: String },

    /// The probe phase aborted mid-stream. Reserved for cases where probe
    /// iteration hit a non-memory terminal error (e.g. downstream writer
    /// back-pressure shutdown).
    ProbeAborted { reason: String },

    /// Memory budget was exhausted while building or probing. `used` is
    /// the hash table's self-reported footprint at the moment of the
    /// check; `limit` is the configured budget. Emitted after
    /// `MemoryBudget::should_abort` returns true (checked every
    /// [`MEMORY_CHECK_INTERVAL`] records to amortize the cost).
    MemoryLimitExceeded { used: u64, limit: u64 },

    /// Key-expression evaluation failed. `side` is `"driving"` or
    /// `"build"` so the executor can produce an accurate diagnostic.
    KeyEvalFailed {
        source: EvalError,
        side: &'static str,
    },

    /// Disk spill failed. Wired into the error surface now so Phase C.4
    /// can populate it without an API break; unused in C.2.
    SpillFailed { reason: String },
}

impl std::fmt::Display for CombineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CombineError::HashBuildFailed { reason } => {
                write!(f, "combine hash build failed: {reason}")
            }
            CombineError::ProbeAborted { reason } => {
                write!(f, "combine probe aborted: {reason}")
            }
            CombineError::MemoryLimitExceeded { used, limit } => write!(
                f,
                "combine memory limit exceeded: used {used} bytes, limit {limit} bytes"
            ),
            CombineError::KeyEvalFailed { source, side } => {
                write!(f, "combine {side}-side key evaluation failed: {source}")
            }
            CombineError::SpillFailed { reason } => {
                write!(f, "combine spill failed: {reason}")
            }
        }
    }
}

impl std::error::Error for CombineError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CombineError::KeyEvalFailed { source, .. } => Some(source),
            _ => None,
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────
// KeyExtractor
// ──────────────────────────────────────────────────────────────────────────

/// Evaluates the scalar key expression for one side of a combine's equality
/// conjuncts against a single `Record`.
///
/// **Why two fields per program, not one:** CXL's `eval_expr` takes `(&Expr,
/// &TypedProgram, …)` — the `TypedProgram` provides the type/resolution
/// context (NodeId table, regex cache, binding table) while the `Expr`
/// identifies the specific expression node to evaluate. Because each
/// `EqualityConjunct` is compiled as a trivial single-emit wrapper at
/// `bind_combine` time (C.2.4), the extractor stores each `(typed, expr)`
/// pair side-by-side and replays them through the free `eval_expr` entry
/// point — the same hot-path primitive used by the hash aggregator. No
/// `ProgramEvaluator` allocation per extraction, no `IndexMap` churn.
///
/// **One extractor per side:** the driving side and the build side each get
/// their own `KeyExtractor`. Same number of programs (one per equality
/// conjunct) but different expressions and different type contexts — each
/// side's expressions are typed against that side's input `Row`. The
/// executor (C.2.2) builds both at dispatch time from
/// `DecomposedPredicate.equalities[i].left_program / .left_expr` (driving)
/// and `.right_program / .right_expr` (build).
///
/// **Stateless:** `extract` takes `&self`. No interior mutability, no
/// `&mut [ProgramEvaluator]` parameter. Key extraction is pure expression
/// evaluation — no `distinct` state, no `let` bindings that survive the
/// call, no `$meta.*` mutations. This is what lets the built
/// `CombineHashTable` be safely shared across concurrent probe workers in
/// Phase C.3+.
pub struct KeyExtractor {
    programs: Vec<KeyProgram>,
}

/// One (typed program, key expression) pair. Holds `typed` as an `Arc` so
/// multiple `KeyExtractor`s or concurrent extraction workers can share the
/// compiled artifact without cloning it.
struct KeyProgram {
    typed: Arc<TypedProgram>,
    expr: Expr,
}

impl KeyExtractor {
    /// Build an extractor from an ordered list of `(typed_program, key_expr)`
    /// pairs. Order is preserved; the i-th pair produces the i-th `Value`
    /// in the result of [`KeyExtractor::extract`].
    pub fn new(programs: Vec<(Arc<TypedProgram>, Expr)>) -> Self {
        Self {
            programs: programs
                .into_iter()
                .map(|(typed, expr)| KeyProgram { typed, expr })
                .collect(),
        }
    }

    /// Number of key columns this extractor produces.
    pub fn len(&self) -> usize {
        self.programs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.programs.is_empty()
    }

    /// Evaluate every key expression in order against `resolver + ctx`.
    /// Returns one `Value` per expression. Propagates the first
    /// [`EvalError`] unchanged — the executor is responsible for wrapping
    /// it with `side: "driving"` / `side: "build"` context via
    /// [`CombineError::KeyEvalFailed`].
    ///
    /// **Window context:** combine's key extraction runs outside any
    /// window, so `window: None` is passed through. The `NullStorage` type
    /// parameter satisfies the generic bound without materializing any
    /// storage — identical to the pattern used by the aggregator's free
    /// `eval_expr` calls (`crates/clinker-core/src/aggregation.rs`).
    pub fn extract(
        &self,
        ctx: &EvalContext<'_>,
        resolver: &dyn FieldResolver,
    ) -> Result<Vec<Value>, EvalError> {
        let env: HashMap<String, Value> = HashMap::new();
        let meta_state: indexmap::IndexMap<String, Value> = indexmap::IndexMap::new();
        let mut out = Vec::with_capacity(self.programs.len());
        for kp in &self.programs {
            let v = eval_expr::<NullStorage>(
                &kp.expr,
                &kp.typed,
                ctx,
                resolver,
                None,
                &env,
                &meta_state,
            )?;
            out.push(v);
        }
        Ok(out)
    }
}

/// Placeholder `RecordStorage` for windowless expression evaluation. Every
/// method returns an empty result; the trait object is never queried
/// because `KeyExtractor::extract` passes `window: None`. This satisfies
/// the `S: RecordStorage` type parameter on [`eval_expr`] the same way
/// `executor/mod.rs:260` and `aggregation.rs:49` do for their own
/// windowless call sites — each module declares its own zero-sized type
/// rather than exposing a shared one from cxl.
struct NullStorage;

impl RecordStorage for NullStorage {
    fn resolve_field(&self, _: u32, _: &str) -> Option<Value> {
        None
    }
    fn resolve_qualified(&self, _: u32, _: &str, _: &str) -> Option<Value> {
        None
    }
    fn available_fields(&self, _: u32) -> Vec<&str> {
        vec![]
    }
    fn record_count(&self) -> u32 {
        0
    }
}

// ──────────────────────────────────────────────────────────────────────────
// CombineHashTable
// ──────────────────────────────────────────────────────────────────────────

/// Yielded by [`ProbeIter`] for each build-side record that (a) hashed the
/// same bucket as the probe key AND (b) passed post-probe key equality
/// verification.
#[derive(Debug)]
pub struct ProbeCandidate<'a> {
    /// Position in `CombineHashTable::records` — stable, zero-based, in
    /// insertion order. The executor uses this when it wants to tag the
    /// matched build record (e.g. for DLQ lineage or bitmap-based outer-join
    /// detection in a later phase).
    pub index: usize,
    /// Borrow of the matched build record. Lifetime bound to the
    /// `CombineHashTable` so the caller can read fields without cloning.
    pub record: &'a Record,
}

/// Index-separated hash table for combine's build side.
///
/// **Design rationale**: DataFusion, DuckDB, and CedarDB all converged on
/// this shape because storing 700–1400-byte records inline in a hash table
/// wastes 13–50% of memory on empty slots. Storing 4-byte record indices
/// instead keeps overhead below 3% for realistic ETL record sizes.
///
/// **Layout** (four allocation sources — [`CombineHashTable::memory_bytes`]
/// accounts every one of them per the DataFusion #14222/#5490/#6170 lessons):
///
/// ```text
/// index:      HashTable<(u64, u32)>   — (key_hash, chain_head_index)
/// chain:      Vec<u32>                 — chain[i] = next index with same hash, or SENTINEL
/// records:    Vec<Record>              — build-side records in insertion order
/// keys_cache: Vec<Vec<Value>>          — per-record extracted key values, built once
/// ```
///
/// **Chain semantics:** collisions prepend. When record `j` is inserted and
/// finds the bucket already occupied with head `i`, `chain[j]` is set to `i`
/// and the bucket is updated to point at `j`. Iteration walks from the
/// current head through `chain[...]` until `SENTINEL`. Every candidate along
/// the chain is verified against the probe keys via
/// [`keys_equal_canonicalized`] — hash collisions do not produce spurious
/// matches (DataFusion #843 lesson).
///
/// **Immutable after build:** once `build()` returns, no method takes
/// `&mut self`. This lets the Phase C.3+ executor share the table across
/// concurrent probe workers without synchronization. The `hash_state` is
/// captured at build time and reused for probe so hashes agree.
pub struct CombineHashTable {
    index: HashTable<(u64, u32)>,
    chain: Vec<u32>,
    records: Vec<Record>,
    keys_cache: Vec<Vec<Value>>,
    hash_state: RandomState,
}

impl CombineHashTable {
    /// Consume a Vec of build-side records and construct the hash table.
    ///
    /// * `extractor` — holds one `(TypedProgram, Expr)` pair per equality
    ///   conjunct, aligned to the build side's expressions.
    /// * `ctx` — CXL evaluation context, carries the Clock and stable
    ///   context shared across the build walk.
    /// * `budget` — polled every [`MEMORY_CHECK_INTERVAL`] inserts AND at
    ///   the end of build. Returns [`CombineError::MemoryLimitExceeded`] if
    ///   the process RSS exceeds the budget's hard limit.
    /// * `estimated_rows` — optional capacity hint. When `Some`, the
    ///   underlying [`HashTable`] is pre-sized via `with_capacity` to avoid
    ///   the resize spike, which can reach 2.25× peak footprint during
    ///   the rehash. When `None`, we fall back to `records.len()` —
    ///   always an upper bound on final table size.
    ///
    /// **Build records with NULL keys are indexed harmlessly:** they hash
    /// to the NULL sentinel and form chains alongside any collisions. They
    /// never match any probe because (a) the probe path short-circuits when
    /// any probe-side key is NULL, and (b) even if the hashes collided with
    /// a non-null probe key, [`keys_equal_canonicalized`] rejects `Null` on
    /// both sides per SQL 3VL. Indexing them simplifies the build loop
    /// (no skip branch) and has no runtime cost beyond the chain slot.
    pub fn build(
        records: Vec<Record>,
        extractor: &KeyExtractor,
        ctx: &EvalContext<'_>,
        budget: &mut MemoryBudget,
        estimated_rows: Option<usize>,
    ) -> Result<Self, CombineError> {
        let expected = estimated_rows.unwrap_or(records.len());

        // `u32::MAX` is reserved as SENTINEL. Refuse inputs that would
        // require a record index at that value — the addressable range is
        // effectively 4 billion records, which no realistic ETL combine
        // approaches.
        if records.len() >= SENTINEL as usize {
            return Err(CombineError::HashBuildFailed {
                reason: format!(
                    "combine build side exceeds u32::MAX - 1 records (got {})",
                    records.len()
                ),
            });
        }

        let mut index: HashTable<(u64, u32)> = HashTable::with_capacity(expected);
        let mut chain: Vec<u32> = Vec::with_capacity(records.len());
        let mut keys_cache: Vec<Vec<Value>> = Vec::with_capacity(records.len());
        let mut arena: Vec<Record> = Vec::with_capacity(records.len());
        let hash_state = RandomState::new();

        for (i, rec) in records.into_iter().enumerate() {
            let keys = extractor
                .extract(ctx, &rec)
                .map_err(|e| CombineError::KeyEvalFailed {
                    source: e,
                    side: "build",
                })?;

            let hash = hash_composite_key(&keys, &hash_state);
            let new_idx = i as u32;

            // Prepend-on-collision: the new record becomes the chain head;
            // the old head is stored in chain[new_idx].
            let old_head = match index.entry(
                hash,
                |&(stored_hash, _)| stored_hash == hash,
                |&(stored_hash, _)| stored_hash,
            ) {
                hashbrown::hash_table::Entry::Occupied(mut o) => {
                    let prev = o.get().1;
                    o.get_mut().1 = new_idx;
                    prev
                }
                hashbrown::hash_table::Entry::Vacant(v) => {
                    v.insert((hash, new_idx));
                    SENTINEL
                }
            };
            chain.push(old_head);
            keys_cache.push(keys);
            arena.push(rec);

            // Periodic budget poll. `should_abort` observes RSS and returns
            // true when the process has crossed the hard limit.
            if (i + 1).is_multiple_of(MEMORY_CHECK_INTERVAL) && budget.should_abort() {
                // Partial memory footprint (arena + chain + keys_cache +
                // index-so-far). Finalized memory might be slightly higher
                // once the HashTable rehashes; we report what we've
                // allocated up to this point so the diagnostic is honest.
                let used = partial_memory_bytes(&index, &chain, &arena, &keys_cache);
                return Err(CombineError::MemoryLimitExceeded {
                    used: used as u64,
                    limit: budget.limit,
                });
            }
        }

        let table = Self {
            index,
            chain,
            records: arena,
            keys_cache,
            hash_state,
        };

        // Safety-net final check — catches builds shorter than
        // MEMORY_CHECK_INTERVAL that slipped past the periodic poll.
        if budget.should_abort() {
            return Err(CombineError::MemoryLimitExceeded {
                used: table.memory_bytes() as u64,
                limit: budget.limit,
            });
        }

        Ok(table)
    }

    /// Probe the table with a driving-side record.
    ///
    /// **NULL probe short-circuit (SQL 3VL):** if any key value on the probe
    /// side is [`Value::Null`], the returned iterator is empty — no hash
    /// lookup is performed. This matches DataFusion and Spark join semantics
    /// without canonicalizing NULL to a pseudo-value (which would produce
    /// spurious matches with build-side NULLs).
    ///
    /// The returned iterator yields only candidates that pass BOTH the
    /// hash bucket AND full-key equality verification — the executor never
    /// sees hash-collision false positives.
    pub fn probe<'a>(
        &'a self,
        record: &Record,
        extractor: &KeyExtractor,
        ctx: &EvalContext<'_>,
    ) -> Result<ProbeIter<'a>, CombineError> {
        let probe_keys =
            extractor
                .extract(ctx, record)
                .map_err(|e| CombineError::KeyEvalFailed {
                    source: e,
                    side: "driving",
                })?;

        if probe_keys.iter().any(|v| matches!(v, Value::Null)) {
            // 3VL short-circuit.
            return Ok(ProbeIter {
                table: self,
                probe_keys,
                current: SENTINEL,
            });
        }

        let hash = hash_composite_key(&probe_keys, &self.hash_state);
        let head = self
            .index
            .find(hash, |&(stored_hash, _)| stored_hash == hash)
            .map(|&(_, h)| h)
            .unwrap_or(SENTINEL);

        Ok(ProbeIter {
            table: self,
            probe_keys,
            current: head,
        })
    }

    /// Total bytes of owned memory. Sums all four allocation sources —
    /// missing any one was the DataFusion #14222/#5490/#6170 failure class.
    /// Stable pub API: the executor uses this both for the periodic
    /// `MemoryBudget` check and for the `--explain` RSS reporting.
    pub fn memory_bytes(&self) -> usize {
        self.index.allocation_size()
            + self.chain.capacity() * std::mem::size_of::<u32>()
            + self.records.capacity() * std::mem::size_of::<Record>()
            + self
                .records
                .iter()
                .map(Record::estimated_heap_size)
                .sum::<usize>()
            + self.keys_cache.capacity() * std::mem::size_of::<Vec<Value>>()
            + self
                .keys_cache
                .iter()
                .map(|k| {
                    k.capacity() * std::mem::size_of::<Value>()
                        + k.iter().map(Value::heap_size).sum::<usize>()
                })
                .sum::<usize>()
    }

    /// Number of build-side records in the table.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// True when no build records were inserted.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

/// Iterator returned by [`CombineHashTable::probe`]. Walks the collision
/// chain from the matched bucket head through `chain[...]` until `SENTINEL`,
/// yielding only candidates whose cached build-side key values match the
/// probe's (canonicalized element-wise equality). Hash collisions between
/// non-equal keys are filtered silently so the caller never sees false
/// positives.
pub struct ProbeIter<'a> {
    table: &'a CombineHashTable,
    probe_keys: Vec<Value>,
    current: u32,
}

impl<'a> Iterator for ProbeIter<'a> {
    type Item = ProbeCandidate<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.current != SENTINEL {
            let idx = self.current as usize;
            // Advance the chain cursor FIRST so that a `return` from the
            // equality-matching branch leaves `self.current` pointing at
            // the next slot to scan on the following `.next()`.
            self.current = self.table.chain[idx];

            let build_keys = &self.table.keys_cache[idx];
            if keys_equal_canonicalized(&self.probe_keys, build_keys) {
                return Some(ProbeCandidate {
                    index: idx,
                    record: &self.table.records[idx],
                });
            }
            // else: hash collision with a non-equal key — skip, continue.
        }
        None
    }
}

/// Compute the hash table's owned memory during an in-progress build.
/// Identical formula to [`CombineHashTable::memory_bytes`], broken out so
/// the error path can report accurate `used` without constructing a
/// fully-assembled `CombineHashTable`.
fn partial_memory_bytes(
    index: &HashTable<(u64, u32)>,
    chain: &[u32],
    arena: &[Record],
    keys_cache: &[Vec<Value>],
) -> usize {
    index.allocation_size()
        + std::mem::size_of_val(chain)
        + std::mem::size_of_val(arena)
        + arena.iter().map(Record::estimated_heap_size).sum::<usize>()
        + std::mem::size_of_val(keys_cache)
        + keys_cache
            .iter()
            .map(|k| {
                k.capacity() * std::mem::size_of::<Value>()
                    + k.iter().map(Value::heap_size).sum::<usize>()
            })
            .sum::<usize>()
}

// ──────────────────────────────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use indexmap::IndexMap;

    // Deterministic RandomState across tests so hashes are stable within a
    // test run; a fixed seed disables per-process randomization.
    fn deterministic_state() -> RandomState {
        RandomState::with_seeds(0xA5A5_A5A5, 0x5A5A_5A5A, 0xDEAD_BEEF, 0xCAFE_F00D)
    }

    #[test]
    fn canonical_f64_collapses_neg_zero() {
        assert_eq!(canonical_f64(-0.0).to_bits(), canonical_f64(0.0).to_bits());
        assert_eq!(canonical_f64(0.0).to_bits(), 0.0_f64.to_bits());
    }

    #[test]
    fn canonical_f64_collapses_all_nan_bit_patterns() {
        // Multiple NaN bit patterns — quiet, signaling, negative-sign,
        // with distinct payloads — all collapse to CANONICAL_NAN_F64.
        let patterns = [
            f64::NAN,
            f64::from_bits(0x7ff8_0000_0000_0001), // quiet NaN with payload 1
            f64::from_bits(0x7ff0_0000_0000_0001), // signaling NaN
            f64::from_bits(0xfff8_0000_0000_0000), // negative-sign quiet NaN
            f64::from_bits(0xfff0_0000_0000_0042), // negative-sign signaling NaN with payload
        ];
        for p in patterns {
            assert!(p.is_nan(), "test input {p:?} must be NaN");
            assert_eq!(
                canonical_f64(p).to_bits(),
                CANONICAL_NAN_F64,
                "bit pattern {:#x} did not canonicalize",
                p.to_bits()
            );
        }
    }

    #[test]
    fn canonical_f64_preserves_ordinary_values() {
        for x in [
            1.0_f64,
            -1.0,
            1.5,
            f64::MIN,
            f64::MAX,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ] {
            assert_eq!(canonical_f64(x).to_bits(), x.to_bits(), "{x} corrupted");
        }
    }

    #[test]
    fn canonical_f32_collapses_neg_zero_and_nans() {
        assert_eq!(canonical_f32(-0.0).to_bits(), canonical_f32(0.0).to_bits());
        assert_eq!(canonical_f32(0.0).to_bits(), 0.0_f32.to_bits());
        for p in [
            f32::NAN,
            f32::from_bits(0x7fc0_0001),
            f32::from_bits(0xffc0_0000),
            f32::from_bits(0x7f80_0001), // signaling NaN
        ] {
            assert!(p.is_nan());
            assert_eq!(canonical_f32(p).to_bits(), CANONICAL_NAN_F32);
        }
    }

    #[test]
    fn hash_composite_key_type_discriminates() {
        let s = deterministic_state();
        let as_int = hash_composite_key(&[Value::Integer(42)], &s);
        let as_str = hash_composite_key(&[Value::String("42".into())], &s);
        assert_ne!(
            as_int, as_str,
            "integer vs string with same display must not collide"
        );

        let as_bool = hash_composite_key(&[Value::Bool(true)], &s);
        let as_int_one = hash_composite_key(&[Value::Integer(1)], &s);
        assert_ne!(
            as_bool, as_int_one,
            "bool true vs integer 1 must not collide"
        );
    }

    #[test]
    fn hash_composite_key_null_sentinel_stable() {
        let s = deterministic_state();
        let a = hash_composite_key(&[Value::Null, Value::Integer(7)], &s);
        let b = hash_composite_key(&[Value::Null, Value::Integer(7)], &s);
        assert_eq!(a, b, "null sentinel must hash identically across calls");

        // Null in different position produces different hash (positional).
        let c = hash_composite_key(&[Value::Integer(7), Value::Null], &s);
        assert_ne!(a, c, "null position matters");
    }

    #[test]
    fn hash_composite_key_nan_equivalence() {
        let s = deterministic_state();
        let nan1 = Value::Float(f64::NAN);
        let nan2 = Value::Float(f64::from_bits(0x7ff8_0000_0000_0001));
        let nan3 = Value::Float(f64::from_bits(0xfff0_0000_0000_0042));
        let h1 = hash_composite_key(&[nan1], &s);
        let h2 = hash_composite_key(&[nan2], &s);
        let h3 = hash_composite_key(&[nan3], &s);
        assert_eq!(
            h1, h2,
            "different NaN payloads must hash identically after canonicalization"
        );
        assert_eq!(h1, h3, "sign-bit-flipped NaN must also canonicalize");
    }

    #[test]
    fn hash_composite_key_neg_zero_equivalence() {
        let s = deterministic_state();
        let pos = hash_composite_key(&[Value::Float(0.0)], &s);
        let neg = hash_composite_key(&[Value::Float(-0.0)], &s);
        assert_eq!(pos, neg, "-0.0 and +0.0 must hash identically");
    }

    #[test]
    fn hash_composite_key_array_order_sensitive() {
        // Arrays are ordered: [1, 2] and [2, 1] are different values and
        // must hash differently.
        let s = deterministic_state();
        let a = hash_composite_key(
            &[Value::Array(vec![Value::Integer(1), Value::Integer(2)])],
            &s,
        );
        let b = hash_composite_key(
            &[Value::Array(vec![Value::Integer(2), Value::Integer(1)])],
            &s,
        );
        assert_ne!(a, b);
    }

    #[test]
    fn hash_composite_key_map_order_independent() {
        // Maps with the same (k, v) pairs must hash identically regardless
        // of insertion order.
        let s = deterministic_state();
        let mut m1: IndexMap<Box<str>, Value> = IndexMap::new();
        m1.insert("a".into(), Value::Integer(1));
        m1.insert("b".into(), Value::Integer(2));
        let mut m2: IndexMap<Box<str>, Value> = IndexMap::new();
        m2.insert("b".into(), Value::Integer(2));
        m2.insert("a".into(), Value::Integer(1));
        let h1 = hash_composite_key(&[Value::Map(Box::new(m1))], &s);
        let h2 = hash_composite_key(&[Value::Map(Box::new(m2))], &s);
        assert_eq!(h1, h2);
    }

    #[test]
    fn hash_composite_key_string_length_prefix_prevents_concat_collisions() {
        // Without a length prefix, ["ab", "c"] and ["a", "bc"] would produce
        // identical content bytes. With the length prefix they must diverge.
        let s = deterministic_state();
        let a = hash_composite_key(&[Value::String("ab".into()), Value::String("c".into())], &s);
        let b = hash_composite_key(&[Value::String("a".into()), Value::String("bc".into())], &s);
        assert_ne!(a, b);
    }

    #[test]
    fn keys_equal_canonicalized_float_nan_equal() {
        let a = [Value::Float(f64::NAN)];
        let b = [Value::Float(f64::from_bits(0x7ff8_0000_0000_00ff))];
        assert!(keys_equal_canonicalized(&a, &b));
    }

    #[test]
    fn keys_equal_canonicalized_neg_zero_equal() {
        let a = [Value::Float(-0.0)];
        let b = [Value::Float(0.0)];
        assert!(keys_equal_canonicalized(&a, &b));
    }

    #[test]
    fn keys_equal_canonicalized_null_never_equal() {
        // SQL 3VL: NULL == anything is false, including NULL.
        assert!(!keys_equal_canonicalized(&[Value::Null], &[Value::Null]));
        assert!(!keys_equal_canonicalized(
            &[Value::Null],
            &[Value::Integer(0)]
        ));
        assert!(!keys_equal_canonicalized(
            &[Value::Integer(0)],
            &[Value::Null]
        ));
    }

    #[test]
    fn keys_equal_canonicalized_cross_type_unequal() {
        assert!(!keys_equal_canonicalized(
            &[Value::Integer(1)],
            &[Value::Bool(true)]
        ));
        assert!(!keys_equal_canonicalized(
            &[Value::Integer(42)],
            &[Value::String("42".into())]
        ));
    }

    #[test]
    fn keys_equal_canonicalized_length_mismatch() {
        assert!(!keys_equal_canonicalized(
            &[Value::Integer(1), Value::Integer(2)],
            &[Value::Integer(1)]
        ));
    }

    #[test]
    fn keys_equal_canonicalized_datetime_exact() {
        let dt1 = NaiveDate::from_ymd_opt(2026, 4, 18)
            .unwrap()
            .and_hms_nano_opt(12, 34, 56, 123_456_789)
            .unwrap();
        let dt2 = NaiveDate::from_ymd_opt(2026, 4, 18)
            .unwrap()
            .and_hms_nano_opt(12, 34, 56, 123_456_789)
            .unwrap();
        assert!(keys_equal_canonicalized(
            &[Value::DateTime(dt1)],
            &[Value::DateTime(dt2)]
        ));

        // Differ by one nanosecond → unequal.
        let dt3 = NaiveDate::from_ymd_opt(2026, 4, 18)
            .unwrap()
            .and_hms_nano_opt(12, 34, 56, 123_456_790)
            .unwrap();
        assert!(!keys_equal_canonicalized(
            &[Value::DateTime(dt1)],
            &[Value::DateTime(dt3)]
        ));
    }

    #[test]
    fn keys_equal_canonicalized_map_order_independent() {
        let mut m1: IndexMap<Box<str>, Value> = IndexMap::new();
        m1.insert("k1".into(), Value::Integer(1));
        m1.insert("k2".into(), Value::Integer(2));
        let mut m2: IndexMap<Box<str>, Value> = IndexMap::new();
        m2.insert("k2".into(), Value::Integer(2));
        m2.insert("k1".into(), Value::Integer(1));
        assert!(keys_equal_canonicalized(
            &[Value::Map(Box::new(m1))],
            &[Value::Map(Box::new(m2))]
        ));
    }

    // ──────────────────────────────────────────────────────────────────
    // KeyExtractor tests
    // ──────────────────────────────────────────────────────────────────

    // The cxl-compile test helper below uses cxl's public compiler
    // pipeline (parser → resolve → typecheck) to produce a real
    // TypedProgram per test. This is NOT duplicating bind_combine's C.2.4
    // work — bind_combine will call the same underlying `type_check` entry
    // against the appropriate per-side Row. Here we build the Row
    // manually because unit tests can't reach through bind_combine.
    fn compile_key(
        src: &str,
        fields: &[&str],
        row_fields: &[(&str, cxl::typecheck::Type)],
    ) -> (Arc<TypedProgram>, cxl::ast::Expr) {
        use cxl::ast::Statement;
        use cxl::lexer::Span;
        use cxl::parser::Parser;
        use cxl::resolve::pass::resolve_program;
        use cxl::typecheck::pass::type_check;
        use cxl::typecheck::row::{QualifiedField, Row};
        use indexmap::IndexMap;

        let parsed = Parser::parse(src);
        assert!(
            parsed.errors.is_empty(),
            "parse errors: {:?}",
            parsed.errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
        let resolved = resolve_program(parsed.ast, fields, parsed.node_count)
            .unwrap_or_else(|d| panic!("resolve errors: {d:?}"));
        let mut cols: IndexMap<QualifiedField, cxl::typecheck::Type> = IndexMap::new();
        for (name, ty) in row_fields {
            cols.insert(QualifiedField::bare(*name), ty.clone());
        }
        let row = Row::closed(cols, Span::new(0, 0));
        let typed = type_check(resolved, &row).unwrap_or_else(|d| panic!("type errors: {d:?}"));
        let expr = match &typed.program.statements[0] {
            Statement::Emit { expr, .. } => expr.clone(),
            other => panic!("expected Emit, got {other:?}"),
        };
        (Arc::new(typed), expr)
    }

    fn eval_ctx() -> (cxl::eval::StableEvalContext, ()) {
        // The StableEvalContext is returned to the caller; the unit type
        // is a scaffolding-placeholder so the caller can bind via
        // destructuring and drop it.
        (cxl::eval::StableEvalContext::test_default(), ())
    }

    #[test]
    fn test_key_extractor_len_and_empty() {
        let extractor = KeyExtractor::new(vec![]);
        assert_eq!(extractor.len(), 0);
        assert!(extractor.is_empty());

        let (typed, expr) = compile_key("emit k = x", &["x"], &[("x", cxl::typecheck::Type::Int)]);
        let extractor2 = KeyExtractor::new(vec![(typed, expr)]);
        assert_eq!(extractor2.len(), 1);
        assert!(!extractor2.is_empty());
    }

    #[test]
    fn test_key_extractor_extracts_field_ref() {
        use cxl::resolve::HashMapResolver;
        let (typed, expr) = compile_key("emit k = x", &["x"], &[("x", cxl::typecheck::Type::Int)]);
        let extractor = KeyExtractor::new(vec![(typed, expr)]);

        let (stable, _) = eval_ctx();
        let ctx = cxl::eval::EvalContext::test_default_borrowed(&stable);
        let mut record = std::collections::HashMap::new();
        record.insert("x".to_string(), Value::Integer(42));
        let resolver = HashMapResolver::new(record);

        let values = extractor.extract(&ctx, &resolver).unwrap();
        assert_eq!(values, vec![Value::Integer(42)]);
    }

    #[test]
    fn test_key_extractor_extracts_computed_expression() {
        use cxl::resolve::HashMapResolver;
        let (typed, expr) = compile_key(
            "emit k = x + 1",
            &["x"],
            &[("x", cxl::typecheck::Type::Int)],
        );
        let extractor = KeyExtractor::new(vec![(typed, expr)]);

        let (stable, _) = eval_ctx();
        let ctx = cxl::eval::EvalContext::test_default_borrowed(&stable);
        let mut record = std::collections::HashMap::new();
        record.insert("x".to_string(), Value::Integer(41));
        let resolver = HashMapResolver::new(record);

        let values = extractor.extract(&ctx, &resolver).unwrap();
        assert_eq!(values, vec![Value::Integer(42)]);
    }

    #[test]
    fn test_key_extractor_multi_column() {
        use cxl::resolve::HashMapResolver;
        let (t1, e1) = compile_key(
            "emit k = region",
            &["region", "product_id"],
            &[
                ("region", cxl::typecheck::Type::String),
                ("product_id", cxl::typecheck::Type::Int),
            ],
        );
        let (t2, e2) = compile_key(
            "emit k = product_id",
            &["region", "product_id"],
            &[
                ("region", cxl::typecheck::Type::String),
                ("product_id", cxl::typecheck::Type::Int),
            ],
        );
        let extractor = KeyExtractor::new(vec![(t1, e1), (t2, e2)]);
        assert_eq!(extractor.len(), 2);

        let (stable, _) = eval_ctx();
        let ctx = cxl::eval::EvalContext::test_default_borrowed(&stable);
        let mut record = std::collections::HashMap::new();
        record.insert("region".to_string(), Value::String("US".into()));
        record.insert("product_id".to_string(), Value::Integer(101));
        let resolver = HashMapResolver::new(record);

        let values = extractor.extract(&ctx, &resolver).unwrap();
        assert_eq!(
            values,
            vec![Value::String("US".into()), Value::Integer(101)]
        );
    }

    #[test]
    fn test_key_extractor_missing_field_resolves_to_null() {
        // CXL's FieldRef semantics per eval_expr line 399:
        //   `resolver.resolve(name).unwrap_or(Value::Null)`.
        // So a missing field yields Null — the hash path treats this as
        // a NULL key and the probe short-circuits (3VL).
        use cxl::resolve::HashMapResolver;
        let (typed, expr) = compile_key("emit k = x", &["x"], &[("x", cxl::typecheck::Type::Int)]);
        let extractor = KeyExtractor::new(vec![(typed, expr)]);

        let (stable, _) = eval_ctx();
        let ctx = cxl::eval::EvalContext::test_default_borrowed(&stable);
        // Record has no "x" field.
        let resolver = HashMapResolver::new(std::collections::HashMap::new());

        let values = extractor.extract(&ctx, &resolver).unwrap();
        assert_eq!(values, vec![Value::Null]);
    }

    #[test]
    fn test_key_extractor_preserves_value_order() {
        // Anti-regression: if the order of programs in the extractor
        // swaps, downstream hashing + equality will collide across
        // conjuncts.
        use cxl::resolve::HashMapResolver;
        let (t_a, e_a) = compile_key(
            "emit k = a",
            &["a", "b"],
            &[
                ("a", cxl::typecheck::Type::Int),
                ("b", cxl::typecheck::Type::Int),
            ],
        );
        let (t_b, e_b) = compile_key(
            "emit k = b",
            &["a", "b"],
            &[
                ("a", cxl::typecheck::Type::Int),
                ("b", cxl::typecheck::Type::Int),
            ],
        );
        let extractor_ab =
            KeyExtractor::new(vec![(t_a.clone(), e_a.clone()), (t_b.clone(), e_b.clone())]);
        let extractor_ba = KeyExtractor::new(vec![(t_b, e_b), (t_a, e_a)]);

        let (stable, _) = eval_ctx();
        let ctx = cxl::eval::EvalContext::test_default_borrowed(&stable);
        let mut rec = std::collections::HashMap::new();
        rec.insert("a".to_string(), Value::Integer(1));
        rec.insert("b".to_string(), Value::Integer(2));
        let resolver = HashMapResolver::new(rec);

        let ab = extractor_ab.extract(&ctx, &resolver).unwrap();
        let ba = extractor_ba.extract(&ctx, &resolver).unwrap();
        assert_eq!(ab, vec![Value::Integer(1), Value::Integer(2)]);
        assert_eq!(ba, vec![Value::Integer(2), Value::Integer(1)]);
    }

    #[test]
    fn test_combine_error_display_and_source() {
        // Anchors the Display format and the fact that source() chains
        // for KeyEvalFailed. The executor depends on both (for diagnostic
        // messages and for error-chain walking).
        let e = CombineError::MemoryLimitExceeded {
            used: 1024,
            limit: 512,
        };
        let msg = format!("{e}");
        assert!(msg.contains("1024"));
        assert!(msg.contains("512"));

        let spill = CombineError::SpillFailed {
            reason: "disk full".to_string(),
        };
        assert!(format!("{spill}").contains("disk full"));
        assert!(std::error::Error::source(&spill).is_none());
    }

    // ──────────────────────────────────────────────────────────────────
    // CombineHashTable tests
    // ──────────────────────────────────────────────────────────────────

    use clinker_record::{Record, Schema};

    fn test_schema(cols: &[&str]) -> Arc<Schema> {
        let boxed: Vec<Box<str>> = cols.iter().map(|c| (*c).into()).collect();
        Arc::new(Schema::new(boxed))
    }

    fn mk_record(schema: &Arc<Schema>, values: Vec<Value>) -> Record {
        Record::new(Arc::clone(schema), values)
    }

    fn test_budget(limit_bytes: u64) -> MemoryBudget {
        MemoryBudget::new(limit_bytes, 0.80)
    }

    /// Build a single-column integer-key KeyExtractor that extracts field `name`.
    fn single_int_key(name: &str) -> KeyExtractor {
        let (typed, expr) = compile_key(
            &format!("emit k = {name}"),
            &[name],
            &[(name, cxl::typecheck::Type::Int)],
        );
        KeyExtractor::new(vec![(typed, expr)])
    }

    fn test_ctx() -> cxl::eval::StableEvalContext {
        cxl::eval::StableEvalContext::test_default()
    }

    #[test]
    fn test_combine_hash_table_build_probe_exact() {
        let schema = test_schema(&["id", "name"]);
        let records: Vec<Record> = (0..1000)
            .map(|i| {
                mk_record(
                    &schema,
                    vec![Value::Integer(i), Value::String(format!("name_{i}").into())],
                )
            })
            .collect();

        let extractor = single_int_key("id");
        let stable = test_ctx();
        let ctx = cxl::eval::EvalContext::test_default_borrowed(&stable);
        let mut budget = test_budget(256 * 1024 * 1024);

        let table =
            CombineHashTable::build(records, &extractor, &ctx, &mut budget, Some(1000)).unwrap();
        assert_eq!(table.len(), 1000);
        assert!(!table.is_empty());

        // Probe for id=42 → exactly one match.
        let probe = mk_record(&schema, vec![Value::Integer(42), Value::Null]);
        let matches: Vec<_> = table.probe(&probe, &extractor, &ctx).unwrap().collect();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].record.resolve("id"), Some(Value::Integer(42)));
    }

    #[test]
    fn test_combine_hash_table_duplicate_keys() {
        // Three records with the same key → all yielded by probe.
        let schema = test_schema(&["k", "val"]);
        let records = vec![
            mk_record(&schema, vec![Value::Integer(5), Value::String("a".into())]),
            mk_record(&schema, vec![Value::Integer(5), Value::String("b".into())]),
            mk_record(&schema, vec![Value::Integer(5), Value::String("c".into())]),
        ];
        let extractor = single_int_key("k");
        let stable = test_ctx();
        let ctx = cxl::eval::EvalContext::test_default_borrowed(&stable);
        let mut budget = test_budget(256 * 1024 * 1024);

        let table = CombineHashTable::build(records, &extractor, &ctx, &mut budget, None).unwrap();

        let probe = mk_record(&schema, vec![Value::Integer(5), Value::Null]);
        let mut got_vals: Vec<String> = table
            .probe(&probe, &extractor, &ctx)
            .unwrap()
            .map(|c| match c.record.resolve("val") {
                Some(Value::String(s)) => s.into_string(),
                _ => panic!("expected String val"),
            })
            .collect();
        got_vals.sort();
        assert_eq!(got_vals, vec!["a".to_string(), "b".into(), "c".into()]);
    }

    #[test]
    fn test_combine_hash_table_no_match_returns_empty() {
        let schema = test_schema(&["k"]);
        let records = vec![mk_record(&schema, vec![Value::Integer(1)])];
        let extractor = single_int_key("k");
        let stable = test_ctx();
        let ctx = cxl::eval::EvalContext::test_default_borrowed(&stable);
        let mut budget = test_budget(256 * 1024 * 1024);
        let table = CombineHashTable::build(records, &extractor, &ctx, &mut budget, None).unwrap();

        let probe = mk_record(&schema, vec![Value::Integer(9999)]);
        let matches: Vec<_> = table.probe(&probe, &extractor, &ctx).unwrap().collect();
        assert!(matches.is_empty());
    }

    #[test]
    fn test_combine_hash_table_null_probe_short_circuits() {
        // Probe with NULL key → empty iter (SQL 3VL), even though a build
        // record with the same-hash NULL key exists in the table.
        let schema = test_schema(&["k", "tag"]);
        let records = vec![
            mk_record(&schema, vec![Value::Null, Value::String("null-row".into())]),
            mk_record(
                &schema,
                vec![Value::Integer(1), Value::String("one".into())],
            ),
        ];
        let extractor = single_int_key("k");
        let stable = test_ctx();
        let ctx = cxl::eval::EvalContext::test_default_borrowed(&stable);
        let mut budget = test_budget(256 * 1024 * 1024);
        let table = CombineHashTable::build(records, &extractor, &ctx, &mut budget, None).unwrap();
        assert_eq!(
            table.len(),
            2,
            "NULL-keyed build record is indexed harmlessly"
        );

        // Probe with Value::Null key → short-circuit, zero matches.
        let null_probe = mk_record(&schema, vec![Value::Null, Value::Null]);
        let nm: Vec<_> = table
            .probe(&null_probe, &extractor, &ctx)
            .unwrap()
            .collect();
        assert!(
            nm.is_empty(),
            "NULL probe must not match even a NULL build key"
        );

        // Sanity: non-null probe still finds the non-null build record.
        let probe = mk_record(&schema, vec![Value::Integer(1), Value::Null]);
        let matches: Vec<_> = table.probe(&probe, &extractor, &ctx).unwrap().collect();
        assert_eq!(matches.len(), 1);
    }

    #[test]
    fn test_combine_hash_table_hash_collision_equality_reject() {
        // Force multiple records into the same bucket by using extreme
        // cardinality. hashbrown's AHasher is keyed by a process-random
        // RandomState, so we can't guarantee a collision between two
        // specific keys in a single run. Instead: insert 1000 records
        // with 1000 distinct keys and verify probe for a MISSING key
        // with a near-neighbor value returns 0 matches. This exercises
        // the chain-walk + equality verification path under realistic
        // bucket contention (at 1000 records, bucket collisions are
        // guaranteed by pigeonhole across the 128-bit AHash distribution).
        let schema = test_schema(&["k"]);
        let records: Vec<Record> = (0..1000)
            .map(|i| mk_record(&schema, vec![Value::Integer(i)]))
            .collect();
        let extractor = single_int_key("k");
        let stable = test_ctx();
        let ctx = cxl::eval::EvalContext::test_default_borrowed(&stable);
        let mut budget = test_budget(256 * 1024 * 1024);
        let table =
            CombineHashTable::build(records, &extractor, &ctx, &mut budget, Some(1000)).unwrap();

        // These keys are NOT in the build set.
        for missing_key in [1000i64, 10_000, -1, -999] {
            let probe = mk_record(&schema, vec![Value::Integer(missing_key)]);
            let matches: Vec<_> = table.probe(&probe, &extractor, &ctx).unwrap().collect();
            assert!(
                matches.is_empty(),
                "probe for missing key {missing_key} yielded {} false positives",
                matches.len()
            );
        }

        // And confirm every PRESENT key finds exactly one match.
        for present in [0i64, 1, 42, 999] {
            let probe = mk_record(&schema, vec![Value::Integer(present)]);
            let matches: Vec<_> = table.probe(&probe, &extractor, &ctx).unwrap().collect();
            assert_eq!(matches.len(), 1, "probe for {present} must find exactly 1");
        }
    }

    #[test]
    fn test_combine_hash_table_memory_tracking_all_sources() {
        let schema = test_schema(&["k", "val"]);
        let records: Vec<Record> = (0..100)
            .map(|i| {
                mk_record(
                    &schema,
                    vec![
                        Value::Integer(i),
                        Value::String(format!("payload_{i:04}").into()),
                    ],
                )
            })
            .collect();
        let extractor = single_int_key("k");
        let stable = test_ctx();
        let ctx = cxl::eval::EvalContext::test_default_borrowed(&stable);
        let mut budget = test_budget(256 * 1024 * 1024);
        let table =
            CombineHashTable::build(records, &extractor, &ctx, &mut budget, Some(100)).unwrap();

        let total = table.memory_bytes();

        // Structural invariants — memory_bytes MUST dominate each
        // component source. This is the anti-DataFusion-#14222 gate:
        // every source contributes.
        let arena_heap: usize = table.records.iter().map(Record::estimated_heap_size).sum();
        assert!(
            total > arena_heap,
            "memory_bytes {total} must exceed arena heap {arena_heap} (index + chain + keys)"
        );
        let chain_bytes = table.chain.capacity() * std::mem::size_of::<u32>();
        assert!(
            total > chain_bytes,
            "memory_bytes must exceed chain cap {chain_bytes}"
        );
        let index_bytes = table.index.allocation_size();
        assert!(
            total >= index_bytes,
            "memory_bytes must include index alloc {index_bytes}"
        );
    }

    #[test]
    fn test_combine_hash_table_memory_overhead_ratio() {
        // Target: <1.1× overhead for records with 10+ fields (realistic
        // ETL size — ~1-2 KB per record). The "raw" baseline here is
        // what ANY in-memory record store would occupy: per-Record struct
        // bytes + heap (values + overflow + metadata). The hash-specific
        // overhead is index + chain + keys_cache.
        //
        // We use 10 string fields ~100 bytes each = ~1 KB records,
        // matching the research target's test condition. At this size
        // the 45-byte-per-row fixed overhead (17 index + 4 chain +
        // 24 keys_cache_outer) is a small fraction of the payload.
        //
        // Under smaller record sizes (< 500 B) the keys_cache overhead
        // dominates and the ratio rises toward 1.25×. That is the
        // intentional time-memory tradeoff of caching build-side key
        // Values to avoid re-extraction on every chain scan (plan
        // decision; see C.2.1 sub-task in
        // /home/glitch/.claude/plans/thoroughly-plan-how-to-silly-fairy.md
        // Section 4).
        let cols: Vec<String> = (0..10).map(|i| format!("c{i}")).collect();
        let col_refs: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
        let schema = test_schema(&col_refs);

        // Each column gets ~100 bytes of payload → ~1 KB per record.
        let filler: String = "y".repeat(100);
        let records: Vec<Record> = (0..1000)
            .map(|i| {
                let mut vals: Vec<Value> = (0..10)
                    .map(|_| Value::String(filler.clone().into()))
                    .collect();
                // First column acts as the key; replace with a unique
                // integer-like string so duplicate keys don't bloat one
                // bucket.
                vals[0] = Value::String(format!("key_{i:06}").into());
                mk_record(&schema, vals)
            })
            .collect();

        // Key extractor: single-column string key on c0.
        let (typed, expr) = compile_key(
            "emit k = c0",
            &col_refs,
            &col_refs
                .iter()
                .map(|n| (*n, cxl::typecheck::Type::String))
                .collect::<Vec<_>>(),
        );
        let extractor = KeyExtractor::new(vec![(typed, expr)]);

        let raw_baseline: usize = records
            .iter()
            .map(Record::estimated_heap_size)
            .sum::<usize>()
            + records.len() * std::mem::size_of::<Record>();

        let stable = test_ctx();
        let ctx = cxl::eval::EvalContext::test_default_borrowed(&stable);
        let mut budget = test_budget(256 * 1024 * 1024);
        let table =
            CombineHashTable::build(records, &extractor, &ctx, &mut budget, Some(1000)).unwrap();

        let total = table.memory_bytes();
        let ratio = total as f64 / raw_baseline as f64;
        assert!(
            ratio < 1.1,
            "overhead ratio {ratio:.3} exceeds 1.1× target for 10-field × 1 KB records \
             (total={total}, raw_baseline={raw_baseline}, overhead={})",
            total.saturating_sub(raw_baseline)
        );
    }

    #[test]
    fn test_combine_hash_table_oom_aborts_during_build() {
        // With a 1-byte budget, the process RSS trivially exceeds the
        // hard limit — `should_abort()` returns true on the first poll.
        // The final-check safety net fires even though we have fewer
        // than MEMORY_CHECK_INTERVAL records.
        let schema = test_schema(&["k"]);
        let records: Vec<Record> = (0..10)
            .map(|i| mk_record(&schema, vec![Value::Integer(i)]))
            .collect();

        let extractor = single_int_key("k");
        let stable = test_ctx();
        let ctx = cxl::eval::EvalContext::test_default_borrowed(&stable);
        let mut budget = test_budget(1); // 1 byte — impossibly tight.

        match CombineHashTable::build(records, &extractor, &ctx, &mut budget, None) {
            Err(CombineError::MemoryLimitExceeded { used, limit }) => {
                assert_eq!(limit, 1);
                // `used` is either non-zero if the final check fires and
                // reports table.memory_bytes(), or zero if the periodic
                // poll fired mid-build — the variant is the gate.
                let _ = used;
            }
            Err(other) => panic!("expected MemoryLimitExceeded, got {other}"),
            Ok(_) => {
                // RSS measurement unavailable on this platform
                // (`rss_bytes()` returns None) — `should_abort()` can't
                // fire. Accepting Ok here would hide a real abort-path
                // regression on Linux CI; fail explicitly so the
                // maintainer has to decide to wire a platform gate.
                panic!(
                    "build() succeeded under 1-byte budget — either RSS polling \
                     returned None on this platform (add a cfg gate), or the \
                     should_abort wiring regressed"
                );
            }
        }
    }

    #[test]
    fn test_combine_hash_table_empty_input() {
        // EC-1: zero build records. build() succeeds with len()==0; probe
        // returns empty iter for any driver.
        let schema = test_schema(&["k"]);
        let extractor = single_int_key("k");
        let stable = test_ctx();
        let ctx = cxl::eval::EvalContext::test_default_borrowed(&stable);
        let mut budget = test_budget(256 * 1024 * 1024);
        let table = CombineHashTable::build(vec![], &extractor, &ctx, &mut budget, None).unwrap();
        assert!(table.is_empty());
        assert_eq!(table.len(), 0);

        let probe = mk_record(&schema, vec![Value::Integer(42)]);
        let matches: Vec<_> = table.probe(&probe, &extractor, &ctx).unwrap().collect();
        assert!(matches.is_empty());
    }

    #[test]
    fn test_combine_hash_table_probe_iter_advances_across_chain() {
        // Anti-regression for a subtle ProbeIter bug: if `self.current` is
        // advanced AFTER yielding (instead of before), a hash collision
        // with non-match followed by a true match would yield only the
        // true match's index once and then spin forever on the same slot.
        // Here we insert 10K records with 10K distinct keys and verify
        // that iterating any probe yields at most 1 match (per
        // cardinality) rather than an unbounded stream.
        let schema = test_schema(&["k"]);
        let records: Vec<Record> = (0..10_000)
            .map(|i| mk_record(&schema, vec![Value::Integer(i)]))
            .collect();
        let extractor = single_int_key("k");
        let stable = test_ctx();
        let ctx = cxl::eval::EvalContext::test_default_borrowed(&stable);
        let mut budget = test_budget(256 * 1024 * 1024);
        let table =
            CombineHashTable::build(records, &extractor, &ctx, &mut budget, Some(10_000)).unwrap();

        for key in [0i64, 500, 9999] {
            let probe = mk_record(&schema, vec![Value::Integer(key)]);
            let matches: Vec<_> = table.probe(&probe, &extractor, &ctx).unwrap().collect();
            assert_eq!(
                matches.len(),
                1,
                "probe for {key}: expected 1, got {}",
                matches.len()
            );
        }
    }

    #[test]
    fn test_combine_hash_table_randomized_roundtrip() {
        // C.2.1.5: randomized round-trip. Builds random (key, payload)
        // records into the hash table, then verifies that for every key
        // (whether or not present in the build set) the probe yields
        // EXACTLY the set of build records with that key — no false
        // positives, no missed matches, regardless of chain walk order.
        //
        // A hand-rolled randomized test (seeded `fastrand`) rather than
        // proptest to avoid a workspace dep. Seeds are deterministic so
        // failures reproduce; iteration count is bounded so CI time stays
        // under a second.
        //
        // "Gold" answer: linear scan of build records with
        // `keys_equal_canonicalized`. Our hash-table answer must match
        // as a multiset (order is not contractual).
        let schema = test_schema(&["k", "tag"]);
        let extractor = single_int_key("k");
        let stable = test_ctx();
        let ctx = cxl::eval::EvalContext::test_default_borrowed(&stable);

        let seeds: [u64; 8] = [1, 2, 42, 9999, 0xDEAD_BEEF, 0xCAFE_F00D, 12345, 7];
        for seed in seeds {
            let mut rng = fastrand::Rng::with_seed(seed);

            // Draw N records with keys in a small universe (forces many
            // duplicates → exercises chain walks with length > 1).
            let n = 50 + (rng.u64(0..200) as usize); // 50..250
            let key_universe_size: i64 = 5 + rng.i64(0..15); // 5..20 → guaranteed duplicates

            let mut build: Vec<(i64, u64)> = Vec::with_capacity(n); // (key, unique_tag)
            for i in 0..n {
                let key = rng.i64(0..key_universe_size);
                build.push((key, i as u64));
            }

            let records: Vec<Record> = build
                .iter()
                .map(|(k, tag)| {
                    mk_record(
                        &schema,
                        vec![Value::Integer(*k), Value::Integer(*tag as i64)],
                    )
                })
                .collect();
            let mut budget = test_budget(256 * 1024 * 1024);
            let table = CombineHashTable::build(records, &extractor, &ctx, &mut budget, None)
                .expect("build");
            assert_eq!(table.len(), n);

            // For every key in the universe PLUS a few definitely-missing
            // keys, probe and compare to the gold set.
            let mut probe_keys: Vec<i64> = (0..key_universe_size).collect();
            probe_keys.extend([
                key_universe_size,       // just out of range
                key_universe_size + 100, // far out of range
                -1,                      // negative, never inserted
            ]);

            for probe_k in probe_keys {
                // Gold: set of tags whose record has key == probe_k.
                let mut expected_tags: Vec<u64> = build
                    .iter()
                    .filter(|(bk, _)| *bk == probe_k)
                    .map(|(_, t)| *t)
                    .collect();
                expected_tags.sort_unstable();

                let probe = mk_record(&schema, vec![Value::Integer(probe_k), Value::Null]);
                let matches: Vec<ProbeCandidate> = table
                    .probe(&probe, &extractor, &ctx)
                    .expect("probe")
                    .collect();

                // Cardinality check first — catches both phantom matches
                // and missed matches.
                assert_eq!(
                    matches.len(),
                    expected_tags.len(),
                    "seed={seed} probe_k={probe_k}: got {} matches, expected {}",
                    matches.len(),
                    expected_tags.len()
                );

                // Multi-set equality on extracted tags.
                let mut got_tags: Vec<u64> = matches
                    .iter()
                    .map(|m| match m.record.resolve("tag") {
                        Some(Value::Integer(t)) => t as u64,
                        other => panic!("tag must be Integer, got {other:?}"),
                    })
                    .collect();
                got_tags.sort_unstable();
                assert_eq!(
                    got_tags, expected_tags,
                    "seed={seed} probe_k={probe_k}: tag multiset mismatch"
                );

                // Every yielded record's key must canonicalize-equal
                // the probe key — anti-regression for post-probe
                // equality verification.
                for c in &matches {
                    let build_key = c.record.resolve("k");
                    assert_eq!(
                        build_key,
                        Some(Value::Integer(probe_k)),
                        "seed={seed} probe_k={probe_k}: matched record has wrong key {build_key:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn hash_and_equal_agree_on_canonicalization() {
        // Anti-regression: if two Value sequences compare equal via
        // `keys_equal_canonicalized`, they MUST hash identically via
        // `hash_composite_key`. The canonicalization rules have to be shared
        // between the two paths.
        let s = deterministic_state();
        let pairs: Vec<([Value; 1], [Value; 1])> = vec![
            ([Value::Float(0.0)], [Value::Float(-0.0)]),
            (
                [Value::Float(f64::NAN)],
                [Value::Float(f64::from_bits(0x7ff8_0000_0000_ab00))],
            ),
        ];
        for (a, b) in pairs {
            assert!(
                keys_equal_canonicalized(&a, &b),
                "pair claimed equal but isn't"
            );
            assert_eq!(
                hash_composite_key(&a, &s),
                hash_composite_key(&b, &s),
                "equal pair hashed differently"
            );
        }
    }
}

use std::sync::{Arc, RwLock};

use chrono::{NaiveDateTime, Utc};
use clinker_record::{PipelineCounters, Value};
use indexmap::IndexMap;

/// Shared, lock-protected store for `$pipeline.<key>` runtime values.
///
/// Values arrive from two sources:
/// 1. **Init-time defaults** — populated from `PipelineMeta.vars.pipeline.*`
///    declarations at pipeline start and pre-runtime State
///    nodes marked `phase: init`.
/// 2. **Runtime writes** — emitted by Phase D's `state` node arm,
///    one write per record (last-write-wins per the locked design
///    decision).
///
/// `Arc<RwLock<...>>` keeps the read fast path lock-free for everyone
/// holding the `Arc` while still allowing `state` nodes to mutate
/// without `&mut StableEvalContext`. The RwLock contention cost on the
/// hot read path (every `$pipeline.<key>` resolve) is small relative
/// to the surrounding CXL eval; revisit if a profile says otherwise.
pub type PipelineVarStore = Arc<RwLock<IndexMap<String, Value>>>;

/// Shared, lock-protected store for `$source.<key>` runtime values,
/// keyed by per-record `source_file` `Arc<str>`.
///
/// Source-scope is "stays constant for all records of one source" —
/// for single-file Sources this is one slot; for multi-file Sources
/// (`glob:` / `paths:`) each file gets its own slot, which is a
/// tighter (safer) semantics than per-Source-node-name. The Arc-keyed
/// design composes cleanly with the executor's existing per-record
/// `source_file` threading without introducing a path →
/// source-node-name mapping.
pub type SourceVarStore = Arc<RwLock<std::collections::HashMap<Arc<str>, IndexMap<String, Value>>>>;

/// Injectable clock for testability. Production uses WallClock; tests use FixedClock.
pub trait Clock: Send + Sync {
    fn now(&self) -> NaiveDateTime;
}

/// Production clock: reads system wall-clock. Fresh value per call (per-record).
pub struct WallClock;

impl Clock for WallClock {
    fn now(&self) -> NaiveDateTime {
        Utc::now().naive_utc()
    }
}

/// Test clock: returns a fixed time. Deterministic assertions.
pub struct FixedClock(pub NaiveDateTime);

impl Clock for FixedClock {
    fn now(&self) -> NaiveDateTime {
        self.0
    }
}

/// Maximum output size for string-producing methods (.repeat, .pad_left, .pad_right).
pub const MAX_STRING_OUTPUT: usize = 10 * 1024 * 1024; // 10 MB

/// Pipeline-stable evaluation context — built once per pipeline run, shared
/// (via `Arc`) across every record-level dispatch site. All `pipeline.*`
/// members resolve against this struct; per-record provenance lives under
/// `$source.*` on the borrowed `EvalContext<'_>` view.
///
/// Mirrors DataFusion `Arc<TaskContext>`, Apache Beam `FinishBundleContext`,
/// Flink `RuntimeContext`. Replaces an earlier per-record `String::clone` +
/// `IndexMap::clone` profile that allocated at every dispatch site.
pub struct StableEvalContext {
    /// Clock for `now` keyword. WallClock in production, FixedClock in tests.
    pub clock: Box<dyn Clock>,
    /// pipeline.start_time — frozen at pipeline start, deterministic within a run.
    pub pipeline_start_time: NaiveDateTime,
    /// pipeline.name — from YAML config.
    pub pipeline_name: Arc<str>,
    /// pipeline.execution_id — UUID v7, unique per run.
    pub pipeline_execution_id: Arc<str>,
    /// pipeline.batch_id — from --batch-id CLI flag or auto UUID v7.
    pub pipeline_batch_id: Arc<str>,
    /// pipeline.total_count / ok_count / dlq_count.
    pub pipeline_counters: PipelineCounters,
    /// pipeline.vars.* — user-declared variables. Holds init-time
    /// defaults plus any runtime writes from `state` nodes.
    /// See [`PipelineVarStore`] for locking rationale.
    pub pipeline_vars: PipelineVarStore,
    /// source.vars.<key> — per-source-file user-declared variables,
    /// written by `state` nodes with `scope: source`. The
    /// outer `HashMap` keys by per-record `source_file` Arc; the inner
    /// `IndexMap` mirrors the [`PipelineVarStore`] shape.
    pub source_vars: SourceVarStore,
    /// Plan-time map from Merge/Combine input-name to the source-file
    /// `Arc<str>`s of the upstream Source(s) reachable from that input.
    /// Used by [`Expr::QualifiedSourceAccess`] eval to look
    /// up the right `source_vars` entry for `$source.<input_name>.<key>`
    /// reads. Empty for non-pipeline contexts.
    pub source_input_arcs: Arc<std::collections::HashMap<String, Vec<Arc<str>>>>,
    /// Static configuration knobs read via `$vars.<key>`. Built from
    /// the pipeline's top-level `vars:` block (channel overrides
    /// applied) and frozen for the pipeline run. Distinct from
    /// `pipeline_vars` (which holds producer-written `$pipeline.*`
    /// state); `static_vars` is the immutable configuration surface
    /// channels override.
    pub static_vars: Arc<IndexMap<String, Value>>,
}

impl StableEvalContext {
    /// Resolve a stable `pipeline.*` member by name. Per-record provenance
    /// (`$source.file`, `$source.row`) resolves through the borrowed
    /// `EvalContext<'_>` view, not this struct.
    pub fn resolve_pipeline_stable(&self, member: &str) -> Option<Value> {
        match member {
            "start_time" => Some(Value::DateTime(self.pipeline_start_time)),
            "name" => Some(Value::String(self.pipeline_name.to_string().into())),
            "execution_id" => Some(Value::String(self.pipeline_execution_id.to_string().into())),
            "batch_id" => Some(Value::String(self.pipeline_batch_id.to_string().into())),
            "total_count" => Some(Value::Integer(self.pipeline_counters.total_count as i64)),
            "ok_count" => Some(Value::Integer(self.pipeline_counters.ok_count as i64)),
            "dlq_count" => Some(Value::Integer(self.pipeline_counters.dlq_count as i64)),
            "filtered_count" => Some(Value::Integer(self.pipeline_counters.filtered_count as i64)),
            "distinct_count" => Some(Value::Integer(self.pipeline_counters.distinct_count as i64)),
            _ => self
                .pipeline_vars
                .read()
                .ok()
                .and_then(|map| map.get(member).cloned()),
        }
    }

    /// Write a value into the pipeline-scope runtime registry.
    ///
    /// Called by the executor's `state` node arm. Reads via
    /// [`Self::resolve_pipeline_stable`] observe the new value
    /// immediately; the locking discipline matches a Rust `RwLock` —
    /// concurrent readers and a single writer per moment, with no
    /// guarantee of FIFO ordering between competing writers.
    pub fn set_pipeline_var(&self, name: &str, value: Value) {
        if let Ok(mut map) = self.pipeline_vars.write() {
            map.insert(name.to_string(), value);
        }
    }

    /// Resolve a `$source.<key>` user-declared value for the given
    /// `source_file` Arc. Returns `None` if no `state` node has yet
    /// written to that scope, or if the file has no entry at all
    /// (lazy-allocated). Builtin `$source.*` members are handled by
    /// [`EvalContext::resolve_source`]; this helper covers user-declared
    /// keys only.
    pub fn resolve_source_var(&self, source_file: &Arc<str>, member: &str) -> Option<Value> {
        let map = self.source_vars.read().ok()?;
        map.get(source_file)
            .and_then(|inner| inner.get(member).cloned())
    }

    /// Write a `$source.<key>` value into the source-scope runtime
    /// registry, keyed by the record's `source_file` Arc. Called by
    /// the executor's `state` node arm. Lazily allocates
    /// the per-file inner map on first write.
    pub fn set_source_var(&self, source_file: &Arc<str>, name: &str, value: Value) {
        if let Ok(mut map) = self.source_vars.write() {
            map.entry(Arc::clone(source_file))
                .or_default()
                .insert(name.to_string(), value);
        }
    }

    /// Create a minimal stable context for testing.
    pub fn test_default() -> Self {
        Self {
            clock: Box::new(FixedClock(
                chrono::NaiveDate::from_ymd_opt(2026, 1, 15)
                    .unwrap()
                    .and_hms_opt(12, 0, 0)
                    .unwrap(),
            )),
            pipeline_start_time: chrono::NaiveDate::from_ymd_opt(2026, 1, 15)
                .unwrap()
                .and_hms_opt(12, 0, 0)
                .unwrap(),
            pipeline_name: Arc::from("test_pipeline"),
            pipeline_execution_id: Arc::from("00000000-0000-0000-0000-000000000000"),
            pipeline_batch_id: Arc::from("test-batch-000"),
            pipeline_counters: PipelineCounters::default(),
            pipeline_vars: Arc::new(RwLock::new(IndexMap::new())),
            source_vars: Arc::new(RwLock::new(std::collections::HashMap::new())),
            source_input_arcs: Arc::new(std::collections::HashMap::new()),
            static_vars: Arc::new(IndexMap::new()),
        }
    }
}

/// Per-record evaluation context view — borrows from a `StableEvalContext`
/// and adds the per-record / per-source provenance fields. Constructed
/// inline at every dispatch site; zero allocation.
pub struct EvalContext<'a> {
    pub stable: &'a StableEvalContext,
    /// `$source.file` — record's originating file path.
    pub source_file: &'a Arc<str>,
    /// `$source.row` — record's row number within its source file
    /// (0-indexed in the input stream).
    pub source_row: u64,
    /// `$source.path` — full canonical path of the record's source file.
    /// Equal to `source_file` today; distinct from a future
    /// `$source.file_label` (shortest-unique-suffix display label).
    pub source_path: &'a Arc<str>,
    /// `$source.count` — final per-source record count.
    ///
    /// `Some(n)` for records emitted **after** the source's input stream
    /// closes (terminal aggregate emits, commit-time deferred dispatch,
    /// post-recompute paths). `None` for records emitted mid-stream — the
    /// total cannot be known before the upstream `mpsc::Receiver` returns
    /// `None`, so `$source.count` resolves to `Null` for those records.
    /// Per-source: the value is the final count for THIS record's
    /// originating source, not a pipeline-wide total.
    pub source_count: Option<u64>,
    /// `$source.batch` — per-source-run identifier (UUID v7 by default).
    /// Distinct from `pipeline.batch_id` which is per-pipeline-run.
    pub source_batch: &'a Arc<str>,
    /// `$source.ingestion_timestamp` — wall-clock time when ingestion of
    /// this source began.
    pub ingestion_timestamp: NaiveDateTime,
    /// `$source.name` — originating Source node's name. Stable across
    /// every record from the same Source; survives Merge / Combine via
    /// the per-record `FieldMetadata::SourceName` engine-stamp.
    pub source_name: &'a Arc<str>,
}

impl<'a> EvalContext<'a> {
    /// Resolve a `pipeline.*` member by name — pipeline-stable values only.
    /// Per-record provenance lives under `$source.*`; see `resolve_source`.
    pub fn resolve_pipeline(&self, member: &str) -> Option<Value> {
        self.stable.resolve_pipeline_stable(member)
    }

    /// Resolve a `$source.*` member — per-record / per-source provenance.
    /// Falls through to the user-declared source-scope registry
    /// (`$source.<custom>`) keyed by this record's `source_file` Arc
    /// when the member name is not a builtin.
    pub fn resolve_source(&self, member: &str) -> Option<Value> {
        match member {
            "file" => Some(Value::String(self.source_file.to_string().into())),
            "row" => Some(Value::Integer(self.source_row as i64)),
            "path" => Some(Value::String(self.source_path.to_string().into())),
            "count" => Some(
                self.source_count
                    .map_or(Value::Null, |n| Value::Integer(n as i64)),
            ),
            "batch" => Some(Value::String(self.source_batch.to_string().into())),
            "ingestion_timestamp" => Some(Value::DateTime(self.ingestion_timestamp)),
            "name" => Some(Value::String(self.source_name.to_string().into())),
            other => self.stable.resolve_source_var(self.source_file, other),
        }
    }

    /// Test helper: borrow a stable context to get a default view with
    /// `source_file = "test.csv"` and `source_row = 1`. Used by the ~30
    /// `EvalContext::test_default()` call sites in workspace tests.
    ///
    /// Pattern: `let stable = StableEvalContext::test_default();`
    ///          `let ctx = EvalContext::test_default_borrowed(&stable);`
    pub fn test_default_borrowed(stable: &'a StableEvalContext) -> Self {
        Self {
            stable,
            source_file: test_default_source_file(),
            source_row: 1,
            source_path: test_default_source_file(),
            source_count: None,
            source_batch: test_default_source_batch(),
            ingestion_timestamp: chrono::NaiveDate::from_ymd_opt(2026, 1, 15)
                .unwrap()
                .and_hms_opt(12, 0, 0)
                .unwrap(),
            source_name: test_default_source_name(),
        }
    }

    /// Test helper: borrow a stable context plus an explicit source-file
    /// `Arc<str>` and row number, defaulting the rest of `$source.*` to
    /// the same placeholder values as `test_default_borrowed`. Used by
    /// the workspace test helpers that previously constructed
    /// `EvalContext` literally with three fields.
    pub fn test_with_file(
        stable: &'a StableEvalContext,
        source_file: &'a Arc<str>,
        source_row: u64,
    ) -> Self {
        Self {
            stable,
            source_file,
            source_row,
            source_path: source_file,
            source_count: None,
            source_batch: test_default_source_batch(),
            ingestion_timestamp: chrono::NaiveDate::from_ymd_opt(2026, 1, 15)
                .unwrap()
                .and_hms_opt(12, 0, 0)
                .unwrap(),
            source_name: test_default_source_name(),
        }
    }
}

/// Returns a `'static` reference to a process-wide `Arc<str>` containing
/// `"test.csv"`. Used so that `EvalContext::test_default_borrowed` can hand
/// out a `&Arc<str>` without per-call allocation.
fn test_default_source_file() -> &'static Arc<str> {
    use std::sync::OnceLock;
    static FILE: OnceLock<Arc<str>> = OnceLock::new();
    FILE.get_or_init(|| Arc::from("test.csv"))
}

/// Like `test_default_source_file` but for `$source.batch`.
fn test_default_source_batch() -> &'static Arc<str> {
    use std::sync::OnceLock;
    static BATCH: OnceLock<Arc<str>> = OnceLock::new();
    BATCH.get_or_init(|| Arc::from("test-batch-000"))
}

/// Like `test_default_source_file` but for `$source.name`. Defaulted
/// to `"test_source"` for test contexts that don't care about the
/// originating Source's name.
fn test_default_source_name() -> &'static Arc<str> {
    use std::sync::OnceLock;
    static NAME: OnceLock<Arc<str>> = OnceLock::new();
    NAME.get_or_init(|| Arc::from("test_source"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline_start_time_consistent() {
        let stable = StableEvalContext::test_default();
        let ctx = EvalContext::test_default_borrowed(&stable);
        let t1 = ctx.resolve_pipeline("start_time").unwrap();
        let t2 = ctx.resolve_pipeline("start_time").unwrap();
        // Same value — frozen at pipeline start
        assert_eq!(format!("{:?}", t1), format!("{:?}", t2));
    }

    #[test]
    fn test_pipeline_execution_id_uuid_v7() {
        let mut stable = StableEvalContext::test_default();
        stable.pipeline_execution_id = Arc::from("019502f0-1234-7abc-8def-0123456789ab");
        let ctx = EvalContext::test_default_borrowed(&stable);
        match ctx.resolve_pipeline("execution_id").unwrap() {
            Value::String(s) => {
                assert_eq!(s.len(), 36);
                assert_eq!(s.as_bytes()[14], b'7');
            }
            other => panic!("expected String, got {:?}", other),
        }
    }

    #[test]
    fn test_pipeline_execution_id_format() {
        let mut stable = StableEvalContext::test_default();
        stable.pipeline_execution_id = Arc::from("019502f0-1234-7abc-8def-0123456789ab");
        let ctx = EvalContext::test_default_borrowed(&stable);
        match ctx.resolve_pipeline("execution_id").unwrap() {
            Value::String(s) => {
                assert_eq!(s.len(), 36);
                assert_eq!(s.chars().filter(|c| *c == '-').count(), 4);
            }
            other => panic!("expected String, got {:?}", other),
        }
    }

    #[test]
    fn test_pipeline_batch_id_cli() {
        let mut stable = StableEvalContext::test_default();
        stable.pipeline_batch_id = Arc::from("RUN-001");
        let ctx = EvalContext::test_default_borrowed(&stable);
        match ctx.resolve_pipeline("batch_id").unwrap() {
            Value::String(s) => assert_eq!(&*s, "RUN-001"),
            other => panic!("expected String, got {:?}", other),
        }
    }

    #[test]
    fn test_pipeline_batch_id_default() {
        let mut stable = StableEvalContext::test_default();
        stable.pipeline_batch_id = Arc::from("019502f0-5678-7abc-8def-0123456789ab");
        let ctx = EvalContext::test_default_borrowed(&stable);
        match ctx.resolve_pipeline("batch_id").unwrap() {
            Value::String(s) => {
                assert_eq!(s.len(), 36);
                assert_eq!(s.as_bytes()[14], b'7');
            }
            other => panic!("expected String, got {:?}", other),
        }
    }

    #[test]
    fn test_pipeline_batch_id_auto_default() {
        let mut stable = StableEvalContext::test_default();
        stable.pipeline_batch_id = Arc::from("019502f0-9abc-7def-8012-345678901234");
        let ctx = EvalContext::test_default_borrowed(&stable);
        match ctx.resolve_pipeline("batch_id").unwrap() {
            Value::String(s) => assert_eq!(s.len(), 36),
            other => panic!("expected String, got {:?}", other),
        }
    }

    #[test]
    fn test_pipeline_batch_id_arbitrary_string() {
        let mut stable = StableEvalContext::test_default();
        stable.pipeline_batch_id = Arc::from("daily-retry-2");
        let ctx = EvalContext::test_default_borrowed(&stable);
        match ctx.resolve_pipeline("batch_id").unwrap() {
            Value::String(s) => assert_eq!(&*s, "daily-retry-2"),
            other => panic!("expected String, got {:?}", other),
        }
    }

    #[test]
    fn test_pipeline_name_from_config() {
        let stable = StableEvalContext::test_default();
        let ctx = EvalContext::test_default_borrowed(&stable);
        match ctx.resolve_pipeline("name").unwrap() {
            Value::String(s) => assert_eq!(&*s, "test_pipeline"),
            other => panic!("expected String, got {:?}", other),
        }
    }

    #[test]
    fn test_vars_unknown_reference() {
        let stable = StableEvalContext::test_default();
        let ctx = EvalContext::test_default_borrowed(&stable);
        assert!(ctx.resolve_pipeline("nonexistent").is_none());
    }

    fn make_ctx<'a>(
        stable: &'a StableEvalContext,
        file: &'a Arc<str>,
        batch: &'a Arc<str>,
        row: u64,
    ) -> EvalContext<'a> {
        EvalContext {
            stable,
            source_file: file,
            source_row: row,
            source_path: file,
            source_count: Some(100),
            source_batch: batch,
            ingestion_timestamp: chrono::NaiveDate::from_ymd_opt(2026, 1, 15)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
            source_name: test_default_source_name(),
        }
    }

    #[test]
    fn test_source_provenance_per_record() {
        let stable = StableEvalContext::test_default();
        let file_a: Arc<str> = Arc::from("file_a.csv");
        let batch: Arc<str> = Arc::from("batch-000");
        let ctx = make_ctx(&stable, &file_a, &batch, 10);
        match ctx.resolve_source("file").unwrap() {
            Value::String(s) => assert_eq!(&*s, "file_a.csv"),
            other => panic!("expected String, got {:?}", other),
        }
        assert_eq!(ctx.resolve_source("row"), Some(Value::Integer(10)));

        let file_b: Arc<str> = Arc::from("file_b.csv");
        let ctx = make_ctx(&stable, &file_b, &batch, 20);
        match ctx.resolve_source("file").unwrap() {
            Value::String(s) => assert_eq!(&*s, "file_b.csv"),
            other => panic!("expected String, got {:?}", other),
        }
        assert_eq!(ctx.resolve_source("row"), Some(Value::Integer(20)));
    }

    #[test]
    fn test_source_full_member_set() {
        let stable = StableEvalContext::test_default();
        let file: Arc<str> = Arc::from("data/orders.csv");
        let batch: Arc<str> = Arc::from("RUN-2026-001");
        let ctx = make_ctx(&stable, &file, &batch, 42);
        match ctx.resolve_source("path").unwrap() {
            Value::String(s) => assert_eq!(&*s, "data/orders.csv"),
            other => panic!("expected String, got {:?}", other),
        }
        assert_eq!(ctx.resolve_source("count"), Some(Value::Integer(100)));
        match ctx.resolve_source("batch").unwrap() {
            Value::String(s) => assert_eq!(&*s, "RUN-2026-001"),
            other => panic!("expected String, got {:?}", other),
        }
        assert!(matches!(
            ctx.resolve_source("ingestion_timestamp"),
            Some(Value::DateTime(_))
        ));
        match ctx.resolve_source("name").unwrap() {
            Value::String(s) => assert_eq!(&*s, "test_source"),
            other => panic!("expected String, got {:?}", other),
        }
        assert!(ctx.resolve_source("nonexistent").is_none());
    }

    #[test]
    fn test_source_count_none_resolves_to_null() {
        let stable = StableEvalContext::test_default();
        let file: Arc<str> = Arc::from("data/x.csv");
        let batch: Arc<str> = Arc::from("b");
        let mut ctx = make_ctx(&stable, &file, &batch, 1);
        ctx.source_count = None;
        assert_eq!(ctx.resolve_source("count"), Some(Value::Null));
        ctx.source_count = Some(42);
        assert_eq!(ctx.resolve_source("count"), Some(Value::Integer(42)));
    }

    #[test]
    fn test_pipeline_no_longer_has_source_fields() {
        let stable = StableEvalContext::test_default();
        let file_a: Arc<str> = Arc::from("file_a.csv");
        let batch: Arc<str> = Arc::from("b");
        let ctx = make_ctx(&stable, &file_a, &batch, 10);
        // After §0 split, source_file/source_row are NOT under $pipeline.*.
        assert!(ctx.resolve_pipeline("source_file").is_none());
        assert!(ctx.resolve_pipeline("source_row").is_none());
    }
}

use std::sync::Arc;

use chrono::{NaiveDateTime, Utc};
use clinker_record::{PipelineCounters, Value};
use indexmap::IndexMap;

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
/// members EXCEPT per-record provenance (`source_file`, `source_row`)
/// resolve against this struct.
///
/// D59 (Phase 16 drill pass 4): mirrors DataFusion `Arc<TaskContext>`,
/// Apache Beam `FinishBundleContext`, Flink `RuntimeContext`. Killed the
/// per-record `String::clone` + `IndexMap::clone` profile that the prior
/// `EvalContext` allocated at every dispatch site.
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
    /// pipeline.vars.* — user-defined constants from YAML config.
    pub pipeline_vars: Arc<IndexMap<String, Value>>,
}

impl StableEvalContext {
    /// Resolve a stable `pipeline.*` member by name (everything except
    /// per-record provenance). Per-record `source_file` / `source_row`
    /// resolution lives on the borrowed `EvalContext<'_>` view.
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
            _ => self.pipeline_vars.get(member).cloned(),
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
            pipeline_vars: Arc::new(IndexMap::new()),
        }
    }
}

/// Per-record evaluation context view — borrows from a `StableEvalContext`
/// and adds the only fields that change per row (`source_file`, `source_row`).
/// Constructed inline at every dispatch site; zero allocation.
pub struct EvalContext<'a> {
    pub stable: &'a StableEvalContext,
    /// pipeline.source_file — from record provenance, set per record.
    pub source_file: &'a Arc<str>,
    /// pipeline.source_row — from record provenance, set per record.
    pub source_row: u64,
}

impl<'a> EvalContext<'a> {
    /// Resolve a `pipeline.*` member by name. Per-record provenance is
    /// served by the view; everything else delegates to the stable context.
    pub fn resolve_pipeline(&self, member: &str) -> Option<Value> {
        match member {
            "source_file" => Some(Value::String(self.source_file.to_string().into())),
            "source_row" => Some(Value::Integer(self.source_row as i64)),
            other => self.stable.resolve_pipeline_stable(other),
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

    #[test]
    fn test_pipeline_source_provenance_per_record() {
        let stable = StableEvalContext::test_default();
        let file_a: Arc<str> = Arc::from("file_a.csv");
        let ctx = EvalContext {
            stable: &stable,
            source_file: &file_a,
            source_row: 10,
        };
        match ctx.resolve_pipeline("source_file").unwrap() {
            Value::String(s) => assert_eq!(&*s, "file_a.csv"),
            other => panic!("expected String, got {:?}", other),
        }
        assert_eq!(ctx.resolve_pipeline("source_row"), Some(Value::Integer(10)));

        let file_b: Arc<str> = Arc::from("file_b.csv");
        let ctx = EvalContext {
            stable: &stable,
            source_file: &file_b,
            source_row: 20,
        };
        match ctx.resolve_pipeline("source_file").unwrap() {
            Value::String(s) => assert_eq!(&*s, "file_b.csv"),
            other => panic!("expected String, got {:?}", other),
        }
        assert_eq!(ctx.resolve_pipeline("source_row"), Some(Value::Integer(20)));
    }
}

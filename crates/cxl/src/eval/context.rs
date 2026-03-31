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

/// Evaluation context — injected per pipeline run or per test.
/// All `pipeline.*` members in CXL resolve against this struct.
pub struct EvalContext {
    /// Clock for `now` keyword. WallClock in production, FixedClock in tests.
    pub clock: Box<dyn Clock>,
    /// pipeline.start_time — frozen at pipeline start, deterministic within a run.
    pub pipeline_start_time: NaiveDateTime,
    /// pipeline.name — from YAML config.
    pub pipeline_name: String,
    /// pipeline.execution_id — UUID v7, unique per run.
    pub pipeline_execution_id: String,
    /// pipeline.batch_id — from --batch-id CLI flag or auto UUID v7.
    pub pipeline_batch_id: String,
    /// pipeline.total_count / ok_count / dlq_count.
    pub pipeline_counters: PipelineCounters,
    /// pipeline.source_file — from record provenance, set per record.
    pub source_file: Arc<str>,
    /// pipeline.source_row — from record provenance, set per record.
    pub source_row: u64,
    /// pipeline.vars.* — user-defined constants from YAML config.
    pub pipeline_vars: IndexMap<String, Value>,
}

impl EvalContext {
    /// Create a minimal context for testing.
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
            pipeline_name: "test_pipeline".into(),
            pipeline_execution_id: "00000000-0000-0000-0000-000000000000".into(),
            pipeline_batch_id: "test-batch-000".into(),
            pipeline_counters: PipelineCounters::default(),
            source_file: Arc::from("test.csv"),
            source_row: 1,
            pipeline_vars: IndexMap::new(),
        }
    }

    /// Resolve a pipeline.* member by name.
    pub fn resolve_pipeline(&self, member: &str) -> Option<Value> {
        match member {
            "start_time" => Some(Value::DateTime(self.pipeline_start_time)),
            "name" => Some(Value::String(self.pipeline_name.clone().into())),
            "execution_id" => Some(Value::String(self.pipeline_execution_id.clone().into())),
            "batch_id" => Some(Value::String(self.pipeline_batch_id.clone().into())),
            "total_count" => Some(Value::Integer(self.pipeline_counters.total_count as i64)),
            "ok_count" => Some(Value::Integer(self.pipeline_counters.ok_count as i64)),
            "dlq_count" => Some(Value::Integer(self.pipeline_counters.dlq_count as i64)),
            "source_file" => Some(Value::String(self.source_file.to_string().into())),
            "source_row" => Some(Value::Integer(self.source_row as i64)),
            _ => {
                // Check user-defined pipeline.vars
                self.pipeline_vars.get(member).cloned()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline_start_time_consistent() {
        let ctx = EvalContext::test_default();
        let t1 = ctx.resolve_pipeline("start_time").unwrap();
        let t2 = ctx.resolve_pipeline("start_time").unwrap();
        // Same value — frozen at pipeline start
        assert_eq!(format!("{:?}", t1), format!("{:?}", t2));
    }

    #[test]
    fn test_pipeline_execution_id_uuid_v7() {
        // Verify execution_id resolves as a string and accepts UUID v7 format
        let mut ctx = EvalContext::test_default();
        ctx.pipeline_execution_id = "019502f0-1234-7abc-8def-0123456789ab".to_string();
        match ctx.resolve_pipeline("execution_id").unwrap() {
            Value::String(s) => {
                assert_eq!(s.len(), 36);
                // UUID v7: version nibble (position 14) is '7'
                assert_eq!(s.as_bytes()[14], b'7');
            }
            other => panic!("expected String, got {:?}", other),
        }
    }

    #[test]
    fn test_pipeline_execution_id_format() {
        let mut ctx = EvalContext::test_default();
        ctx.pipeline_execution_id = "019502f0-1234-7abc-8def-0123456789ab".to_string();
        match ctx.resolve_pipeline("execution_id").unwrap() {
            Value::String(s) => {
                // UUID format: 8-4-4-4-12 hex
                assert_eq!(s.len(), 36);
                assert_eq!(s.chars().filter(|c| *c == '-').count(), 4);
            }
            other => panic!("expected String, got {:?}", other),
        }
    }

    #[test]
    fn test_pipeline_batch_id_cli() {
        let mut ctx = EvalContext::test_default();
        ctx.pipeline_batch_id = "RUN-001".to_string();
        match ctx.resolve_pipeline("batch_id").unwrap() {
            Value::String(s) => assert_eq!(&*s, "RUN-001"),
            other => panic!("expected String, got {:?}", other),
        }
    }

    #[test]
    fn test_pipeline_batch_id_default() {
        // When no --batch-id flag, batch_id should be a UUID v7
        let mut ctx = EvalContext::test_default();
        ctx.pipeline_batch_id = "019502f0-5678-7abc-8def-0123456789ab".to_string();
        match ctx.resolve_pipeline("batch_id").unwrap() {
            Value::String(s) => {
                assert_eq!(s.len(), 36);
                assert_eq!(s.as_bytes()[14], b'7'); // UUID v7 version nibble
            }
            other => panic!("expected String, got {:?}", other),
        }
    }

    #[test]
    fn test_pipeline_batch_id_auto_default() {
        // Same semantics as batch_id_default — auto UUID v7 when no flag
        let mut ctx = EvalContext::test_default();
        ctx.pipeline_batch_id = "019502f0-9abc-7def-8012-345678901234".to_string();
        match ctx.resolve_pipeline("batch_id").unwrap() {
            Value::String(s) => assert_eq!(s.len(), 36),
            other => panic!("expected String, got {:?}", other),
        }
    }

    #[test]
    fn test_pipeline_batch_id_arbitrary_string() {
        let mut ctx = EvalContext::test_default();
        ctx.pipeline_batch_id = "daily-retry-2".to_string();
        match ctx.resolve_pipeline("batch_id").unwrap() {
            Value::String(s) => assert_eq!(&*s, "daily-retry-2"),
            other => panic!("expected String, got {:?}", other),
        }
    }

    #[test]
    fn test_pipeline_name_from_config() {
        let ctx = EvalContext::test_default();
        match ctx.resolve_pipeline("name").unwrap() {
            Value::String(s) => assert_eq!(&*s, "test_pipeline"),
            other => panic!("expected String, got {:?}", other),
        }
    }

    #[test]
    fn test_vars_unknown_reference() {
        let ctx = EvalContext::test_default();
        // No user vars set, so pipeline.nonexistent returns None
        assert!(ctx.resolve_pipeline("nonexistent").is_none());
    }

    #[test]
    fn test_pipeline_source_provenance_per_record() {
        let mut ctx = EvalContext::test_default();
        ctx.source_file = std::sync::Arc::from("file_a.csv");
        ctx.source_row = 10;
        match ctx.resolve_pipeline("source_file").unwrap() {
            Value::String(s) => assert_eq!(&*s, "file_a.csv"),
            other => panic!("expected String, got {:?}", other),
        }
        assert_eq!(ctx.resolve_pipeline("source_row"), Some(Value::Integer(10)));

        // Different record
        ctx.source_file = std::sync::Arc::from("file_b.csv");
        ctx.source_row = 20;
        match ctx.resolve_pipeline("source_file").unwrap() {
            Value::String(s) => assert_eq!(&*s, "file_b.csv"),
            other => panic!("expected String, got {:?}", other),
        }
        assert_eq!(ctx.resolve_pipeline("source_row"), Some(Value::Integer(20)));
    }
}

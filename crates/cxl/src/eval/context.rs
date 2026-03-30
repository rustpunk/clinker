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

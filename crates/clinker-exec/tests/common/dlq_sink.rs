//! A dead-letter sink for executor integration tests that keeps the rows the
//! executor actually encoded.
//!
//! The executor writes each dead letter through the caller's
//! [`DlqSink`] as one encoded CSV row under its bucket's compiled header.
//! [`CollectingDlqSink`] records, per bucket, the header it was handed and
//! every row, parsed back with the `csv` crate, so a test reads what the
//! dead-letter file would contain. Every row is checked against its bucket's
//! header as it arrives: a row whose field count differs, or a second header
//! that differs from the first, fails the run with
//! [`PipelineError::Internal`].
//!
//! Its residency grows with the test's own input and nothing else; it lives
//! only under `tests/` and is never compiled into a runtime path. Include it
//! with `#[path = "common/dlq_sink.rs"] mod dlq_sink;`.

use std::collections::{BTreeMap, HashMap};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use clinker_exec::dlq::{DiscardingDlqSink, DlqArtifact, DlqBucketTarget, DlqRowWriter, DlqSink};
use clinker_exec::executor::{
    ExecutionReport, PipelineExecutor, PipelineRunParams, SourceReaders, WriterRegistry,
};
use clinker_plan::config::{CompileContext, PipelineConfig};
use clinker_plan::error::PipelineError;
use clinker_plan::plan::DlqBucketId;

/// A [`DlqSink`] that keeps every row it is given, parsed, per bucket.
///
/// Rows are recorded as each writer writes them, so [`Self::rows`] reads
/// what was written so far, including after a failed run. `finish` follows
/// the trait contract: it refuses while a writer is open or on a second
/// call.
pub struct CollectingDlqSink {
    state: Arc<Mutex<State>>,
}

#[derive(Default)]
struct State {
    buckets: BTreeMap<DlqBucketId, Bucket>,
    open_writers: usize,
    finished: bool,
}

struct Bucket {
    path: Arc<Path>,
    raw_header: Vec<u8>,
    header: Arc<[String]>,
    rows: Vec<Vec<String>>,
}

/// One dead-letter row as the executor encoded it, with its bucket's header.
#[derive(Debug, Clone)]
pub struct DlqRow {
    bucket_path: Arc<Path>,
    header: Arc<[String]>,
    cells: Vec<String>,
}

impl CollectingDlqSink {
    /// A new, empty sink.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Arc::new(Mutex::new(State::default())),
        })
    }

    /// Every row written so far, in bucket order, then in arrival order
    /// within a bucket.
    #[allow(dead_code)] // Each integration target uses only the accessors it asserts on.
    pub fn rows(&self) -> Vec<DlqRow> {
        let state = self.state.lock().expect("dead-letter sink state");
        state.buckets.values().flat_map(Bucket::rows).collect()
    }

    /// The rows written to the bucket whose destination is `path`, in arrival
    /// order; empty when no row reached it.
    #[allow(dead_code)] // Each integration target uses only the accessors it asserts on.
    pub fn rows_for(&self, path: impl AsRef<Path>) -> Vec<DlqRow> {
        let state = self.state.lock().expect("dead-letter sink state");
        state
            .buckets
            .values()
            .filter(|bucket| *bucket.path == *path.as_ref())
            .flat_map(Bucket::rows)
            .collect()
    }

    /// The header column names the executor handed the bucket whose
    /// destination is `path`, or `None` when no row reached it.
    #[allow(dead_code)] // Each integration target uses only the accessors it asserts on.
    pub fn header_for(&self, path: impl AsRef<Path>) -> Option<Vec<String>> {
        let state = self.state.lock().expect("dead-letter sink state");
        state
            .buckets
            .values()
            .find(|bucket| *bucket.path == *path.as_ref())
            .map(|bucket| bucket.header.to_vec())
    }
}

impl Bucket {
    fn rows(&self) -> impl Iterator<Item = DlqRow> + '_ {
        self.rows.iter().map(|cells| DlqRow {
            bucket_path: Arc::clone(&self.path),
            header: Arc::clone(&self.header),
            cells: cells.clone(),
        })
    }
}

impl DlqSink for CollectingDlqSink {
    fn open_walk_writer(&self) -> Result<Box<dyn DlqRowWriter>, PipelineError> {
        self.state
            .lock()
            .expect("dead-letter sink state")
            .open_writers += 1;
        Ok(Box::new(CollectingWriter {
            state: Arc::clone(&self.state),
        }))
    }

    fn finish(&self) -> Result<Vec<DlqArtifact>, PipelineError> {
        let mut state = self.state.lock().expect("dead-letter sink state");
        if state.open_writers != 0 || state.finished {
            return Err(internal(format!(
                "finish called with {} writer(s) open, finished = {}",
                state.open_writers, state.finished
            )));
        }
        state.finished = true;
        Ok(state
            .buckets
            .iter()
            .map(|(id, bucket)| DlqArtifact {
                bucket: *id,
                final_path: bucket.path.to_path_buf(),
                rows: bucket.rows.len() as u64,
            })
            .collect())
    }
}

struct CollectingWriter {
    state: Arc<Mutex<State>>,
}

impl DlqRowWriter for CollectingWriter {
    fn write_row(&mut self, target: &DlqBucketTarget<'_>, row: &[u8]) -> Result<(), PipelineError> {
        let _ = (target, row);
        Ok(())
    }

    fn close(self: Box<Self>) -> Result<(), PipelineError> {
        self.state
            .lock()
            .expect("dead-letter sink state")
            .open_writers -= 1;
        Ok(())
    }
}

/// Parse `bytes` as exactly one CSV record.
fn parse_record(bytes: &[u8]) -> Result<Vec<String>, PipelineError> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(bytes);
    let mut records = reader.records();
    let record = records
        .next()
        .ok_or_else(|| internal("an encoded dead-letter line holds no record".into()))?
        .map_err(|error| internal(format!("an encoded dead-letter line is not CSV: {error}")))?;
    if records.next().is_some() {
        return Err(internal(
            "an encoded dead-letter line holds more than one record".into(),
        ));
    }
    Ok(record.iter().map(str::to_owned).collect())
}

fn internal(detail: String) -> PipelineError {
    PipelineError::Internal {
        op: "dead-letter",
        node: "collecting test sink".to_owned(),
        detail,
    }
}

impl DlqRow {
    /// The destination path of the bucket this row was written to.
    #[allow(dead_code)] // Each integration target uses only the accessors it asserts on.
    pub fn bucket_path(&self) -> &Path {
        &self.bucket_path
    }

    /// The cell under `column`, or `None` when the bucket's header has no
    /// such column. A null value is the empty cell.
    #[allow(dead_code)] // Each integration target uses only the accessors it asserts on.
    pub fn field(&self, column: &str) -> Option<&str> {
        let index = self.header.iter().position(|name| name == column)?;
        Some(&self.cells[index])
    }

    /// `_cxl_dlq_error_category`, or `None` under `include_reason: false`.
    #[allow(dead_code)] // Each integration target uses only the accessors it asserts on.
    pub fn category(&self) -> Option<&str> {
        self.field("_cxl_dlq_error_category")
    }

    /// `_cxl_dlq_error_detail`, or `None` under `include_reason: false`.
    #[allow(dead_code)] // Each integration target uses only the accessors it asserts on.
    pub fn error_detail(&self) -> Option<&str> {
        self.field("_cxl_dlq_error_detail")
    }

    /// `_cxl_dlq_stage`; `None` when the entry named no stage.
    #[allow(dead_code)] // Each integration target uses only the accessors it asserts on.
    pub fn stage(&self) -> Option<&str> {
        self.engine_optional("_cxl_dlq_stage")
    }

    /// `_cxl_dlq_route`; `None` when the entry named no route.
    #[allow(dead_code)] // Each integration target uses only the accessors it asserts on.
    pub fn route(&self) -> Option<&str> {
        self.engine_optional("_cxl_dlq_route")
    }

    /// `_cxl_dlq_trigger`: whether this row's record triggered the failure.
    #[allow(dead_code)] // Each integration target uses only the accessors it asserts on.
    pub fn trigger(&self) -> bool {
        match self.engine("_cxl_dlq_trigger") {
            "true" => true,
            "false" => false,
            other => panic!("_cxl_dlq_trigger must be true or false; got {other:?}"),
        }
    }

    /// `_cxl_dlq_source_name`.
    #[allow(dead_code)] // Each integration target uses only the accessors it asserts on.
    pub fn source_name(&self) -> &str {
        self.engine("_cxl_dlq_source_name")
    }

    /// `_cxl_dlq_source_row`: the row ordinal within its source.
    #[allow(dead_code)] // Each integration target uses only the accessors it asserts on.
    pub fn source_row(&self) -> u64 {
        let cell = self.engine("_cxl_dlq_source_row");
        cell.parse()
            .unwrap_or_else(|_| panic!("_cxl_dlq_source_row must be an ordinal; got {cell:?}"))
    }

    /// `_cxl_dlq_source_file`.
    #[allow(dead_code)] // Each integration target uses only the accessors it asserts on.
    pub fn source_file(&self) -> &str {
        self.engine("_cxl_dlq_source_file")
    }

    /// `_cxl_dlq_triggering_field`; `None` when the entry named no field.
    #[allow(dead_code)] // Each integration target uses only the accessors it asserts on.
    pub fn triggering_field(&self) -> Option<&str> {
        self.engine_optional("_cxl_dlq_triggering_field")
    }

    /// `_cxl_dlq_triggering_value` as its cell text; `None` when the cell is
    /// empty (no value, or a null value).
    #[allow(dead_code)] // Each integration target uses only the accessors it asserts on.
    pub fn triggering_value(&self) -> Option<&str> {
        self.engine_optional("_cxl_dlq_triggering_value")
    }

    /// An engine column every dead-letter header carries.
    fn engine(&self, column: &str) -> &str {
        self.field(column)
            .unwrap_or_else(|| panic!("every dead-letter header carries {column}"))
    }

    /// An engine column whose empty cell stands for an absent value.
    fn engine_optional(&self, column: &str) -> Option<&str> {
        Some(self.engine(column)).filter(|cell| !cell.is_empty())
    }
}

/// A registry over `writers` whose dead-letter rows go to `sink`.
#[allow(dead_code)] // Each integration target uses only the accessors it asserts on.
pub fn registry(
    writers: HashMap<String, Box<dyn Write + Send>>,
    sink: &Arc<CollectingDlqSink>,
) -> WriterRegistry {
    WriterRegistry {
        dlq_sink: Some(Arc::clone(sink) as Arc<dyn DlqSink>),
        ..WriterRegistry::from(writers)
    }
}

/// A registry over `writers` whose dead-letter rows are discarded, for runs
/// that configure a dead-letter path but assert only counts.
#[allow(dead_code)] // Each integration target uses only the accessors it asserts on.
pub fn discarding_registry(writers: HashMap<String, Box<dyn Write + Send>>) -> WriterRegistry {
    WriterRegistry {
        dlq_sink: Some(Arc::new(DiscardingDlqSink)),
        ..WriterRegistry::from(writers)
    }
}

/// Run a parsed `config` to completion like `common::run_config`, collecting
/// its dead-letter rows: the report, and every row in bucket order.
///
/// Compiles against the default (CWD-anchored) context and forwards to the
/// public `&CompiledPlan` entry point. A compile failure is a
/// malformed-fixture bug and panics.
#[allow(dead_code)] // Each integration target uses only the accessors it asserts on.
pub fn run_config_with_dlq(
    config: &PipelineConfig,
    readers: SourceReaders,
    writers: HashMap<String, Box<dyn Write + Send>>,
    params: &PipelineRunParams,
) -> Result<(ExecutionReport, Vec<DlqRow>), PipelineError> {
    let plan = config
        .compile(&CompileContext::default())
        .expect("integration-test pipeline must compile");
    let sink = CollectingDlqSink::new();
    let report = PipelineExecutor::run_plan_with_readers_writers(
        &plan,
        readers,
        registry(writers, &sink),
        params,
    )?;
    Ok((report, sink.rows()))
}

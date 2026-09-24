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

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex};

use clinker_exec::dlq::{
    DiscardingDlqSink, DlqArtifact, DlqBucketTarget, DlqOrigin, DlqPartReceipt, DlqPartSegment,
    DlqPartWriter, DlqRowWriter, DlqSink,
};
use clinker_exec::executor::{
    ExecutionReport, PipelineExecutor, PipelineRunParams, SourceReaders, WriterRegistry,
};
use clinker_plan::config::{CompileContext, PipelineConfig};
use clinker_plan::error::PipelineError;
use clinker_plan::plan::DlqBucketId;

/// A [`DlqSink`] that keeps every row it is given, parsed, per bucket.
///
/// Walk rows are recorded as the walk writes them and a side thread's part
/// where the walk splices it, so [`Self::rows`] reads what was written so
/// far in final file order, including after a failed run. `finish` follows
/// the trait contract: it refuses while a part holding rows was never
/// spliced, while a writer is open, or on a second call.
pub struct CollectingDlqSink {
    state: Arc<Mutex<State>>,
}

#[derive(Default)]
struct State {
    buckets: BTreeMap<DlqBucketId, Bucket>,
    open_writers: usize,
    finished: bool,
    opened_parts: Vec<DlqOrigin>,
    parts_holding_rows: u64,
    parts_spliced: u64,
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

    /// Every row written so far, in bucket order, then within a bucket in
    /// final file order: walk rows as written and each part's rows where it
    /// was spliced.
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

    /// The origin of every part writer opened so far, in opening order.
    #[allow(dead_code)] // Each integration target uses only the accessors it asserts on.
    pub fn opened_parts(&self) -> Vec<DlqOrigin> {
        self.state
            .lock()
            .expect("dead-letter sink state")
            .opened_parts
            .clone()
    }
}

impl Bucket {
    /// A bucket opened by `target`'s first row or splice.
    fn open(target: &DlqBucketTarget<'_>) -> Result<Self, PipelineError> {
        Ok(Self {
            path: Arc::from(target.path),
            raw_header: target.header.to_vec(),
            header: parse_record(target.header)?.into(),
            rows: Vec::new(),
        })
    }

    /// Refuse `target` when it names another destination or header than the
    /// one this bucket was opened with.
    fn check(&self, target: &DlqBucketTarget<'_>) -> Result<(), PipelineError> {
        if *self.path != *target.path || self.raw_header != target.header {
            return Err(internal(format!(
                "bucket {:?} was handed a different destination or header than its first row",
                target.id
            )));
        }
        Ok(())
    }

    /// Parse `row` and refuse it when its field count differs from the
    /// header's.
    fn parse_row(&self, row: &[u8]) -> Result<Vec<String>, PipelineError> {
        let cells = parse_record(row)?;
        if cells.len() != self.header.len() {
            return Err(internal(format!(
                "a row of {} has {} fields under a {}-column header",
                self.path.display(),
                cells.len(),
                self.header.len()
            )));
        }
        Ok(cells)
    }

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

    fn open_part_writer(&self, origin: DlqOrigin) -> Result<Box<dyn DlqPartWriter>, PipelineError> {
        self.state
            .lock()
            .expect("dead-letter sink state")
            .opened_parts
            .push(origin);
        Ok(Box::new(CollectingPartWriter {
            state: Arc::clone(&self.state),
            parts: BTreeMap::new(),
        }))
    }

    fn finish(&self) -> Result<Vec<DlqArtifact>, PipelineError> {
        let mut state = self.state.lock().expect("dead-letter sink state");
        if state.parts_holding_rows != state.parts_spliced {
            return Err(internal(format!(
                "{} dead-letter part(s) holding rows were never spliced",
                state.parts_holding_rows - state.parts_spliced
            )));
        }
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
        let mut state = self.state.lock().expect("dead-letter sink state");
        if state.finished {
            return Err(internal("a row arrived after finish".into()));
        }
        let bucket = match state.buckets.entry(target.id) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(Bucket::open(target)?),
        };
        bucket.check(target)?;
        let cells = bucket.parse_row(row)?;
        bucket.rows.push(cells);
        Ok(())
    }

    fn close(self: Box<Self>) -> Result<(), PipelineError> {
        self.state
            .lock()
            .expect("dead-letter sink state")
            .open_writers -= 1;
        Ok(())
    }

    fn splice(
        &mut self,
        target: &DlqBucketTarget<'_>,
        segment: DlqPartSegment,
    ) -> Result<u64, PipelineError> {
        if segment.bucket() != target.id {
            return Err(internal(format!(
                "a part of bucket {:?} was spliced into bucket {:?}",
                segment.bucket(),
                target.id
            )));
        }
        let part = segment
            .into_part::<CollectedPart>()
            .map_err(|_| internal("a part from another sink was spliced".into()))?;
        let mut state = self.state.lock().expect("dead-letter sink state");
        if state.finished {
            return Err(internal("a part arrived after finish".into()));
        }
        let bucket = match state.buckets.entry(target.id) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(Bucket::open(target)?),
        };
        bucket.check(target)?;
        if part.raw_header != bucket.raw_header {
            return Err(internal(format!(
                "a part of {} was written under a different header",
                target.path.display()
            )));
        }
        let rows = part.rows.len() as u64;
        bucket.rows.extend(part.rows);
        state.parts_spliced += 1;
        Ok(rows)
    }
}

/// A side thread's rows for one bucket, waiting for their splice.
struct CollectedPart {
    raw_header: Vec<u8>,
    rows: Vec<Vec<String>>,
}

struct CollectingPartWriter {
    state: Arc<Mutex<State>>,
    parts: BTreeMap<DlqBucketId, (Bucket, CollectedPart)>,
}

impl DlqPartWriter for CollectingPartWriter {
    fn write_row(&mut self, target: &DlqBucketTarget<'_>, row: &[u8]) -> Result<(), PipelineError> {
        let (bucket, part) = match self.parts.entry(target.id) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                let bucket = Bucket::open(target)?;
                self.state
                    .lock()
                    .expect("dead-letter sink state")
                    .parts_holding_rows += 1;
                entry.insert((
                    bucket,
                    CollectedPart {
                        raw_header: target.header.to_vec(),
                        rows: Vec::new(),
                    },
                ))
            }
        };
        bucket.check(target)?;
        part.rows.push(bucket.parse_row(row)?);
        Ok(())
    }

    fn close(self: Box<Self>) -> Result<DlqPartReceipt, PipelineError> {
        Ok(DlqPartReceipt::new(
            self.parts
                .into_iter()
                .map(|(id, (_, part))| DlqPartSegment::new(id, part.rows.len() as u64, part))
                .collect(),
        ))
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

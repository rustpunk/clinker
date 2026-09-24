//! Process isolation and predecoded fixtures for memory-boundary tests, and a
//! dead-letter capture sink for executor tests.

use clinker_plan::error::PipelineError;

/// Measure the retained metadata for exactly one nonempty, unordered CSV file.
/// Other fixture sources must be header-only: without an order barrier they do
/// not open documents. This is not an allowance for envelopes or multiple files.
/// The finite calibration cap only bounds construction here; the returned
/// allowance is the provider's actual live charge, with final-alias release proven.
pub(crate) fn single_csv_document_metadata_bytes(
    config: &clinker_plan::config::PipelineConfig,
    context: &clinker_plan::config::CompileContext,
    entries: &[(&str, &str)],
) -> u64 {
    use clinker_format::preparation::MemoryOnlyResources;
    use clinker_record::{
        DocumentContext, DocumentId, EnvelopeRecord, owned_storage::OwnedValues,
        schema::AdmittedSchemaBuilder,
    };
    use std::{num::NonZeroUsize, sync::Arc};

    assert_eq!(config.source_configs().count(), entries.len());
    for source in config.source_configs() {
        assert!(matches!(
            source.format,
            clinker_plan::config::InputFormat::Csv(_)
        ));
        assert!(
            source.path.is_some(),
            "fixture requires single-file sources"
        );
        assert!(
            source.envelope.is_none(),
            "fixture requires empty envelopes"
        );
        assert!(
            source.sort_order.is_none(),
            "header-only sources must not open documents through an order barrier"
        );
    }
    let readers = predecoded_csv_readers(config, context, entries);
    assert_eq!(
        readers.len(),
        entries.len(),
        "fixture source names are unique"
    );
    let nonempty_sources = readers
        .into_values()
        .map(|input| {
            let crate::source::SourceInput::Records(mut source) = input else {
                unreachable!("predecoded fixture yields records");
            };
            usize::from(source.next_record().expect("read fixture row").is_some())
        })
        .sum::<usize>();
    assert_eq!(
        nonempty_sources, 1,
        "allowance requires exactly one document"
    );

    let provider = MemoryOnlyResources::new(NonZeroUsize::new(64 * 1024).unwrap());
    let scope = provider.resources().allocation().scope().unwrap();
    let schema = AdmittedSchemaBuilder::try_with_capacity(0, &scope)
        .unwrap()
        .finish(&scope)
        .unwrap();
    let values = OwnedValues::try_with_capacity(0, &scope).unwrap();
    let envelope = EnvelopeRecord::from_owned_values(schema, values).unwrap();
    let document = DocumentContext::try_new(
        DocumentId::next(),
        Arc::from("fixture.csv"),
        envelope,
        &scope,
    )
    .unwrap();
    let bytes = provider.used();
    assert!(bytes > 0, "empty document metadata must be admitted");
    let alias = document.clone();
    drop(document);
    assert_eq!(provider.used(), bytes, "alias retains the entire charge");
    drop(alias);
    assert_eq!(
        provider.used(),
        0,
        "last alias releases all document metadata"
    );
    bytes as u64
}

/// Pair each build fixture record with the row id its Source would mint:
/// ordinal `index + 1` under Source node 1.
///
/// Build fixtures take a Source node other than node 0, which driver fixtures
/// use through the test-only `From<u64>` for `SourceRowId`. A kernel test
/// that confused a build row's identity with a driver's would therefore see a
/// different Source, not just a different ordinal.
pub(crate) fn with_build_row_ids(
    records: Vec<clinker_record::Record>,
) -> Vec<(
    clinker_record::Record,
    crate::executor::stream_event::SourceRowId,
)> {
    let source = <clinker_plan::plan::PlanNodeId as clinker_plan::plan::EntityRef>::new(1);
    records
        .into_iter()
        .enumerate()
        .map(|(index, record)| {
            (
                record,
                crate::executor::stream_event::SourceRowId::new(source, index as u64 + 1),
            )
        })
        .collect()
}

/// Eagerly decode single-file CSV fixtures before execution, using the compiled
/// source body's parser, schema and coercion policy. The returned sources own
/// already-materialized external records; no decoder runs under the test's
/// runtime budget. This isolates downstream operator accounting, not whole-input
/// CSV admission. File identities and physical boundaries match `single_file_reader`.
pub(crate) fn predecoded_csv_readers(
    config: &clinker_plan::config::PipelineConfig,
    context: &clinker_plan::config::CompileContext,
    entries: &[(&str, &str)],
) -> crate::executor::SourceReaders {
    use clinker_format::{FormatError, SourceLifecycleEvent};
    use clinker_record::{Record, Schema, owned_storage::SharedStorage};
    use std::sync::Arc;

    struct DecodedCsv {
        schema: SharedStorage<Schema>,
        rows: std::vec::IntoIter<Record>,
        file: Arc<str>,
        events: Vec<SourceLifecycleEvent>,
        closed: bool,
    }

    impl crate::source::RecordSource for DecodedCsv {
        fn schema(&mut self) -> Result<SharedStorage<Schema>, FormatError> {
            Ok(self.schema.clone())
        }

        fn next_record(&mut self) -> Result<Option<Record>, FormatError> {
            let row = self.rows.next();
            if row.is_none() && !self.closed {
                self.closed = true;
                self.events
                    .push(SourceLifecycleEvent::PhysicalFileClose(Arc::clone(
                        &self.file,
                    )));
            }
            Ok(row)
        }

        fn current_source_file(&self) -> Option<&Arc<str>> {
            Some(&self.file)
        }

        fn take_source_lifecycle_events(&mut self) -> Vec<SourceLifecycleEvent> {
            std::mem::take(&mut self.events)
        }
    }

    let compiled = config
        .compile(context)
        .expect("compile CSV fixture sources");
    entries
        .iter()
        .map(|(name, csv)| {
            let body = compiled
                .config()
                .nodes
                .iter()
                .find_map(|node| match &node.value {
                    clinker_plan::config::PipelineNode::Source { header, config }
                        if header.name == *name =>
                    {
                        Some(config)
                    }
                    _ => None,
                })
                .expect("fixture source exists in bound config");
            assert!(matches!(
                body.source.format,
                clinker_plan::config::InputFormat::Csv(_)
            ));
            let mut reader = crate::executor::build_source_format_reader(
                &body.source,
                &body.schema,
                body.on_unmapped.clone(),
                clinker_format::ReopenableSource::one_shot(Box::new(std::io::Cursor::new(
                    csv.as_bytes().to_vec(),
                ))),
                None,
            )
            .expect("construct CSV fixture decoder");
            let schema = reader.schema().expect("decode CSV fixture schema");
            let mut rows = Vec::new();
            while let Some(row) = reader.next_record().expect("decode CSV fixture record") {
                rows.push(row);
            }
            let file: Arc<str> = Arc::from(format!("{name}.csv"));
            let source = DecodedCsv {
                schema,
                rows: rows.into_iter(),
                events: vec![SourceLifecycleEvent::PhysicalFileOpen(Arc::clone(&file))],
                file,
                closed: false,
            };
            (
                (*name).to_string(),
                crate::source::SourceInput::Records(Box::new(source)),
            )
        })
        .collect()
}

#[test]
fn predecoded_csv_preserves_native_types_projection_order_and_file_output() {
    use clinker_bench_support::io::SharedBuffer;
    use clinker_record::Value;
    use std::{collections::HashMap, sync::Arc};

    let yaml = r#"
pipeline: { name: predecoded_csv_parity }
nodes:
  - type: source
    name: events
    config:
      name: events
      type: csv
      path: events.csv
      on_unmapped: { mode: drop }
      schema:
        - { name: id, type: string }
        - { name: amount, type: int }
  - type: transform
    name: project
    input: events
    config:
      cxl: |
        emit id = id
        emit amount = amount + 1
        emit file = $source.file
        emit source_name = $source.name
  - type: sink
    name: out
    input: project
    config: { name: out, type: csv, path: out.csv }
"#;
    // Header order differs from the declaration, extra data must be dropped,
    // and quoting/newlines must retain their exact bytes through the real parser.
    let csv =
        "amount,extra,id\n0007,unused,\"a,b\"\n-2,ignored,\"quoted \"\"text\"\"\nnext line\"\n";
    let config = clinker_plan::config::parse_config(yaml).expect("parse parity fixture");
    let context = clinker_plan::config::CompileContext::default();
    let mut readers = predecoded_csv_readers(&config, &context, &[("events", csv)]);
    let crate::source::SourceInput::Records(mut source) = readers.remove("events").unwrap() else {
        panic!("fixture must already be decoded");
    };
    let schema = source.schema().unwrap();
    assert_eq!(
        schema
            .columns()
            .iter()
            .map(|name| name.as_ref())
            .collect::<Vec<&str>>(),
        ["id", "amount"]
    );
    assert_eq!(source.current_source_file().unwrap().as_ref(), "events.csv");
    for (id, amount) in [("a,b", 7), ("quoted \"text\"\nnext line", -2)] {
        let row = source.next_record().unwrap().expect("fixture row");
        assert_eq!(row.get("id"), Some(&Value::String(id.into())));
        assert_eq!(row.get("amount"), Some(&Value::Integer(amount)));
        assert!(row.get("extra").is_none());
    }
    assert!(source.next_record().unwrap().is_none());

    let run = |readers| {
        let output = SharedBuffer::new();
        let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
            "out".to_string(),
            Box::new(output.clone()) as Box<dyn std::io::Write + Send>,
        )]);
        crate::executor::PipelineExecutor::run_with_readers_writers_with_arbitrator(
            &config,
            readers,
            writers.into(),
            &crate::executor::PipelineRunParams::default(),
            context.clone(),
            Arc::new(crate::pipeline::memory::MemoryArbitrator::with_policy(
                512 * 1024 * 1024,
                0.80,
                0.70,
                Box::new(crate::pipeline::memory::NoOpPolicy),
            )),
        )
        .expect("parity execution");
        output.contents()
    };
    let decoded = run(predecoded_csv_readers(
        &config,
        &context,
        &[("events", csv)],
    ));
    let files = run(HashMap::from([(
        "events".to_string(),
        crate::executor::single_file_reader(
            "events.csv",
            Box::new(std::io::Cursor::new(csv.as_bytes().to_vec())),
        ),
    )]));
    assert_eq!(decoded, files);
    assert_eq!(decoded, b"id,amount,file,source_name\n\"a,b\",8,events.csv,events\n\"quoted \"\"text\"\"\nnext line\",-1,events.csv,events\n");
}

/// Env flag distinguishing the re-exec'd child from the harness-launched
/// parent: absent in the parent (which re-execs), set in the child
/// (which runs the probe body). Its presence also breaks the otherwise
/// infinite re-exec loop.
const MEMPROBE_ISOLATED_ENV: &str = "CLINKER_MEMPROBE_ISOLATED";

/// Marker the child prints to stdout after the probe's assertions pass.
/// The parent requires it in the child's captured stdout, which is the
/// positive proof the probe actually ran: libtest exits 0 when its
/// filter matches no test, so `status.success()` alone would pass even
/// if `test_path` stopped matching the real test name (a future rename,
/// a filter quirk) and the probe never executed. A dedicated sentinel is
/// robust to libtest output-format drift in a way that scraping for
/// "1 passed" is not.
const MEMPROBE_RAN_SENTINEL: &str = "__clinker_memprobe_ran__";

/// Runs `probe` in a child process where it is the sole test, so its
/// memory samples bracket only its own allocation and the process-global
/// RSS readings cannot be moved by a sibling test thread.
///
/// The probes read a process-global counter (Linux `/proc/self/statm`
/// RSS, Windows `PrivateUsage`, macOS `phys_footprint`) and assert a
/// relation between two or more samples. Under `cargo test`'s default
/// multi-threaded harness, sibling test threads in the same binary
/// commit and free large buffers between the samples, moving the global
/// figure independently of this probe's own allocation and tripping the
/// assertion at random (issue #394). `#[serial]` is insufficient: it
/// only orders `#[serial]`-tagged tests, leaving every other test in the
/// binary concurrent. The mechanism is platform-agnostic, so it runs on
/// every first-class target rather than only macOS/Windows.
///
/// On first entry (parent, env flag absent) this re-execs the test
/// binary filtered to `test_path` alone, with the flag set, then
/// requires both that the child exited successfully and that it printed
/// [`MEMPROBE_RAN_SENTINEL`] — the latter is positive proof the probe
/// ran, since libtest exits 0 on a filter that matches no test. A
/// failure surfaces the child's stderr (the real assertion text). On the
/// recursive entry (child, env flag present) it runs `probe`, prints the
/// sentinel, and returns, letting libtest report the result.
pub(crate) fn run_isolated(test_path: &str, probe: impl FnOnce()) {
    if std::env::var_os(MEMPROBE_ISOLATED_ENV).is_some() {
        probe();
        // Reached only when the probe's assertions all passed; a panic
        // unwinds past this and the sentinel is absent from stdout.
        println!("{MEMPROBE_RAN_SENTINEL}");
        return;
    }

    let exe = std::env::current_exe().expect("test binary path must be readable");
    let output = std::process::Command::new(exe)
        .args(["--exact", test_path, "--test-threads=1", "--nocapture"])
        .env(MEMPROBE_ISOLATED_ENV, "1")
        .output()
        .expect("re-exec of the isolated memory probe must spawn");

    assert!(
        output.status.success(),
        "isolated memory probe {test_path} failed in child process:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains(MEMPROBE_RAN_SENTINEL),
        "isolated memory probe {test_path} never ran: the child exited 0 \
         but did not print its run sentinel, so the `--exact` filter \
         matched no test (likely a stale test-path literal). \
         child stdout:\n{stdout}"
    );
}

/// A dead-letter sink for in-crate executor tests: it keeps the header each
/// bucket was handed and every row the executor encoded, parsed back from
/// CSV, so a test reads what the dead-letter file would contain.
///
/// The in-crate counterpart of the integration tests' collecting sink,
/// which `#[cfg(test)]` code cannot reach. Rows are recorded as they are
/// written; a row whose field count differs from its bucket's header, or a
/// header that changes within a bucket, fails the run with
/// [`PipelineError::Internal`]. Its residency grows only with the test's
/// input.
pub(crate) struct CaptureDlqSink {
    state: std::sync::Arc<std::sync::Mutex<CaptureState>>,
}

#[derive(Default)]
struct CaptureState {
    buckets: std::collections::BTreeMap<clinker_plan::plan::DlqBucketId, CaptureBucket>,
    open_writers: usize,
    finished: bool,
    opened_parts: Vec<crate::dlq::DlqOrigin>,
    parts_holding_rows: u64,
    parts_spliced: u64,
}

struct CaptureBucket {
    path: std::path::PathBuf,
    raw_header: Vec<u8>,
    header: std::sync::Arc<[String]>,
    rows: Vec<Vec<String>>,
}

impl CaptureBucket {
    /// A bucket opened by `target`'s first row or splice.
    fn open(target: &crate::dlq::DlqBucketTarget<'_>) -> Result<Self, PipelineError> {
        Ok(Self {
            path: target.path.to_path_buf(),
            raw_header: target.header.to_vec(),
            header: parse_capture_record(target.header)?.into(),
            rows: Vec::new(),
        })
    }

    /// Refuse `target` when it names another destination or header than the
    /// one this bucket was opened with.
    fn check(&self, target: &crate::dlq::DlqBucketTarget<'_>) -> Result<(), PipelineError> {
        if self.path != target.path || self.raw_header != target.header {
            return Err(capture_error(format!(
                "bucket {:?} was handed a different destination or header than its first row",
                target.id
            )));
        }
        Ok(())
    }

    /// Parse `row` and refuse it when its field count differs from the
    /// header's.
    fn parse_row(&self, row: &[u8]) -> Result<Vec<String>, PipelineError> {
        let cells = parse_capture_record(row)?;
        if cells.len() != self.header.len() {
            return Err(capture_error(format!(
                "a row of {} has {} fields under a {}-column header",
                self.path.display(),
                cells.len(),
                self.header.len()
            )));
        }
        Ok(cells)
    }
}

/// One captured dead-letter row, with its bucket's header.
#[derive(Debug, Clone)]
pub(crate) struct CapturedDlqRow {
    header: std::sync::Arc<[String]>,
    cells: Vec<String>,
}

impl CaptureDlqSink {
    /// A new, empty sink.
    pub(crate) fn new() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            state: Default::default(),
        })
    }

    /// Every row written so far, in bucket order, then within a bucket in
    /// final file order: walk rows as written and each part's rows where it
    /// was spliced.
    pub(crate) fn rows(&self) -> Vec<CapturedDlqRow> {
        let state = self.state.lock().expect("dead-letter capture state");
        state
            .buckets
            .values()
            .flat_map(|bucket| {
                bucket.rows.iter().map(|cells| CapturedDlqRow {
                    header: std::sync::Arc::clone(&bucket.header),
                    cells: cells.clone(),
                })
            })
            .collect()
    }

    /// The origin of every part writer opened so far, in opening order.
    pub(crate) fn opened_parts(&self) -> Vec<crate::dlq::DlqOrigin> {
        self.state
            .lock()
            .expect("dead-letter capture state")
            .opened_parts
            .clone()
    }

    /// A writer registry over `writers` whose dead-letter rows go to this
    /// sink.
    pub(crate) fn registry(
        self: &std::sync::Arc<Self>,
        writers: std::collections::HashMap<String, Box<dyn std::io::Write + Send>>,
    ) -> crate::executor::WriterRegistry {
        crate::executor::WriterRegistry {
            dlq_sink: Some(std::sync::Arc::clone(self) as std::sync::Arc<dyn crate::dlq::DlqSink>),
            ..crate::executor::WriterRegistry::from(writers)
        }
    }
}

impl crate::dlq::DlqSink for CaptureDlqSink {
    fn open_walk_writer(&self) -> Result<Box<dyn crate::dlq::DlqRowWriter>, PipelineError> {
        self.state
            .lock()
            .expect("dead-letter capture state")
            .open_writers += 1;
        Ok(Box::new(CaptureWriter {
            state: std::sync::Arc::clone(&self.state),
        }))
    }

    fn open_part_writer(
        &self,
        origin: crate::dlq::DlqOrigin,
    ) -> Result<Box<dyn crate::dlq::DlqPartWriter>, PipelineError> {
        self.state
            .lock()
            .expect("dead-letter capture state")
            .opened_parts
            .push(origin);
        Ok(Box::new(CapturePartWriter {
            state: std::sync::Arc::clone(&self.state),
            parts: std::collections::BTreeMap::new(),
        }))
    }

    fn finish(&self) -> Result<Vec<crate::dlq::DlqArtifact>, PipelineError> {
        let mut state = self.state.lock().expect("dead-letter capture state");
        if state.parts_holding_rows != state.parts_spliced {
            return Err(capture_error(format!(
                "{} dead-letter part(s) holding rows were never spliced",
                state.parts_holding_rows - state.parts_spliced
            )));
        }
        if state.open_writers != 0 || state.finished {
            return Err(capture_error(format!(
                "finish called with {} writer(s) open, finished = {}",
                state.open_writers, state.finished
            )));
        }
        state.finished = true;
        Ok(state
            .buckets
            .iter()
            .map(|(id, bucket)| crate::dlq::DlqArtifact {
                bucket: *id,
                final_path: bucket.path.clone(),
                rows: bucket.rows.len() as u64,
            })
            .collect())
    }
}

struct CaptureWriter {
    state: std::sync::Arc<std::sync::Mutex<CaptureState>>,
}

impl crate::dlq::DlqRowWriter for CaptureWriter {
    fn write_row(
        &mut self,
        target: &crate::dlq::DlqBucketTarget<'_>,
        row: &[u8],
    ) -> Result<(), PipelineError> {
        use std::collections::btree_map::Entry;
        let mut state = self.state.lock().expect("dead-letter capture state");
        if state.finished {
            return Err(capture_error("a row arrived after finish".into()));
        }
        let bucket = match state.buckets.entry(target.id) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(CaptureBucket::open(target)?),
        };
        bucket.check(target)?;
        let cells = bucket.parse_row(row)?;
        bucket.rows.push(cells);
        Ok(())
    }

    fn close(self: Box<Self>) -> Result<(), PipelineError> {
        self.state
            .lock()
            .expect("dead-letter capture state")
            .open_writers -= 1;
        Ok(())
    }

    fn splice(
        &mut self,
        target: &crate::dlq::DlqBucketTarget<'_>,
        segment: crate::dlq::DlqPartSegment,
    ) -> Result<u64, PipelineError> {
        use std::collections::btree_map::Entry;
        if segment.bucket() != target.id {
            return Err(capture_error(format!(
                "a part of bucket {:?} was spliced into bucket {:?}",
                segment.bucket(),
                target.id
            )));
        }
        let part = segment
            .into_part::<CapturePart>()
            .map_err(|_| capture_error("a part from another sink was spliced".into()))?;
        let mut state = self.state.lock().expect("dead-letter capture state");
        if state.finished {
            return Err(capture_error("a part arrived after finish".into()));
        }
        let bucket = match state.buckets.entry(target.id) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(CaptureBucket::open(target)?),
        };
        bucket.check(target)?;
        if part.raw_header != bucket.raw_header {
            return Err(capture_error(format!(
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
struct CapturePart {
    raw_header: Vec<u8>,
    rows: Vec<Vec<String>>,
}

struct CapturePartWriter {
    state: std::sync::Arc<std::sync::Mutex<CaptureState>>,
    parts:
        std::collections::BTreeMap<clinker_plan::plan::DlqBucketId, (CaptureBucket, CapturePart)>,
}

impl crate::dlq::DlqPartWriter for CapturePartWriter {
    fn write_row(
        &mut self,
        target: &crate::dlq::DlqBucketTarget<'_>,
        row: &[u8],
    ) -> Result<(), PipelineError> {
        use std::collections::btree_map::Entry;
        let (bucket, part) = match self.parts.entry(target.id) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                let bucket = CaptureBucket::open(target)?;
                self.state
                    .lock()
                    .expect("dead-letter capture state")
                    .parts_holding_rows += 1;
                entry.insert((
                    bucket,
                    CapturePart {
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

    fn close(self: Box<Self>) -> Result<crate::dlq::DlqPartReceipt, PipelineError> {
        Ok(crate::dlq::DlqPartReceipt::new(
            self.parts
                .into_iter()
                .map(|(id, (_, part))| {
                    crate::dlq::DlqPartSegment::new(id, part.rows.len() as u64, part)
                })
                .collect(),
        ))
    }
}

/// Parse `bytes` as exactly one CSV record.
fn parse_capture_record(bytes: &[u8]) -> Result<Vec<String>, PipelineError> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(bytes);
    let mut records = reader.records();
    let record = records
        .next()
        .ok_or_else(|| capture_error("an encoded dead-letter line holds no record".into()))?
        .map_err(|error| {
            capture_error(format!("an encoded dead-letter line is not CSV: {error}"))
        })?;
    if records.next().is_some() {
        return Err(capture_error(
            "an encoded dead-letter line holds more than one record".into(),
        ));
    }
    Ok(record.iter().map(str::to_owned).collect())
}

fn capture_error(detail: String) -> PipelineError {
    PipelineError::Internal {
        op: "dead-letter",
        node: "capture test sink".to_owned(),
        detail,
    }
}

impl CapturedDlqRow {
    /// The cell under `column`, or `None` when the bucket's header has no
    /// such column. A null value is the empty cell.
    pub(crate) fn field(&self, column: &str) -> Option<&str> {
        let index = self.header.iter().position(|name| name == column)?;
        Some(&self.cells[index])
    }

    /// `_cxl_dlq_error_category`, or `None` under `include_reason: false`.
    pub(crate) fn category(&self) -> Option<&str> {
        self.field("_cxl_dlq_error_category")
    }

    /// `_cxl_dlq_trigger`: whether this row's record triggered the failure.
    pub(crate) fn trigger(&self) -> bool {
        match self.field("_cxl_dlq_trigger") {
            Some("true") => true,
            Some("false") => false,
            other => panic!("_cxl_dlq_trigger must be true or false; got {other:?}"),
        }
    }

    /// `_cxl_dlq_source_row`: the row ordinal within its source.
    pub(crate) fn source_row(&self) -> u64 {
        let cell = self
            .field("_cxl_dlq_source_row")
            .expect("every dead-letter header carries _cxl_dlq_source_row");
        cell.parse()
            .unwrap_or_else(|_| panic!("_cxl_dlq_source_row must be an ordinal; got {cell:?}"))
    }

    /// The header and the cells, with the cell of each column named in
    /// `columns` replaced by a fixed token.
    ///
    /// For byte-identity comparisons that ignore generated columns: the
    /// encoder writes a row as a pure function of its header and cells, so
    /// two runs whose masked rows are equal wrote the same bytes outside the
    /// masked columns. A name the header lacks masks nothing.
    pub(crate) fn masked(&self, columns: &[&str]) -> (Vec<String>, Vec<String>) {
        const MASK: &str = "<masked>";
        let cells = self
            .header
            .iter()
            .zip(&self.cells)
            .map(|(name, cell)| {
                if columns.contains(&name.as_str()) {
                    MASK.to_owned()
                } else {
                    cell.clone()
                }
            })
            .collect();
        (self.header.to_vec(), cells)
    }
}

#[cfg(test)]
mod tests {
    use super::CaptureDlqSink;
    use crate::dlq::{DlqBucketTarget, DlqOrigin, DlqSink};
    use clinker_plan::config::{CompileContext, parse_config};

    /// The capture sink reads back in final file order: walk rows as they
    /// were written and each part's rows where it was spliced. It records the
    /// origin of every part writer opened.
    #[test]
    fn capture_sink_rows_follow_splice_order() {
        let plan = parse_config(
            "pipeline:\n  name: capture\nerror_handling:\n  strategy: continue\n  dlq:\n    path: dlq.csv\n\
nodes:\n- type: source\n  name: src\n  config:\n    name: src\n    type: csv\n    path: in.csv\n    schema:\n      - { name: id, type: string }\n\
- type: sink\n  name: out\n  input: src\n  config:\n    name: out\n    type: csv\n    path: out.csv\n",
        )
        .expect("pipeline parses")
        .compile(&CompileContext::default())
        .expect("pipeline compiles");
        let layout = plan.dlq_layout().expect("a DLQ block yields a layout");
        let id = layout.bucket_for_source("src").expect("bucket");
        let target = DlqBucketTarget {
            id,
            path: std::path::Path::new("dlq.csv"),
            header: b"_cxl_dlq_source_row\n",
        };
        let sink = CaptureDlqSink::new();
        let origin = DlqOrigin::AggregateIngest {
            node: "agg".to_owned(),
        };

        let mut part = sink.open_part_writer(origin.clone()).expect("part");
        part.write_row(&target, b"2\n").expect("part row");
        part.write_row(&target, b"3\n").expect("part row");
        let receipt = part.close().expect("close part");
        let mut walk = sink.open_walk_writer().expect("walk");
        walk.write_row(&target, b"1\n").expect("walk row");
        for segment in receipt.into_segments() {
            assert_eq!(walk.splice(&target, segment).expect("splice"), 2);
        }
        walk.write_row(&target, b"4\n").expect("walk row");
        walk.close().expect("close walk");

        assert_eq!(
            sink.rows()
                .iter()
                .map(super::CapturedDlqRow::source_row)
                .collect::<Vec<_>>(),
            [1, 2, 3, 4]
        );
        assert_eq!(sink.opened_parts(), [origin]);
        assert_eq!(sink.finish().expect("finish")[0].rows, 4);
    }
}

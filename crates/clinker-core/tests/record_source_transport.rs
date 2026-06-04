//! A non-file transport registers through `SourceInput::Records` and
//! feeds the identical ingest channel as a file source — no special-case
//! dispatch path. Its `$source.*` provenance resolves to the stable
//! per-source synthetic identifier (`<source:NAME>`) because the row
//! yielder exposes no per-record file identity.

use std::sync::Arc;

use clinker_format::{EnvelopeConfig, FormatError};
use clinker_record::{Record, Schema, SchemaBuilder, Value};
use indexmap::IndexMap;

use clinker_core::executor::{PipelineExecutor, PipelineRunParams};
use clinker_core::source::RecordSource;
use std::collections::HashMap;
use std::io::Write;

/// In-memory row yielder with no byte stream behind it — the shape a
/// pathless row-yielding transport takes. Holds a fixed schema and a queue
/// of rows; `current_source_file` stays `None` so the ingest loop falls
/// back to the source's stable synthetic id.
struct StubRecordSource {
    schema: Arc<Schema>,
    rows: std::collections::VecDeque<Vec<Value>>,
}

impl StubRecordSource {
    fn new(columns: &[&str], rows: Vec<Vec<Value>>) -> Self {
        let mut builder = SchemaBuilder::with_capacity(columns.len());
        for col in columns {
            builder = builder.with_field(*col);
        }
        Self {
            schema: builder.build(),
            rows: rows.into_iter().collect(),
        }
    }
}

impl RecordSource for StubRecordSource {
    fn schema(&mut self) -> Result<Arc<Schema>, FormatError> {
        Ok(Arc::clone(&self.schema))
    }

    fn next_record(&mut self) -> Result<Option<Record>, FormatError> {
        Ok(self
            .rows
            .pop_front()
            .map(|values| Record::new(Arc::clone(&self.schema), values)))
    }

    fn prepare_document(
        &mut self,
        _config: &EnvelopeConfig,
    ) -> Result<IndexMap<Box<str>, Value>, FormatError> {
        Ok(IndexMap::new())
    }
}

/// A pathless source declared with a file matcher only so the config
/// parses and typechecks; the matcher is discarded because the executor
/// drives the `SourceInput::Records` entry registered below, never fs
/// discovery. Clearing `path` after parse simulates the pathless shape a
/// non-file transport carries, exercising the `<source:NAME>` synthetic
/// id fallback.
const PIPELINE: &str = r#"
pipeline:
  name: record_source_transport
nodes:
  - type: source
    name: ledger
    config:
      name: ledger
      type: csv
      path: placeholder.csv
      schema:
        - { name: id, type: int }
        - { name: amount, type: int }
  - type: transform
    name: stamp
    input: ledger
    config:
      cxl: |
        emit id = id
        emit amount = amount
        emit src_file = $source.file
        emit src_name = $source.name
  - type: output
    name: out
    input: stamp
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;

#[test]
fn non_file_transport_emits_into_shared_channel_with_synthetic_id() {
    let mut config = clinker_core::config::parse_config(PIPELINE).unwrap();

    // Strip the placeholder matcher so the source is pathless, the shape
    // a non-file transport carries. `path_str()` then returns empty and
    // the ingest loop stamps the stable `<source:ledger>` synthetic id.
    for spanned in &mut config.nodes {
        if let clinker_core::config::PipelineNode::Source { config: body, .. } = &mut spanned.value
        {
            body.source.path = None;
        }
    }

    let plan = config
        .compile(&clinker_core::config::CompileContext::default())
        .expect("compile pathless-source pipeline");

    let stub = StubRecordSource::new(
        &["id", "amount"],
        vec![
            vec![Value::Integer(1), Value::Integer(10)],
            vec![Value::Integer(2), Value::Integer(20)],
            vec![Value::Integer(3), Value::Integer(30)],
        ],
    );

    let readers: clinker_core::executor::SourceReaders = HashMap::from([(
        "ledger".to_string(),
        clinker_core::executor::SourceInput::Records(Box::new(stub)),
    )]);

    let output_buf = clinker_bench_support::io::SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(output_buf.clone()) as Box<dyn Write + Send>,
    )]);

    let report = PipelineExecutor::run_plan_with_readers_writers(
        &plan,
        readers,
        writers,
        &PipelineRunParams {
            execution_id: "rec-src-exec".to_string(),
            batch_id: "rec-src-batch".to_string(),
            ..Default::default()
        },
    )
    .expect("drive non-file RecordSource to completion");

    // All three stub rows reached the shared channel and committed —
    // identical accounting to a file source, no special dispatch path.
    assert_eq!(report.counters.total_count, 3);
    assert_eq!(report.counters.ok_count, 3);

    let output = output_buf.as_string();
    let data_lines: Vec<&str> = output.lines().filter(|l| !l.is_empty()).collect();
    // Header + 3 rows.
    assert_eq!(data_lines.len(), 4, "output was:\n{output}");

    // Every body row stamps the stable synthetic id for both
    // `$source.file` and `$source.name` (the node name).
    for line in &data_lines[1..] {
        assert!(
            line.contains("<source:ledger>"),
            "expected synthetic source id in row {line:?}; full output:\n{output}"
        );
        assert!(
            line.contains("ledger"),
            "expected source name in row {line:?}; full output:\n{output}"
        );
    }
}

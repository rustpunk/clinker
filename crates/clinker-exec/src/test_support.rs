//! Process isolation and predecoded fixtures for memory-boundary tests.

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

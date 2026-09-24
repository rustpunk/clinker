use clinker_exec::{
    executor::preparation::ExecutorResources,
    pipeline::{
        memory::{MemoryArbitrator, NoOpPolicy},
        shutdown::ShutdownToken,
    },
};
use clinker_record::owned_storage::ResourceErrorKind;

#[path = "common/dlq_sink.rs"]
mod dlq_sink;

mod physical_runtime {
    use super::*;
    use clinker_exec::executor::{PipelineExecutor, PipelineRunParams, WriterRegistry};
    use clinker_exec::telemetry::{MetricKey, SpanName, SpanStatus};
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    fn pipeline(swift: bool) -> String {
        let source_schema = if swift {
            "[{ name: tag, type: string }, { name: value, type: string }]"
        } else {
            "[{ name: value, type: string }]"
        };
        let sink = if swift {
            "      type: swift\n      options: { basic_header: HEADER, trailer: TAIL }"
        } else {
            "      type: fixed_width\n      schema: [{ name: value, type: string, width: 100000 }]"
        };
        format!(
            r#"
pipeline: {{ name: physical_delivery }}
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      path: input.csv
      schema: {source_schema}
  - type: sink
    name: result
    input: rows
    config:
      name: result
{sink}
      path: output.txt
"#
        )
    }

    fn input(swift: bool) -> Vec<u8> {
        let a = "a".repeat(100_000);
        let b = "b".repeat(100_000);
        if swift {
            format!("tag,value\n20,{a}\n21,{b}\n")
        } else {
            format!("value\n{a}\n{b}\n")
        }
        .into_bytes()
    }

    fn operations(swift: bool) -> [Vec<u8>; 3] {
        let a = "a".repeat(100_000);
        let b = "b".repeat(100_000);
        if swift {
            [
                format!("{{1:HEADER}}{{4:\r\n:20:{a}\r\n").into_bytes(),
                format!(":21:{b}\r\n").into_bytes(),
                b"-}{5:TAIL}".to_vec(),
            ]
        } else {
            [
                format!("{a}\n").into_bytes(),
                format!("{b}\n").into_bytes(),
                Vec::new(),
            ]
        }
    }

    #[test]
    fn physical_compiled_resident_and_spill_publish_identical_literal_files() {
        for swift in [false, true] {
            let plan = clinker_plan::config::parse_config(&pipeline(swift))
                .unwrap()
                .compile(&clinker_plan::config::CompileContext::default())
                .unwrap();
            let expected = operations(swift).concat();
            for spill in [false, true] {
                let root = tempfile::tempdir().unwrap();
                let destination = root.path().join("output.txt");
                let staging = clinker_exec::output::staging::OutputStagingRegistry::default();
                let (_, file) = staging
                    .stage_output(
                        "result",
                        clinker_plan::config::IfExistsPolicy::Error,
                        false,
                        |_| Ok(destination.clone()),
                    )
                    .unwrap();
                let registry = WriterRegistry {
                    single: [("result".into(), Box::new(file) as Box<dyn Write + Send>)].into(),
                    output_staging: staging,
                    ..Default::default()
                };
                let readers = [(
                    "rows".into(),
                    clinker_exec::executor::single_file_reader(
                        "input.csv",
                        Box::new(std::io::Cursor::new(input(swift))),
                    ),
                )]
                .into();
                let (producer, receiver) = telemetry();
                let params = PipelineRunParams {
                    spill_root_dir: spill.then(|| root.path().to_owned()),
                    telemetry_producer: Some(producer),
                    ..Default::default()
                };
                let report = PipelineExecutor::run_plan_with_readers_writers(
                    &plan, readers, registry, &params,
                )
                .unwrap();
                assert_eq!(std::fs::read(&destination).unwrap(), expected);
                assert_eq!(report.counters.total_count, 2);
                assert_eq!(report.counters.records_written, 2);
                assert_eq!(report.counters.dlq_count, 0);
                let mut spills = 0;
                let mut stages = 0;
                let mut spill_bytes = 0;
                while let Some(batch) = receiver.try_recv_batch() {
                    spills += batch.metric(MetricKey::WriterSpillCompleted);
                    stages += batch.metric(MetricKey::WriterStageCompleted);
                    spill_bytes += batch.metric(MetricKey::WriterSpillBytes);
                    assert_eq!(batch.metric(MetricKey::WriterSpillFailed), 0);
                }
                assert_eq!(spills, if spill { 2 } else { 0 });
                assert_eq!(stages, 3);
                assert_eq!(
                    spill_bytes,
                    if spill {
                        operations(swift)[..2]
                            .iter()
                            .map(|op| op.len() as u64)
                            .sum()
                    } else {
                        0
                    }
                );
                // These post-join snapshots establish cleanup, independently
                // of the operation metrics above establishing actual spill.
                assert_eq!(report.cumulative_spill_bytes, 0);
                assert!(
                    report
                        .per_stage_spill_bytes
                        .values()
                        .all(|&bytes| bytes == 0)
                );
                assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
            }
        }
    }

    struct Destination {
        bytes: Arc<Mutex<Vec<u8>>>,
        calls: Arc<AtomicUsize>,
        failures: Arc<AtomicUsize>,
        flushes: Arc<AtomicUsize>,
        limit: usize,
        fault: &'static str,
        token: ShutdownToken,
    }
    impl Write for Destination {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fault == "interrupted" && call == 0 {
                return Err(std::io::ErrorKind::Interrupted.into());
            }
            let mut output = self.bytes.lock().unwrap();
            if output.len() == self.limit {
                self.failures.fetch_add(1, Ordering::SeqCst);
                return if self.fault == "zero" {
                    Ok(0)
                } else {
                    Err(std::io::ErrorKind::BrokenPipe.into())
                };
            }
            let n = bytes.len().min(self.limit - output.len());
            output.extend_from_slice(&bytes[..n]);
            if self.fault == "cancel" && output.len() == self.limit {
                self.token.request();
            }
            Ok(n)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            self.flushes.fetch_add(1, Ordering::SeqCst);
            if self.fault == "flush" {
                Err(std::io::ErrorKind::BrokenPipe.into())
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn physical_resource_refusals_preserve_each_operation_and_release_its_owners() {
        use clinker_format::counting::{CountingWriter, SharedByteCounter};
        use clinker_format::fixed_width::writer::{FixedWidthEncoder, FixedWidthWriterConfig};
        use clinker_format::swift::writer::{SwiftEncoder, SwiftWriterConfig};
        use clinker_format::{FormatError, FormatWriterHandle};
        use clinker_record::owned_storage::{OwnedMap, SharedStorage};
        use clinker_record::{DocumentContext, DocumentId, EnvelopeRecord, Record, Schema, Value};
        let mut cases = 0;
        for swift in [false, true] {
            let schema = SharedStorage::from_arc(Arc::new(Schema::new(if swift {
                vec!["tag".into(), "value".into()]
            } else {
                vec!["value".into()]
            })));
            let body = "B".repeat(100_000);
            let head = "H".repeat(100_000);
            let tail = "T".repeat(100_000);
            let doc = Arc::new(DocumentContext::new(
                DocumentId::next(),
                Arc::from("input.txt"),
                EnvelopeRecord::from_sections([("opening", &head), ("closing", &tail)].map(
                    |(name, text)| {
                        (
                            name.into(),
                            Value::Map(OwnedMap::from_map(
                                [("body".into(), Value::String(text.as_str().into()))].into(),
                            )),
                        )
                    },
                )),
            ));
            let mut row = Record::new(
                schema.clone(),
                if swift {
                    vec![
                        Value::String("20".into()),
                        Value::String(body.as_str().into()),
                    ]
                } else {
                    vec![Value::String(body.as_str().into())]
                },
            );
            row.set_doc_ctx(SharedStorage::from_arc(doc.clone()));
            let body_bytes = if swift {
                format!("{{1:HEADER}}{{4:\r\n:20:{body}\r\n")
            } else {
                format!("{body}\n")
            };
            // First body, later body, document open/close, and finalization are
            // separate transactions; SWIFT has no document-hook output.
            for operation in if swift {
                &[0, 1, 4][..]
            } else {
                &[0, 1, 2, 3, 4][..]
            } {
                let expected_operation = match (swift, operation) {
                    (_, 0) => body_bytes.clone(),
                    (false, 1) => body_bytes.clone(),
                    (true, 1) => format!(":20:{body}\r\n"),
                    (_, 2) => format!("{head}\n"),
                    (_, 3) => format!("{tail}\n"),
                    (false, 4) => String::new(),
                    (true, 4) => format!("-}}{{5:{tail}}}"),
                    _ => unreachable!(),
                };
                for fault in ["none", "pressure", "disk", "descriptor", "create"] {
                    if expected_operation.is_empty() && matches!(fault, "disk" | "create") {
                        continue;
                    }
                    for mode in 0..3 {
                        cases += 1;
                        let context =
                            format!("swift={swift}/operation={operation}/{fault}/telemetry={mode}");
                        let root = tempfile::tempdir().unwrap();
                        let spill_root = root.path().join("spill");
                        std::fs::create_dir(&spill_root).unwrap();
                        let arb = Arc::new(MemoryArbitrator::with_policy(
                            1024 * 1024,
                            0.8,
                            0.7,
                            Box::new(NoOpPolicy),
                        ));
                        let token = ShutdownToken::detached();
                        let (producer, receiver) = telemetry();
                        let provider = ExecutorResources::new(
                            arb.clone(),
                            token.clone(),
                            Some(&configured(&spill_root)),
                            NonZeroUsize::new(1).unwrap(),
                            (mode != 0).then(|| producer.clone()),
                        )
                        .unwrap();
                        let baseline = arb.writer_resource_usage().memory;
                        let resources = provider.resources();
                        let bytes = Arc::new(Mutex::new(Vec::new()));
                        let counter = SharedByteCounter::new();
                        let destination = CountingWriter::new(
                            Destination {
                                bytes: bytes.clone(),
                                calls: Arc::new(AtomicUsize::new(0)),
                                failures: Arc::new(AtomicUsize::new(0)),
                                flushes: Arc::new(AtomicUsize::new(0)),
                                limit: usize::MAX,
                                fault: "none",
                                token,
                            },
                            counter.clone(),
                        );
                        let mut writer: FormatWriterHandle = if swift {
                            SwiftEncoder::new(
                                schema.clone(),
                                &SwiftWriterConfig {
                                    basic_header: Some("HEADER".into()),
                                    trailer_from_doc: Some("closing".into()),
                                    ..Default::default()
                                },
                                resources.clone(),
                            )
                            .unwrap()
                            .into_boxed_writer(destination, resources.clone())
                            .unwrap()
                        } else {
                            FixedWidthEncoder::new(
                                &[clinker_format::Column {
                                    width: Some(100_000),
                                    ..clinker_format::Column::bare(
                                        "value",
                                        cxl::typecheck::Type::String,
                                    )
                                }],
                                &FixedWidthWriterConfig {
                                    envelope: Some(clinker_format::OutputEnvelopeSpec {
                                        header_from_doc: Some("opening".into()),
                                        footer_from_doc: Some("closing".into()),
                                        footer_record_count_field: None,
                                    }),
                                    ..Default::default()
                                },
                                resources.clone(),
                            )
                            .unwrap()
                            .into_boxed_writer(destination, resources.clone())
                            .unwrap()
                        };
                        let mut expected = Vec::new();
                        if matches!(operation, 1 | 3 | 4) {
                            writer.write_record(&row).unwrap();
                            expected.extend_from_slice(body_bytes.as_bytes());
                        }
                        let mut prior_spills = 0;
                        while let Some(batch) = receiver.try_recv_batch() {
                            prior_spills += batch.metric(MetricKey::WriterSpillCompleted);
                        }
                        assert_eq!(
                            prior_spills,
                            u64::from(mode != 0 && matches!(operation, 1 | 3 | 4))
                        );
                        if mode == 2 {
                            saturate_writer_telemetry(&producer);
                        }
                        let arena = producer.snapshot();
                        let retained = arb.writer_resource_usage().memory;
                        let scope = resources.scope().unwrap();
                        let pressure = (fault == "pressure").then(|| {
                            scope
                                .reserve(
                                    Layout::array::<u8>(
                                        (arb.limit()
                                            - arb.writer_resource_usage().memory
                                            - 256 * 1024)
                                            as usize,
                                    )
                                    .unwrap(),
                                )
                                .unwrap()
                        });
                        let descriptor = (fault == "descriptor").then(|| scope.stage().unwrap());
                        if fault == "disk" {
                            arb.set_max_spill_bytes(0).unwrap();
                        }
                        if fault == "create" {
                            std::fs::remove_dir(&spill_root).unwrap();
                            std::fs::write(&spill_root, b"not a directory").unwrap();
                        }
                        let apply = |writer: &mut FormatWriterHandle| match operation {
                            0 | 1 => writer.write_record(&row),
                            2 => writer.begin_document(&doc),
                            3 => writer.end_document(&doc),
                            _ => writer.flush(),
                        };
                        let result = apply(&mut writer);
                        let success = matches!(fault, "none" | "pressure");
                        if success {
                            result.unwrap();
                            expected.extend_from_slice(expected_operation.as_bytes());
                        } else {
                            let kind = match fault {
                                "disk" => ResourceErrorKind::DiskQuota,
                                "descriptor" => ResourceErrorKind::DescriptorQuota,
                                _ => ResourceErrorKind::Storage,
                            };
                            assert!(
                                matches!(result, Err(FormatError::Resource(error)) if error.kind == kind),
                                "{context}: {result:?}"
                            );
                        }
                        assert_eq!(*bytes.lock().unwrap(), expected, "{context}");
                        assert_eq!(counter.bytes_written(), expected.len() as u64);
                        drop(descriptor);
                        drop(pressure);
                        if !success {
                            assert_eq!(arb.writer_resource_usage().memory, retained, "{context}");
                        }
                        if mode != 0 {
                            let batch = receiver.try_recv_batch().unwrap();
                            assert_eq!(
                                batch.metric(MetricKey::WriterSpillCompleted),
                                u64::from(success && !expected_operation.is_empty()),
                                "own operation: {context}"
                            );
                            assert_eq!(
                                batch.metric(MetricKey::WriterStageCompleted),
                                u64::from(success),
                                "{context}"
                            );
                            assert_eq!(producer.snapshot().owned_bytes, arena.owned_bytes);
                            if mode == 2 {
                                assert_eq!(producer.snapshot().accepted, arena.accepted);
                                assert!(producer.snapshot().full_drops > arena.full_drops);
                            }
                        } else {
                            assert!(receiver.try_recv_batch().is_none());
                        }
                        // Preparation refusals remain retryable after restoring
                        // the same authority, without duplicating committed bytes.
                        if !success {
                            if fault == "disk" {
                                arb.set_max_spill_bytes(u64::MAX).unwrap();
                            }
                            if fault == "create" {
                                std::fs::remove_file(&spill_root).unwrap();
                                std::fs::create_dir(&spill_root).unwrap();
                            }
                            apply(&mut writer).unwrap();
                            expected.extend_from_slice(expected_operation.as_bytes());
                            assert_eq!(*bytes.lock().unwrap(), expected);
                            assert_eq!(counter.bytes_written(), expected.len() as u64);
                        }
                        drop(writer);
                        drop(scope);
                        drop(resources);
                        assert_eq!(arb.writer_resource_usage().memory, baseline);
                        assert_eq!(arb.writer_resource_usage().disk, 0);
                        assert_eq!(arb.writer_resource_usage().descriptors, 0);
                        assert_eq!(provider.cleanup_debt_count(), 0);
                        assert_eq!(std::fs::read_dir(&spill_root).unwrap().count(), 0);
                        drop(provider);
                        assert_eq!(arb.writer_resource_usage().memory, 0);
                        assert_eq!(arb.consumer_count(), 0);
                    }
                }
            }
        }
        assert_eq!(cases, 114);
    }

    #[test]
    fn physical_sink_counts_only_accepted_bytes_and_committed_records_with_fixed_arena() {
        for swift in [false, true] {
            let plan = clinker_plan::config::parse_config(&pipeline(swift))
                .unwrap()
                .compile(&clinker_plan::config::CompileContext::default())
                .unwrap();
            let operations = operations(swift);
            let complete = operations.concat();
            for full in [false, true] {
                for (fault, operation) in [
                    ("none", 0),
                    ("interrupted", 0),
                    ("prefix", 0),
                    ("prefix", 1),
                    ("zero", 0),
                    ("cancel", 0),
                    ("cancel", 1),
                    ("flush", 2),
                    ("prefix", 2),
                    ("cancel", 2),
                ] {
                    if !swift && operation == 2 && fault != "flush" {
                        continue;
                    }
                    let root = tempfile::tempdir().unwrap();
                    let bytes = Arc::new(Mutex::new(Vec::new()));
                    let calls = Arc::new(AtomicUsize::new(0));
                    let failures = Arc::new(AtomicUsize::new(0));
                    let flushes = Arc::new(AtomicUsize::new(0));
                    let token = ShutdownToken::detached();
                    let limit = match fault {
                        "prefix" => operations[..operation].iter().map(Vec::len).sum::<usize>() + 3,
                        "cancel" => {
                            operations[..operation].iter().map(Vec::len).sum::<usize>()
                                + operations[operation]
                                    .len()
                                    .min(clinker_format::preparation::PROGRESS_BYTES / 2)
                        }
                        "zero" => 0,
                        _ => usize::MAX,
                    };
                    let registry = WriterRegistry {
                        single: [(
                            "result".into(),
                            Box::new(Destination {
                                bytes: bytes.clone(),
                                calls: calls.clone(),
                                failures: failures.clone(),
                                flushes: flushes.clone(),
                                limit,
                                fault,
                                token: token.clone(),
                            }) as Box<dyn Write + Send>,
                        )]
                        .into(),
                        ..Default::default()
                    };
                    let readers = [(
                        "rows".into(),
                        clinker_exec::executor::single_file_reader(
                            "input.csv",
                            Box::new(std::io::Cursor::new(input(swift))),
                        ),
                    )]
                    .into();
                    let (producer, receiver) = telemetry();
                    if full {
                        saturate_writer_telemetry(&producer);
                    }
                    let arena = producer.snapshot();
                    let params = PipelineRunParams {
                        shutdown_token: Some(token),
                        spill_root_dir: Some(root.path().to_owned()),
                        telemetry_producer: Some(producer.clone()),
                        ..Default::default()
                    };
                    let result = PipelineExecutor::run_plan_with_readers_writers(
                        &plan, readers, registry, &params,
                    );
                    let success = matches!(fault, "none" | "interrupted");
                    let cancelled = fault == "cancel";
                    let accepted = limit.min(complete.len());
                    let records = if success || fault == "flush" {
                        2
                    } else {
                        operation as u64
                    };
                    let context = format!("swift={swift}/full={full}/{fault}/{operation}");
                    if success || cancelled {
                        let report = result.unwrap_or_else(|error| panic!("{context}: {error:?}"));
                        assert_eq!(report.interrupted, cancelled, "{context}");
                        assert_eq!(report.counters.records_written, records, "{context}");
                    } else {
                        assert!(result.is_err(), "{context}");
                    }
                    assert_eq!(*bytes.lock().unwrap(), complete[..accepted], "{context}");
                    assert_eq!(
                        failures.load(Ordering::SeqCst),
                        usize::from(matches!(fault, "prefix" | "zero")),
                        "no retry: {context}"
                    );
                    if !success {
                        assert_eq!(
                            flushes.load(Ordering::SeqCst),
                            usize::from(fault == "flush"),
                            "{context}"
                        );
                    }
                    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
                    let mut counts = [0; 7];
                    let mut sink_spans = 0;
                    let mut drained = 0;
                    let mut spills = 0;
                    while let Some(batch) = receiver.try_recv_batch() {
                        drained += batch.logs().len() + batch.traces().len();
                        spills += batch.metric(MetricKey::WriterSpillCompleted);
                        for (i, key) in [
                            MetricKey::SinkStarted,
                            MetricKey::SinkFailed,
                            MetricKey::SinkRecords,
                            MetricKey::SinkErrors,
                            MetricKey::SinkBytes,
                            MetricKey::SinkCompleted,
                            MetricKey::SinkInterrupted,
                        ]
                        .into_iter()
                        .enumerate()
                        {
                            counts[i] += batch.metric(key);
                        }
                        for span in batch
                            .traces()
                            .iter()
                            .filter(|span| span.name == SpanName::Sink)
                        {
                            sink_spans += 1;
                            assert_eq!(
                                span.status,
                                if success {
                                    SpanStatus::Ok
                                } else if cancelled {
                                    SpanStatus::Unset
                                } else {
                                    SpanStatus::Error
                                },
                                "{context}"
                            );
                            assert!(
                                span.started_at_unix_nanos > 0
                                    && span.started_at_unix_nanos <= span.ended_at_unix_nanos
                            );
                        }
                    }
                    assert_eq!(
                        counts,
                        [
                            1,
                            u64::from(!success && !cancelled),
                            records,
                            u64::from(!success && !cancelled),
                            accepted as u64,
                            u64::from(success),
                            u64::from(cancelled)
                        ],
                        "{context}"
                    );
                    assert_eq!(
                        spills,
                        if success || operation >= 1 || fault == "flush" {
                            2
                        } else {
                            1
                        },
                        "{context}"
                    );
                    let after = producer.snapshot();
                    assert_eq!(after.owned_bytes, arena.owned_bytes);
                    assert_eq!(
                        after.accepted,
                        drained as u64 + after.undecodable_drops,
                        "signal conservation: {context}"
                    );
                    assert_eq!(after.undecodable_drops, 0);
                    if full {
                        assert_eq!(sink_spans, 0);
                        assert_eq!(after.accepted, arena.accepted);
                        assert!(
                            after.full_drops + after.contention_drops
                                > arena.full_drops + arena.contention_drops
                        );
                    } else if sink_spans == 0 {
                        assert!(
                            after.full_drops + after.contention_drops
                                > arena.full_drops + arena.contention_drops,
                            "missing terminal signal must be counted: {context}"
                        );
                    }
                }
            }
        }
    }
}

mod swift_resources {
    use super::*;
    use clinker_format::counting::{CountingWriter, SharedByteCounter};
    use clinker_format::preparation::{FormatEncoder, OutputOperation, PreparedWriter};
    use clinker_format::swift::writer::{SwiftEncoder, SwiftWriterConfig};
    use clinker_format::{FormatError, FormatWriter};
    use clinker_record::owned_storage::{OwnedKey, OwnedMap, SharedStorage};
    use clinker_record::{DocumentContext, DocumentId, EnvelopeRecord, Record, Schema, Value};
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    fn record(body: &str, trailer: &str) -> Record {
        let schema =
            SharedStorage::from_arc(Arc::new(Schema::new(vec!["tag".into(), "value".into()])));
        let mut row = Record::new(
            schema,
            vec![Value::String("20".into()), Value::String(body.into())],
        );
        row.set_doc_ctx(SharedStorage::from_arc(Arc::new(DocumentContext::new(
            DocumentId::next(),
            Arc::from("input.swift"),
            EnvelopeRecord::from_sections([(
                OwnedKey::from("authored trailer"),
                Value::Map(OwnedMap::from_map(
                    [(OwnedKey::from("body"), Value::String(trailer.into()))].into(),
                )),
            )]),
        ))));
        row
    }
    fn config() -> SwiftWriterConfig {
        SwiftWriterConfig {
            basic_header: Some("HEADER".into()),
            trailer_from_doc: Some("authored trailer".into()),
            ..Default::default()
        }
    }

    #[test]
    fn swift_runtime_factory_config_and_writer_backings_remain_charged_through_deallocation() {
        use clinker_format::splitting::WriterFactory;
        use clinker_format::swift::writer::SwiftEncoderConfig;
        let arb = Arc::new(MemoryArbitrator::with_policy(
            256 * 1024,
            0.8,
            0.7,
            Box::new(NoOpPolicy),
        ));
        let provider = ExecutorResources::new(
            arb.clone(),
            ShutdownToken::detached(),
            None,
            NonZeroUsize::new(1).unwrap(),
            None,
        )
        .unwrap();
        let resources = provider.resources();
        let scope = resources.scope().unwrap();
        let observer = arb.writer_resource_observer();
        NATIVE_BACKINGS.with(|watch| {
            watch.set(Some(NativeBackingWatch {
                observer: &observer,
                capture: Some(0),
                capture_last: true,
                backings: [NativeBacking::default(); 3],
            }))
        });
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                NATIVE_BACKINGS.with(|watch| watch.set(None));
            }
        }
        let reset = Reset;
        let config = SwiftEncoderConfig::new(&config(), &resources).unwrap();
        NATIVE_BACKINGS.with(|watch| {
            let mut state = watch.get().unwrap();
            state.capture = None;
            state.capture_last = false;
            watch.set(Some(state));
        });
        let config_memory = observer.usage().memory;
        let alias = config.clone();
        let captured = resources.clone();
        let make = move |destination, schema| {
            SwiftEncoder::from_config(schema, config.clone())?
                .into_boxed_writer(destination, captured.clone())
        };
        let factory_bytes = std::mem::size_of_val(&make);
        NATIVE_BACKINGS.with(|watch| {
            let mut state = watch.get().unwrap();
            state.capture = Some(1);
            watch.set(Some(state));
        });
        let factory = WriterFactory::try_new(make, scope.allocation()).unwrap();
        let row = record("body", "tail");
        let destination: Box<dyn Write + Send> = Box::new(std::io::sink());
        let counter = SharedByteCounter::new();
        let counting = CountingWriter::new(destination, counter);
        NATIVE_BACKINGS.with(|watch| {
            let mut state = watch.get().unwrap();
            state.capture = Some(2);
            watch.set(Some(state));
        });
        let mut writer = factory.create(counting, row.schema().clone()).unwrap();
        let backings = NATIVE_BACKINGS.with(|watch| watch.get().unwrap().backings);
        assert!(backings[0].bytes > 0);
        assert_eq!(backings[1].bytes, factory_bytes);
        assert_eq!(
            backings[2].bytes,
            std::mem::size_of::<PreparedWriter<CountingWriter<Box<dyn Write + Send>>, SwiftEncoder>>(
            )
        );
        writer.write_record(&row).unwrap();
        let before_factory_drop = observer.usage().memory;
        drop(factory);
        drop(writer);
        assert_eq!(observer.usage().memory, config_memory);
        arb.close_writer_resources();
        assert!(observer.is_closed());
        drop(alias);
        let backings = NATIVE_BACKINGS.with(|watch| watch.get().unwrap().backings);
        assert_eq!(
            backings[1].admitted_at_deallocation,
            Some(before_factory_drop)
        );
        // The writer drops its trailer before deallocating its own wrapper.
        // Config remains live through the alias, so its bytes cannot mask an
        // early release of this particular wrapper's grant.
        assert_eq!(
            backings[2].admitted_at_deallocation,
            Some(config_memory + backings[2].bytes as u64)
        );
        assert_eq!(
            backings[0].admitted_at_deallocation,
            // SharedStorage moves its payload out, frees the shared backing,
            // then drops payload children and the backing lease.
            Some(config_memory)
        );
        for backing in backings {
            assert!(backing.admitted_at_allocation >= backing.bytes as u64);
            assert!(backing.admitted_at_deallocation.unwrap() >= backing.bytes as u64);
        }
        assert_eq!(observer.usage().memory, 0);
        assert_eq!(arb.consumer_count(), 0);
        drop(reset);
    }

    #[test]
    fn swift_runtime_config_allocator_failures_release_every_partial_owner() {
        use clinker_format::preparation::MemoryOnlyResources;
        use clinker_format::swift::writer::SwiftEncoderConfig;
        let provider = MemoryOnlyResources::new(NonZeroUsize::new(256 * 1024).unwrap());
        let config = SwiftWriterConfig {
            basic_header: Some("header".repeat(2000)),
            app_header_from_doc: Some("authored header".into()),
            user_header: Some("{108:ref}".into()),
            trailer_from_doc: Some("authored trailer".into()),
            ..Default::default()
        };
        let resources = provider.resources();
        ALLOCATIONS.with(|count| count.set(Some(0)));
        let admitted = SwiftEncoderConfig::new(&config, &resources).unwrap();
        let attempts = ALLOCATIONS.with(|count| count.replace(None).unwrap());
        eprintln!("SWIFT configuration allocation attempts tested: {attempts}");
        drop(admitted);
        assert_eq!(provider.used(), 0);
        assert!(attempts >= 5);
        for fail in 1..=attempts {
            ALLOCATIONS.with(|count| count.set(Some(0)));
            FAIL_ALLOCATION.with(|fault| fault.set(Some(fail)));
            let result = SwiftEncoderConfig::new(&config, &resources);
            FAIL_ALLOCATION.with(|fault| fault.set(None));
            ALLOCATIONS.with(|count| count.set(None));
            assert!(
                matches!(result, Err(FormatError::Resource(error)) if error.kind == ResourceErrorKind::Allocation),
                "allocation {fail}"
            );
            assert_eq!(provider.used(), 0);
        }
    }

    #[test]
    fn swift_runtime_pending_trailer_allocator_failures_release_replacement_overlap() {
        use clinker_format::preparation::MemoryOnlyResources;
        let provider = MemoryOnlyResources::new(NonZeroUsize::new(256 * 1024).unwrap());
        let resources = provider.resources();
        let scope = resources.scope().unwrap();
        let row = record("body", &"tail".repeat(12000));
        let encoder = SwiftEncoder::new(row.schema().clone(), &config(), resources).unwrap();
        let baseline = provider.used();
        ALLOCATIONS.with(|count| count.set(Some(0)));
        let pending = encoder
            .prepare(OutputOperation::Record(&row), &mut std::io::sink(), &scope)
            .unwrap();
        let attempts = ALLOCATIONS.with(|count| count.replace(None).unwrap());
        eprintln!("SWIFT trailer allocation attempts tested: {attempts}");
        assert!(attempts >= 3);
        drop(pending);
        assert_eq!(provider.used(), baseline);
        for fail in 1..=attempts {
            ALLOCATIONS.with(|count| count.set(Some(0)));
            FAIL_ALLOCATION.with(|fault| fault.set(Some(fail)));
            let result =
                encoder.prepare(OutputOperation::Record(&row), &mut std::io::sink(), &scope);
            FAIL_ALLOCATION.with(|fault| fault.set(None));
            ALLOCATIONS.with(|count| count.set(None));
            assert!(
                matches!(result, Err(FormatError::Resource(error)) if error.kind == ResourceErrorKind::Allocation),
                "allocation {fail}"
            );
            assert_eq!(provider.used(), baseline);
        }
    }
    struct Destination {
        bytes: Arc<Mutex<Vec<u8>>>,
        calls: Arc<AtomicUsize>,
        failure: Arc<AtomicUsize>,
        cancellation: Arc<AtomicUsize>,
        token: ShutdownToken,
    }
    impl Write for Destination {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let mut output = self.bytes.lock().unwrap();
            let remaining = self
                .failure
                .load(Ordering::SeqCst)
                .saturating_sub(output.len());
            if remaining == 0 {
                return Err(std::io::ErrorKind::BrokenPipe.into());
            }
            let count = bytes.len().min(remaining);
            output.extend_from_slice(&bytes[..count]);
            if output.len() >= self.cancellation.load(Ordering::SeqCst) {
                self.token.request();
            }
            Ok(count)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn swift_runtime_partial_delivery_and_cancellation_preserve_accepted_bytes_and_owners() {
        for finalization in [false, true] {
            for cancel in [false, true] {
                for spill in [false, true] {
                    let dir = tempfile::tempdir().unwrap();
                    let arb = Arc::new(MemoryArbitrator::with_policy(
                        2 * 1024 * 1024,
                        0.8,
                        0.7,
                        Box::new(NoOpPolicy),
                    ));
                    let token = ShutdownToken::detached();
                    let storage = configured(dir.path());
                    let (producer, receiver) = telemetry();
                    let provider = ExecutorResources::new(
                        arb.clone(),
                        token.clone(),
                        spill.then_some(&storage),
                        NonZeroUsize::new(1).unwrap(),
                        Some(producer),
                    )
                    .unwrap();
                    let baseline = arb.writer_resource_usage().memory;
                    let body = if spill {
                        "body ".repeat(20000)
                    } else {
                        "body".into()
                    };
                    let trailer = "tail".repeat(if spill { 20000 } else { 3000 });
                    let row = record(&body, &trailer);
                    let record_bytes = format!("{{1:HEADER}}{{4:\r\n:20:{body}\r\n");
                    let bytes = Arc::new(Mutex::new(Vec::new()));
                    let calls = Arc::new(AtomicUsize::new(0));
                    let failure = Arc::new(AtomicUsize::new(usize::MAX));
                    let cancellation = Arc::new(AtomicUsize::new(usize::MAX));
                    let destination = Destination {
                        bytes: bytes.clone(),
                        calls: calls.clone(),
                        failure: failure.clone(),
                        cancellation: cancellation.clone(),
                        token,
                    };
                    let counter = SharedByteCounter::new();
                    let encoder =
                        SwiftEncoder::new(row.schema().clone(), &config(), provider.resources())
                            .unwrap();
                    let mut writer = PreparedWriter::new(
                        CountingWriter::new(destination, counter.clone()),
                        encoder,
                        provider.resources(),
                    )
                    .unwrap();
                    let before = arb.writer_resource_usage().memory;
                    if finalization {
                        writer.write_record(&row).unwrap();
                        // Drain the earlier successful record's signals so the
                        // failing finalization must prove its own stage spill.
                        let prior = receiver.try_recv_batch().unwrap();
                        assert_eq!(
                            prior.metric(clinker_exec::telemetry::MetricKey::WriterSpillCompleted),
                            u64::from(spill)
                        );
                    }
                    let committed = arb.writer_resource_usage().memory;
                    if finalization {
                        assert!(committed >= before + trailer.len() as u64);
                    }
                    let start = if finalization { record_bytes.len() } else { 0 };
                    if cancel {
                        cancellation.store(start + 1, Ordering::SeqCst);
                    } else {
                        failure.store(start + 3, Ordering::SeqCst);
                    }
                    let result = if finalization {
                        writer.flush()
                    } else {
                        writer.write_record(&row)
                    };
                    if cancel {
                        assert!(
                            matches!(result, Err(FormatError::Resource(error)) if error.kind == ResourceErrorKind::Cancelled)
                        );
                    } else {
                        assert!(
                            matches!(result, Err(FormatError::Io(error)) if error.kind() == std::io::ErrorKind::BrokenPipe)
                        );
                    }
                    let delivered = bytes.lock().unwrap().clone();
                    let complete = format!("{record_bytes}-}}{{5:{trailer}}}");
                    assert_eq!(delivered, complete.as_bytes()[..delivered.len()]);
                    assert!(delivered.len() > start);
                    if !cancel {
                        assert_eq!(delivered.len(), start + 3);
                    }
                    assert_eq!(counter.bytes_written(), delivered.len() as u64);
                    assert_eq!(arb.writer_resource_usage().memory, committed);
                    let attempts = calls.load(Ordering::SeqCst);
                    assert!(writer.write_record(&row).is_err());
                    assert!(writer.flush().is_err());
                    assert!(writer.flush_bytes().is_err());
                    drop(writer);
                    assert_eq!(calls.load(Ordering::SeqCst), attempts);
                    assert_eq!(arb.writer_resource_usage().memory, baseline);
                    assert_eq!(arb.writer_resource_usage().disk, 0);
                    assert_eq!(arb.writer_resource_usage().descriptors, 0);
                    assert_eq!(provider.cleanup_debt_count(), 0);
                    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
                    let batch = receiver.try_recv_batch().unwrap();
                    assert_eq!(
                        batch.metric(clinker_exec::telemetry::MetricKey::WriterSpillCompleted),
                        u64::from(spill),
                        "failing operation spill: finalize={finalization}, cancel={cancel}"
                    );
                    assert_eq!(
                        batch.metric(clinker_exec::telemetry::MetricKey::WriterStageInterrupted),
                        u64::from(cancel)
                    );
                }
            }
        }
    }

    #[test]
    fn swift_runtime_budget_denial_is_retryable_without_header_or_trailer_commit() {
        let arb = Arc::new(MemoryArbitrator::with_policy(
            256 * 1024,
            0.8,
            0.7,
            Box::new(NoOpPolicy),
        ));
        let provider = ExecutorResources::new(
            arb.clone(),
            ShutdownToken::detached(),
            None,
            NonZeroUsize::new(1).unwrap(),
            None,
        )
        .unwrap();
        let resources = provider.resources();
        let scope = resources.scope().unwrap();
        let row = record("body", "retained trailer");
        let encoder =
            SwiftEncoder::new(row.schema().clone(), &config(), resources.clone()).unwrap();
        let mut writer = PreparedWriter::new(Vec::new(), encoder, resources).unwrap();
        let before = arb.writer_resource_usage().memory;
        let pressure = scope
            .reserve(Layout::array::<u8>((arb.limit() - before) as usize).unwrap())
            .unwrap();
        assert!(
            matches!(writer.write_record(&row), Err(FormatError::Resource(error)) if error.kind == ResourceErrorKind::Budget)
        );
        assert!(writer.destination().is_empty());
        drop(pressure);
        assert_eq!(arb.writer_resource_usage().memory, before);
        writer.write_record(&row).unwrap();
        let retained = arb.writer_resource_usage().memory;
        assert!(retained > before);
        let pressure = scope
            .reserve(Layout::array::<u8>((arb.limit() - retained) as usize).unwrap())
            .unwrap();
        assert!(
            matches!(writer.flush(), Err(FormatError::Resource(error)) if error.kind == ResourceErrorKind::Budget)
        );
        assert_eq!(writer.destination(), b"{1:HEADER}{4:\r\n:20:body\r\n");
        drop(pressure);
        assert_eq!(arb.writer_resource_usage().memory, retained);
        writer.flush().unwrap();
        writer.flush().unwrap();
        assert_eq!(
            writer.destination(),
            b"{1:HEADER}{4:\r\n:20:body\r\n-}{5:retained trailer}"
        );
        assert_eq!(arb.writer_resource_usage().memory, before);
    }

    #[test]
    fn swift_runtime_pending_trailer_drops_before_release_and_commit_does_not_allocate() {
        let arb = Arc::new(MemoryArbitrator::with_policy(
            256 * 1024,
            0.8,
            0.7,
            Box::new(NoOpPolicy),
        ));
        let provider = ExecutorResources::new(
            arb.clone(),
            ShutdownToken::detached(),
            None,
            NonZeroUsize::new(1).unwrap(),
            None,
        )
        .unwrap();
        let resources = provider.resources();
        let scope = resources.scope().unwrap();
        let observer = arb.writer_resource_observer();
        let row = record("body", &"é".repeat(10000));
        let mut encoder = SwiftEncoder::new(row.schema().clone(), &config(), resources).unwrap();
        let before = observer.usage().memory;
        for commit in [false, true] {
            NATIVE_BACKINGS.with(|watch| {
                watch.set(Some(NativeBackingWatch {
                    observer: &observer,
                    capture: Some(0),
                    capture_last: true,
                    backings: [NativeBacking::default(); 3],
                }))
            });
            struct Reset;
            impl Drop for Reset {
                fn drop(&mut self) {
                    NATIVE_BACKINGS.with(|watch| watch.set(None));
                }
            }
            let reset = Reset;
            let pending = encoder
                .prepare(OutputOperation::Record(&row), &mut std::io::sink(), &scope)
                .unwrap();
            NATIVE_BACKINGS.with(|watch| {
                let mut state = watch.get().unwrap();
                state.capture = None;
                watch.set(Some(state));
            });
            assert!(observer.usage().memory >= before + 20000);
            if commit {
                ALLOCATIONS.with(|count| count.set(Some(0)));
                FAIL_ALLOCATION.with(|fail| fail.set(Some(1)));
                encoder.commit(pending);
                FAIL_ALLOCATION.with(|fail| fail.set(None));
                assert_eq!(ALLOCATIONS.with(|count| count.replace(None)), Some(0));
                let pending = encoder
                    .prepare(OutputOperation::Finalize, &mut std::io::sink(), &scope)
                    .unwrap();
                encoder.commit(pending);
            } else {
                drop(pending);
            }
            let backing = NATIVE_BACKINGS.with(|watch| watch.get().unwrap().backings[0]);
            assert!(backing.bytes >= 20000);
            assert!(backing.admitted_at_allocation >= before + backing.bytes as u64);
            assert!(backing.admitted_at_deallocation.unwrap() >= before + backing.bytes as u64);
            assert_eq!(observer.usage().memory, before);
            drop(reset);
        }
    }
}

fn decode_coercion_reader(
    bytes: Vec<u8>,
    config: clinker_format::csv::reader::CsvReaderConfig,
    columns: &[clinker_format::Column],
    policy: clinker_plan::config::pipeline_node::OnUnmapped,
    resources: clinker_record::owned_storage::AllocationResources,
) -> Box<dyn clinker_format::FormatReader> {
    use clinker_exec::pipeline::schema_coerce::CoercingReader;
    use clinker_format::preparation::{DecodeWorkspace, TextStorage};
    let reader = clinker_format::csv::reader::CsvReader::from_reader_admitted(
        std::io::Cursor::new(bytes),
        config,
        DecodeWorkspace::new(resources.clone()).unwrap(),
        TextStorage::Shared,
    )
    .unwrap();
    Box::new(
        CoercingReader::new_csv_admitted(Box::new(reader), columns, policy, "rows", resources)
            .unwrap(),
    )
}

#[test]
fn decode_coercion_owns_projected_schema_slots_widening_and_unique_text() {
    use clinker_format::{Column, csv::reader::CsvReaderConfig, preparation::MemoryOnlyResources};
    use clinker_plan::config::pipeline_node::OnUnmapped;
    use clinker_record::Value;
    use cxl::typecheck::Type;
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(1024 * 1024).unwrap());
    let text = "long-decoded-field-".repeat(8);
    let input = format!("physical,unique,extra\n{text},{text},{text}\n");
    let columns = [
        Column {
            source_name: Some("physical".into()),
            ..Column::bare("logical", Type::String)
        },
        Column {
            long_unique: Some(true),
            ..Column::bare("unique", Type::String)
        },
    ];
    let mut reader = decode_coercion_reader(
        input.into_bytes(),
        CsvReaderConfig::default(),
        &columns,
        OnUnmapped::AutoWiden,
        provider.resources().allocation().clone(),
    );
    let schema = reader.schema().unwrap();
    assert_eq!(schema.legacy_estimated_outer_heap_size(), 0);
    assert_eq!(schema.legacy_estimated_heap_size(), 0);
    let row = reader.next_record().unwrap().unwrap();
    assert!(row.values_are_governed());
    for name in ["logical", "unique"] {
        let Value::String(value) = row.get(name).unwrap() else {
            panic!("text")
        };
        assert_eq!(value.as_str(), text);
        assert_eq!(value.legacy_heap_size(), 0);
    }
    let Value::Map(widened) = row.get("$widened").unwrap() else {
        panic!("sidecar")
    };
    assert_eq!(widened.legacy_heap_size(), 0);
    assert_eq!(widened.keys().next().unwrap().legacy_heap_size(), 0);
    let alias = row.get("logical").unwrap().clone();
    let independent = row.get("unique").unwrap().clone();
    let Value::String(unique) = &independent else {
        panic!("unique")
    };
    assert!(
        unique.legacy_heap_size() > 0,
        "independent legacy copies never inherit grants"
    );
    drop(row);
    drop(reader);
    assert!(
        provider.used() > 0,
        "bare schema and text aliases keep their owners"
    );
    drop(schema);
    assert!(
        provider.used() > 0,
        "bare Value keeps the shared text owner"
    );
    drop(alias);
    assert_eq!(provider.used(), 0);
    assert_eq!(unique.as_str(), text);
}

#[test]
fn decode_coercion_repeated_values_are_admitted_and_rejections_keep_originals() {
    use clinker_format::{Column, csv::reader::CsvReaderConfig, preparation::MemoryOnlyResources};
    use clinker_plan::config::pipeline_node::OnUnmapped;
    use clinker_record::Value;
    use cxl::typecheck::Type;
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(1024 * 1024).unwrap());
    let config = CsvReaderConfig {
        split_values: vec![clinker_format::multi_value::SplitValues {
            field: "values".into(),
            delimiter: ";".into(),
            escape: String::new(),
            json: false,
        }],
        ..Default::default()
    };
    let columns = [Column {
        multiple: Some(true),
        ..Column::bare("values", Type::Int)
    }];
    let mut reader = decode_coercion_reader(
        b"values\n1;2\n3;invalid\n".to_vec(),
        config,
        &columns,
        OnUnmapped::Drop,
        provider.resources().allocation().clone(),
    );
    let row = reader.next_record().unwrap().unwrap();
    let Value::Array(values) = row.get("values").unwrap() else {
        panic!("array")
    };
    assert!(values.is_governed());
    assert_eq!(values.as_slice(), &[Value::Integer(1), Value::Integer(2)]);
    let clinker_format::FormatError::DeclaredType(failure) = reader.next_record().unwrap_err()
    else {
        panic!("typed rejection")
    };
    assert_eq!(
        failure.original_record.get("values"),
        Some(&failure.original_value)
    );
    let Value::Array(original) = failure.original_record.get("values").unwrap() else {
        panic!("array")
    };
    assert_eq!(original[0], Value::String("3".into()));
    drop(failure);
    drop(row);
    drop(reader);
    assert_eq!(provider.used(), 0);
}

#[test]
fn decode_coercion_long_unique_reaches_repeated_and_nested_text() {
    use clinker_format::{Column, csv::reader::CsvReaderConfig, preparation::MemoryOnlyResources};
    use clinker_plan::config::pipeline_node::OnUnmapped;
    use clinker_record::Value;
    use cxl::typecheck::Type;
    fn assert_unique(value: &Value) {
        match value {
            Value::String(text) => {
                assert_eq!(text.legacy_heap_size(), 0);
                if text.heap_size() > 0 {
                    let independent = text.clone();
                    assert_ne!(
                        text.as_str().as_ptr(),
                        independent.as_str().as_ptr(),
                        "every long leaf must use unique storage"
                    );
                    assert!(independent.legacy_heap_size() > 0);
                    assert_eq!(independent, *text);
                }
            }
            Value::Array(items) => {
                assert!(items.is_governed());
                for item in items.iter() {
                    assert_unique(item);
                }
            }
            Value::Map(items) => {
                assert_eq!(items.legacy_heap_size(), 0);
                for (key, item) in items.iter() {
                    assert_eq!(key.legacy_heap_size(), 0);
                    assert_unique(item);
                }
            }
            _ => {}
        }
    }
    let text = "unique-nested-text-".repeat(12);
    for json in [false, true] {
        let split = if json { "json: true" } else { "delimiter: ';'" };
        let ty = if json { "any" } else { "string" };
        let yaml = format!(
            r#"
pipeline: {{ name: repeated_text_storage }}
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      path: rows.csv
      split_values: [{{ field: values, {split} }}]
      schema: [{{ name: values, type: {ty}, multiple: true, long_unique: true }}]
  - type: sink
    name: result
    input: rows
    config: {{ name: result, type: json, path: result.json }}
"#
        );
        clinker_plan::config::parse_config(&yaml)
            .unwrap()
            .compile(&clinker_plan::config::CompileContext::default())
            .unwrap();
        let provider = MemoryOnlyResources::new(NonZeroUsize::new(1024 * 1024).unwrap());
        let config = CsvReaderConfig {
            split_values: vec![clinker_format::multi_value::SplitValues {
                field: "values".into(),
                delimiter: ";".into(),
                escape: String::new(),
                json,
            }],
            ..Default::default()
        };
        let input = if json {
            let cell = format!(r#"["{text}",{{"nested":["{text}",null,42]}},["{text}","short"]]"#);
            format!("values\n\"{}\"\n", cell.replace('"', "\"\""))
        } else {
            format!("values\n{text};{text};short\n")
        };
        let columns = [Column {
            multiple: Some(true),
            long_unique: Some(true),
            ..Column::bare("values", if json { Type::Any } else { Type::String })
        }];
        let mut reader = decode_coercion_reader(
            input.into_bytes(),
            config,
            &columns,
            OnUnmapped::Drop,
            provider.resources().allocation().clone(),
        );
        let row = reader.next_record().unwrap().unwrap();
        assert_unique(row.get("values").unwrap());
        drop(row);
        drop(reader);
        assert_eq!(provider.used(), 0);
    }
}

#[test]
fn decode_second_file_header_refusal_remains_resource_error() {
    let root = tempfile::tempdir().unwrap();
    let header = format!("{}\n", "x".repeat(2 * 1024 * 1024));
    let error = decode_file_run(
        root.path(),
        &[b"value\n1\n", header.as_bytes()],
        "",
        "        - { name: value, type: int }",
        "1M",
        None,
    )
    .unwrap_err();
    assert!(
        matches!(error, clinker_plan::error::PipelineError::Format(
        clinker_format::FormatError::Resource(ref error)) if error.kind == ResourceErrorKind::Budget),
        "{error:?}"
    );
}

#[derive(Default)]
struct DecodeFaultAuthority {
    calls: std::sync::atomic::AtomicUsize,
    fail_at: usize,
    fail_allocator_at: usize,
    used: std::sync::atomic::AtomicUsize,
    cancelled: std::sync::atomic::AtomicBool,
}

impl clinker_record::owned_storage::AllocationAuthority for DecodeFaultAuthority {
    fn try_reserve(
        self: Arc<Self>,
        owner: clinker_record::owned_storage::OwnerId,
        layout: Layout,
    ) -> Result<
        clinker_record::owned_storage::AllocationLease,
        clinker_record::owned_storage::ResourceError,
    > {
        use clinker_record::owned_storage::{AllocationLease, ResourceError};
        use std::sync::atomic::Ordering::SeqCst;
        self.check_cancelled()?;
        let call = self.calls.fetch_add(1, SeqCst) + 1;
        if call == self.fail_at {
            return Err(ResourceError::new(
                ResourceErrorKind::Budget,
                layout.size(),
                0,
            ));
        }
        self.used
            .fetch_update(SeqCst, SeqCst, |used| {
                used.checked_add(layout.size())
                    .filter(|n| *n <= 1024 * 1024)
            })
            .map_err(|_| ResourceError::new(ResourceErrorKind::Budget, layout.size(), 0))?;
        let fail_allocator = call == self.fail_allocator_at;
        let lease = AllocationLease::admitted(self, owner, layout.size())?;
        if fail_allocator {
            ALLOCATIONS.with(|count| count.set(Some(0)));
            FAIL_ALLOCATION.with(|fail| fail.set(Some(1)));
        }
        Ok(lease)
    }
    fn release(&self, _: clinker_record::owned_storage::OwnerId, bytes: usize) {
        self.used
            .fetch_sub(bytes, std::sync::atomic::Ordering::SeqCst);
    }
    fn check_cancelled(&self) -> Result<(), clinker_record::owned_storage::ResourceError> {
        if self.cancelled.load(std::sync::atomic::Ordering::SeqCst) {
            Err(clinker_record::owned_storage::ResourceError::new(
                ResourceErrorKind::Cancelled,
                0,
                0,
            ))
        } else {
            Ok(())
        }
    }
}

#[test]
fn decode_coercion_every_reservation_failure_releases_all_owners() {
    use clinker_exec::pipeline::schema_coerce::CoercingReader;
    use clinker_format::{
        Column, FormatReader,
        csv::reader::{CsvReader, CsvReaderConfig},
        preparation::{DecodeWorkspace, TextStorage},
    };
    use clinker_plan::config::pipeline_node::OnUnmapped;
    use clinker_record::owned_storage::AllocationResources;
    use cxl::typecheck::Type;
    let long = "long-independent-text-".repeat(4);
    let nested = format!(r#"[{{"inside":["{long}"]}}]"#).replace('"', "\"\"");
    let input = format!("value,numbers,nested,extra\n{long},1;2,\"{nested}\",{long}\n");
    let columns = [
        Column {
            long_unique: Some(true),
            ..Column::bare("value", Type::String)
        },
        Column {
            multiple: Some(true),
            ..Column::bare("numbers", Type::Int)
        },
        Column {
            multiple: Some(true),
            long_unique: Some(true),
            ..Column::bare("nested", Type::Any)
        },
    ];
    let mut failures = 0;
    for fail_at in 1..200 {
        let authority = Arc::new(DecodeFaultAuthority {
            fail_at,
            ..Default::default()
        });
        let resources = AllocationResources::new(authority.clone());
        let result = (|| -> Result<(), clinker_format::FormatError> {
            let raw = CsvReader::from_reader_admitted(
                std::io::Cursor::new(input.clone()),
                CsvReaderConfig {
                    split_values: vec![
                        clinker_format::multi_value::SplitValues {
                            field: "numbers".into(),
                            delimiter: ";".into(),
                            escape: String::new(),
                            json: false,
                        },
                        clinker_format::multi_value::SplitValues {
                            field: "nested".into(),
                            delimiter: ";".into(),
                            escape: String::new(),
                            json: true,
                        },
                    ],
                    ..Default::default()
                },
                DecodeWorkspace::new(resources.clone())?,
                TextStorage::Shared,
            )?;
            let mut reader = CoercingReader::new_csv_admitted(
                Box::new(raw),
                &columns,
                OnUnmapped::AutoWiden,
                "rows",
                resources,
            )?;
            let row = reader.next_record()?.unwrap();
            assert!(row.values_are_governed());
            Ok(())
        })();
        assert_eq!(
            authority.used.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "leaked grant at refusal {fail_at}"
        );
        match result {
            Err(clinker_format::FormatError::Resource(error)) => {
                assert_eq!(error.kind, ResourceErrorKind::Budget);
                failures += 1;
            }
            Ok(()) => {
                assert!(failures > 30, "must exercise every allocation boundary");
                return;
            }
            other => panic!("typed resource failure required: {other:?}"),
        }
    }
    panic!("fault enumeration never reached success");
}

#[test]
fn decode_replacement_keeps_cached_schema_and_bare_value_until_last_drop() {
    use clinker_exec::source::{
        RecordSource,
        multi_file::{FileSlot, MultiFileFormatReader},
    };
    use clinker_format::{
        FormatReader,
        csv::reader::{CsvReader, CsvReaderConfig},
        preparation::{DecodeWorkspace, MemoryOnlyResources, TextStorage},
    };
    use clinker_record::{Value, owned_storage::SharedStorage};
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(1024 * 1024).unwrap());
    let resources = provider.resources().allocation().clone();
    let first = "first-admitted-text-".repeat(8);
    let second = "second-admitted-text-".repeat(8);
    let mut reader: Box<dyn FormatReader> = Box::new(MultiFileFormatReader::new(
        vec![
            FileSlot::new(
                "first.csv",
                Box::new(std::io::Cursor::new(format!("value\n{first}\n"))),
            ),
            FileSlot::new(
                "second.csv",
                Box::new(std::io::Cursor::new(format!("value\n{second}\n"))),
            ),
        ],
        Box::new(move |source| {
            Ok(Box::new(CsvReader::from_reader_admitted(
                source.open()?,
                CsvReaderConfig::default(),
                DecodeWorkspace::new(resources.clone())?,
                TextStorage::Shared,
            )?))
        }),
    ));
    let schema = RecordSource::schema(&mut reader).unwrap();
    let row = RecordSource::next_record(&mut reader).unwrap().unwrap();
    let value = row.get("value").unwrap().clone();
    let before = provider.used();
    drop(row);
    assert!(
        provider.used() < before,
        "dropping slots releases only their own grant"
    );
    let next = RecordSource::next_record(&mut reader).unwrap().unwrap();
    assert_eq!(next.get("value"), Some(&Value::String(second.into())));
    assert!(SharedStorage::ptr_eq(
        &schema,
        &RecordSource::schema(&mut reader).unwrap()
    ));
    drop(next);
    assert!(RecordSource::next_record(&mut reader).unwrap().is_none());
    drop(reader);
    let before = provider.used();
    drop(schema);
    assert!(provider.used() < before);
    assert!(provider.used() > 0);
    assert_eq!(value, Value::String(first.into()));
    drop(value);
    assert_eq!(provider.used(), 0);
}

#[test]
fn decode_take_value_moves_unique_nested_storage_without_cloning() {
    use clinker_format::preparation::MemoryOnlyResources;
    use clinker_record::{FieldStr, Record, SchemaBuilder, Value, owned_storage::OwnedValues};
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(65536).unwrap());
    let scope = provider.resources().allocation().scope().unwrap();
    let text = FieldStr::try_new_unique(&"unique-".repeat(30), &scope).unwrap();
    let pointer = text.as_str().as_ptr();
    let mut nested = OwnedValues::try_with_capacity(1, &scope).unwrap();
    nested.try_push(Value::String(text), &scope).unwrap();
    let mut slots = OwnedValues::try_with_capacity(1, &scope).unwrap();
    slots.try_push(Value::Array(nested), &scope).unwrap();
    let mut row =
        Record::from_owned_values(SchemaBuilder::new().with_field("nested").build(), slots)
            .unwrap();
    let before = provider.used();
    let value = row.take_value_at(0).unwrap();
    assert_eq!(provider.used(), before);
    assert!(matches!(row.get("nested"), Some(Value::Null)));
    drop(row);
    assert!(provider.used() < before);
    let Value::Array(items) = &value else {
        panic!("nested")
    };
    let Value::String(text) = &items[0] else {
        panic!("unique")
    };
    assert_eq!(text.as_str().as_ptr(), pointer);
    assert_eq!(text.legacy_heap_size(), 0);
    assert!(items.is_governed());
    drop(value);
    assert_eq!(provider.used(), 0);
}

#[test]
fn decode_ordered_spill_reload_keeps_original_aliases_and_context_owners() {
    use clinker_exec::pipeline::sort_buffer::{SortBuffer, SortedOutput};
    use clinker_format::{Column, csv::reader::CsvReaderConfig};
    use clinker_plan::config::{SortField, SortOrder, pipeline_node::OnUnmapped};
    use clinker_record::owned_storage::{OwnedKey, OwnedMap, OwnedValues};
    use clinker_record::{
        AdmittedSchemaBuilder, DocumentContext, DocumentId, EnvelopeRecord, FieldStr,
        RecordPayload, Value,
    };
    use cxl::typecheck::Type;
    let arb = Arc::new(MemoryArbitrator::with_policy(
        16 * 1024 * 1024,
        0.8,
        0.7,
        Box::new(NoOpPolicy),
    ));
    let observer = arb.writer_resource_observer();
    let provider = ExecutorResources::new(
        arb.clone(),
        ShutdownToken::detached(),
        None,
        NonZeroUsize::new(1).unwrap(),
        None,
    )
    .unwrap();
    let scope = provider.allocation().scope().unwrap();
    let text = "original-decoded-text-".repeat(8);
    let mut reader = decode_coercion_reader(
        format!("value\n{text}\n").into_bytes(),
        CsvReaderConfig::default(),
        &[Column::bare("value", Type::String)],
        OnUnmapped::Drop,
        provider.allocation(),
    );
    let mut row = reader.next_record().unwrap().unwrap();
    let schema = row.schema().clone();
    // Exercise an existing admitted context crossing coercion/spill. CSV
    // multi-record section construction is tested with its own document carrier.
    let mut section = OwnedMap::try_with_capacity(1, &scope).unwrap();
    section
        .try_insert(
            OwnedKey::try_new("origin", &scope).unwrap(),
            Value::String(FieldStr::try_new(&text, &scope).unwrap()),
            &scope,
        )
        .unwrap();
    let mut sections = OwnedValues::try_with_capacity(1, &scope).unwrap();
    sections.try_push(Value::Map(section), &scope).unwrap();
    let mut builder = AdmittedSchemaBuilder::try_with_capacity(1, &scope).unwrap();
    builder
        .try_push(
            OwnedKey::try_new("batch_info", &scope).unwrap(),
            None,
            &scope,
        )
        .unwrap();
    let context = DocumentContext::try_new(
        DocumentId::next(),
        Arc::from("input.csv"),
        EnvelopeRecord::from_owned_values(builder.finish(&scope).unwrap(), sections).unwrap(),
        &scope,
    )
    .unwrap();
    row.set_doc_ctx(context.clone());
    let alias = row.get("value").unwrap().clone();
    let previous = row.clone();
    let payload = RecordPayload::from_record(&row);
    let root = tempfile::tempdir().unwrap();
    let mut sorter = SortBuffer::<()>::new(
        vec![SortField {
            field: "value".into(),
            order: SortOrder::Asc,
            null_order: None,
        }],
        1,
        Some(root.path().to_owned()),
        false,
        schema.clone(),
        provider.allocation(),
    );
    sorter.push(row, ());
    assert!(sorter.should_spill());
    assert!(
        sorter.sort_and_spill().unwrap() > 0,
        "must write an actual sorted run"
    );
    let (SortedOutput::Spilled(files), _) = sorter.finish().unwrap() else {
        panic!("must spill")
    };
    assert_eq!(files.len(), 1);
    assert!(files[0].bytes() > 0);
    let mut reload = files[0].reader().unwrap();
    let (mut reloaded, ()) = reload.next().unwrap().unwrap();
    assert!(reload.next().is_none());
    let Value::String(original) = &alias else {
        panic!("text")
    };
    let Value::String(copy) = reloaded.get("value").unwrap() else {
        panic!("text")
    };
    assert_eq!(copy.as_str(), original.as_str());
    assert_ne!(copy.as_str().as_ptr(), original.as_str().as_ptr());
    assert!(
        copy.legacy_heap_size() > 0,
        "reload is an independent legacy allocation"
    );
    assert!(!reloaded.values_are_governed());
    // The ordered barrier reattaches original document attribution after
    // deserialization; that exact shared owner must survive the run too.
    reloaded.set_doc_ctx(context.clone());
    drop(reload);
    drop(files);
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    drop(reader);
    drop(schema);
    drop(provider);
    arb.close_writer_resources();
    assert_eq!(arb.consumer_count(), 0);
    drop(arb);
    assert!(observer.is_closed());
    assert!(observer.usage().memory > 0);
    drop(previous);
    drop(alias);
    assert!(
        observer.usage().memory > 0,
        "serializer payload still aliases original text"
    );
    drop(payload);
    drop(context);
    assert!(
        observer.usage().memory > 0,
        "reattached original context is still owned"
    );
    drop(reloaded);
    assert_eq!(observer.usage().memory, 0);
}

#[test]
fn decode_dispatch_paths_dead_letter_decoded_source_text() {
    // Decode-time ownership is covered at the coercion layer by schema_coerce.rs tests csv_multirecord_projection_moves_nested_owners_and_logical_names and csv_multirecord_projection_uses_local_repeated_text_policy.
    use clinker_exec::executor::{PipelineExecutor, PipelineRunParams, single_file_reader};
    for mode in ["fused", "merge", "fanout"] {
        let text = "detached-source-text-".repeat(40);
        let source = |name: &str| {
            format!(
                r#"
  - type: source
    name: {name}
    config:
      name: {name}
      type: csv
      path: {name}.csv
      schema:
        - {{ name: exposed, source_name: physical, type: string }}
"#
            )
        };
        let mut yaml = format!(
            r#"
pipeline:
  name: decoded_ownership
  memory: {{ limit: 256M }}
error_handling:
  strategy: continue
  dlq: {{ path: rejected.csv }}
nodes:
{}"#,
            source("rows")
        );
        let mut inputs = std::collections::HashMap::from([(
            "rows".into(),
            single_file_reader(
                "rows.csv",
                Box::new(std::io::Cursor::new(format!(
                    "physical,extra\n{text},{text}\n"
                ))),
            ),
        )]);
        let upstream = if mode == "merge" {
            yaml.push_str(&source("peer"));
            yaml.push_str("  - type: merge\n    name: merged\n    inputs: [rows, peer]\n    config: { mode: interleave }\n");
            inputs.insert(
                "peer".into(),
                single_file_reader(
                    "peer.csv",
                    Box::new(std::io::Cursor::new(format!(
                        "physical,extra\n{text},{text}\n"
                    ))),
                ),
            );
            "merged"
        } else {
            "rows"
        };
        let branches = if mode == "fanout" { 2 } else { 1 };
        let sink = dlq_sink::CollectingDlqSink::new();
        let mut writers = dlq_sink::registry(std::collections::HashMap::new(), &sink);
        for index in 0..branches {
            yaml.push_str(&format!(
                r#"  - type: transform
    name: reject_{index}
    input: {upstream}
    config:
      cxl: "emit failure = 1 / 0"
  - type: sink
    name: out_{index}
    input: reject_{index}
    config:
      name: out_{index}
      type: csv
      path: out_{index}.csv
"#
            ));
            writers
                .single
                .insert(format!("out_{index}"), Box::new(std::io::sink()));
        }
        let plan = clinker_plan::config::parse_config(&yaml)
            .unwrap()
            .compile(&clinker_plan::config::CompileContext::default())
            .unwrap();
        let report = PipelineExecutor::run_plan_with_readers_writers(
            &plan,
            inputs,
            writers,
            &PipelineRunParams::default(),
        )
        .unwrap();
        let expected = if mode == "fused" { 1 } else { 2 };
        assert_eq!(report.counters.dlq_count, expected, "{mode}");
        let rows = sink.rows();
        assert_eq!(rows.len() as u64, expected, "{mode}");
        let header = sink
            .header_for("rejected.csv")
            .expect("the rejected rows reach the dead-letter file");
        assert!(header.iter().any(|column| column == "exposed"), "{mode}");
        for row in &rows {
            assert_eq!(row.field("exposed"), Some(text.as_str()), "{mode}");
            assert!(row.source_file().ends_with(".csv"), "{mode}");
        }
    }
}

#[test]
fn decode_coercion_cancellation_precedes_schema_and_subsequent_reads() {
    use clinker_exec::pipeline::schema_coerce::CoercingReader;
    use clinker_format::{Column, FormatReader};
    use clinker_plan::config::pipeline_node::OnUnmapped;
    use clinker_record::{
        Record, Schema, SchemaBuilder, Value,
        owned_storage::{AllocationResources, SharedStorage},
    };
    use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
    struct ObservedReader {
        calls: Arc<AtomicUsize>,
        schema: SharedStorage<Schema>,
    }
    impl FormatReader for ObservedReader {
        fn schema(&mut self) -> Result<SharedStorage<Schema>, clinker_format::FormatError> {
            self.calls.fetch_add(1, SeqCst);
            Ok(self.schema.clone())
        }
        fn next_record(&mut self) -> Result<Option<Record>, clinker_format::FormatError> {
            self.calls.fetch_add(1, SeqCst);
            Ok(Some(Record::new(
                self.schema.clone(),
                vec![Value::Integer(1)],
            )))
        }
    }
    for before_schema in [true, false] {
        let authority = Arc::new(DecodeFaultAuthority::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let raw = ObservedReader {
            calls: calls.clone(),
            schema: SchemaBuilder::new().with_field("value").build(),
        };
        authority.cancelled.store(before_schema, SeqCst);
        let result = CoercingReader::new_csv_admitted(
            Box::new(raw),
            &[Column::bare("value", cxl::typecheck::Type::Int)],
            OnUnmapped::Drop,
            "rows",
            AllocationResources::new(authority.clone()),
        );
        let error = if before_schema {
            assert_eq!(calls.load(SeqCst), 0);
            result.err().unwrap()
        } else {
            let mut reader = result.unwrap();
            assert!(reader.next_record().unwrap().is_some());
            let before = calls.load(SeqCst);
            authority.cancelled.store(true, SeqCst);
            let error = reader.next_record().unwrap_err();
            assert_eq!(calls.load(SeqCst), before);
            error
        };
        assert!(
            matches!(error, clinker_format::FormatError::Resource(error) if error.kind == ResourceErrorKind::Cancelled)
        );
        assert_eq!(authority.used.load(SeqCst), 0);
    }
}

fn decode_file_run(
    root: &std::path::Path,
    inputs: &[&[u8]],
    source_options: &str,
    schema: &str,
    memory_limit: &str,
    shutdown: Option<ShutdownToken>,
) -> Result<clinker_exec::executor::ExecutionReport, clinker_plan::error::PipelineError> {
    decode_file_run_with_params(
        root,
        inputs,
        source_options,
        schema,
        memory_limit,
        &clinker_exec::executor::PipelineRunParams {
            shutdown_token: shutdown,
            ..Default::default()
        },
    )
}

fn decode_file_run_with_params(
    root: &std::path::Path,
    inputs: &[&[u8]],
    source_options: &str,
    schema: &str,
    memory_limit: &str,
    params: &clinker_exec::executor::PipelineRunParams,
) -> Result<clinker_exec::executor::ExecutionReport, clinker_plan::error::PipelineError> {
    decode_file_run_into(
        root,
        inputs,
        source_options,
        schema,
        memory_limit,
        params,
        Arc::new(clinker_exec::dlq::DiscardingDlqSink),
    )
}

/// [`decode_file_run`], returning the dead-letter rows the run wrote as well.
fn decode_file_run_collecting(
    root: &std::path::Path,
    inputs: &[&[u8]],
    source_options: &str,
    schema: &str,
    memory_limit: &str,
) -> Result<
    (
        clinker_exec::executor::ExecutionReport,
        Vec<dlq_sink::DlqRow>,
    ),
    clinker_plan::error::PipelineError,
> {
    let sink = dlq_sink::CollectingDlqSink::new();
    let report = decode_file_run_into(
        root,
        inputs,
        source_options,
        schema,
        memory_limit,
        &clinker_exec::executor::PipelineRunParams::default(),
        sink.clone(),
    )?;
    Ok((report, sink.rows()))
}

fn decode_file_run_into(
    root: &std::path::Path,
    inputs: &[&[u8]],
    source_options: &str,
    schema: &str,
    memory_limit: &str,
    params: &clinker_exec::executor::PipelineRunParams,
    dlq_sink: Arc<dyn clinker_exec::dlq::DlqSink>,
) -> Result<clinker_exec::executor::ExecutionReport, clinker_plan::error::PipelineError> {
    use clinker_exec::executor::{PipelineExecutor, WriterRegistry};
    use clinker_exec::source::{SourceInput, multi_file::FileSlot};
    let yaml = format!(
        r#"
pipeline:
  name: admitted_csv_ingest
  memory:
    limit: {memory_limit}
    backpressure: spill
error_handling:
  strategy: continue
  dlq: {{ path: rejected.csv }}
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      path: input.csv
{source_options}
      schema:
{schema}
  - type: sink
    name: result
    input: rows
    config:
      name: result
      type: csv
      path: output.csv
"#
    );
    let plan = clinker_plan::config::parse_config(&yaml)
        .unwrap()
        .compile(&clinker_plan::config::CompileContext::default())
        .unwrap();
    let files = inputs
        .iter()
        .enumerate()
        .map(|(index, bytes)| {
            let path = root.join(format!("input-{index}.csv"));
            std::fs::write(&path, bytes).unwrap();
            FileSlot::new(path.clone(), Box::new(std::fs::File::open(path).unwrap()))
        })
        .collect();
    let writers = WriterRegistry {
        single: [(
            "result".into(),
            Box::new(std::fs::File::create(root.join("output.csv")).unwrap())
                as Box<dyn Write + Send>,
        )]
        .into(),
        dlq_sink: Some(dlq_sink),
        ..Default::default()
    };
    PipelineExecutor::run_plan_with_readers_writers(
        &plan,
        [("rows".into(), SourceInput::Files(files))].into(),
        writers,
        params,
    )
}

#[test]
fn decode_single_schema_ingest_tracer() {
    // Decode-time ownership is covered at the coercion layer by schema_coerce.rs tests csv_multirecord_projection_moves_nested_owners_and_logical_names and csv_multirecord_projection_uses_local_repeated_text_policy.
    let root = tempfile::tempdir().unwrap();
    let invalid = "not-an-integer-".repeat(100);
    let input = format!("value\n42\n{invalid}\n");
    let (report, rows) = decode_file_run_collecting(
        root.path(),
        &[input.as_bytes()],
        "",
        "        - { name: value, type: int }",
        "256M",
    )
    .unwrap();
    assert_eq!(
        std::fs::read(root.path().join("output.csv")).unwrap(),
        b"value\n42\n"
    );
    assert_eq!(report.counters.total_count, 2);
    assert_eq!(report.counters.dlq_count, 1);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].field("value"),
        Some(invalid.as_str()),
        "the rejected row must retain decoded text"
    );
}

#[test]
fn decode_physical_files_preserve_latin1_bytes_and_rejected_text() {
    // Decode-time ownership is covered at the coercion layer by schema_coerce.rs tests csv_multirecord_projection_moves_nested_owners_and_logical_names and csv_multirecord_projection_uses_local_repeated_text_policy.
    let root = tempfile::tempdir().unwrap();
    let invalid = "é".repeat(100);
    let mut first = b"value\n42\n".to_vec();
    first.extend(std::iter::repeat_n(0xe9, 100));
    first.push(b'\n');
    let mut second = b"value\n43\n".to_vec();
    second.extend(std::iter::repeat_n(0xe9, 100));
    second.push(b'\n');
    let (report, rows) = decode_file_run_collecting(
        root.path(),
        &[&first, &second],
        "      options: { encoding: iso-8859-1 }",
        "        - { name: value, type: int }",
        "256M",
    )
    .unwrap();
    assert_eq!(report.counters.total_count, 4);
    assert_eq!(report.counters.dlq_count, 2);
    assert_eq!(rows.len(), 2);
    assert_eq!(
        std::fs::read(root.path().join("output.csv")).unwrap(),
        b"value\n42\n43\n"
    );
    for row in &rows {
        assert_eq!(
            row.field("value"),
            Some(invalid.as_str()),
            "original decoded text must survive each physical reader"
        );
    }
}

#[test]
fn decode_split_json_rejection_dead_letters_nested_original_values() {
    // Decode-time ownership is covered at the coercion layer by schema_coerce.rs tests csv_multirecord_projection_moves_nested_owners_and_logical_names and csv_multirecord_projection_uses_local_repeated_text_policy.
    for json in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let long = "invalid-integer-".repeat(20);
        let (input, options) = if json {
            (
                format!("value\n\"[\"\"{long}\"\",null]\"\n"),
                "      split_values: [{ field: value, json: true }]",
            )
        } else {
            (
                format!("value\n{long};42\n"),
                "      split_values: [{ field: value, delimiter: ';' }]",
            )
        };
        let (report, rows) = decode_file_run_collecting(
            root.path(),
            &[input.as_bytes()],
            options,
            "        - { name: value, type: int, multiple: true }",
            "256M",
        )
        .unwrap();
        assert_eq!(report.counters.dlq_count, 1);
        assert_eq!(rows.len(), 1);
        assert!(
            std::fs::read(root.path().join("output.csv"))
                .unwrap()
                .is_empty()
        );
        // The dead-letter cell is the decoded array as the encoder writes it:
        // two elements, the first the decoded text, the second null for JSON.
        let expected = if json {
            format!("[{{\"String\":\"{long}\"}},\"Null\"]")
        } else {
            format!("[{{\"String\":\"{long}\"}},{{\"String\":\"42\"}}]")
        };
        assert_eq!(
            rows[0].field("value"),
            Some(expected.as_str()),
            "rejection must retain the decoded array"
        );
    }
}

#[test]
fn decode_header_and_body_refusal_remain_typed() {
    for header in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let oversized = "x".repeat(2 * 1024 * 1024);
        let input = if header {
            format!("{oversized}\n")
        } else {
            format!("value\n{oversized}\n")
        };
        let error = decode_file_run(
            root.path(),
            &[input.as_bytes()],
            "",
            "        - { name: value, type: string }",
            "1M",
            None,
        )
        .unwrap_err();
        let clinker_plan::error::PipelineError::Format(clinker_format::FormatError::Resource(
            resource,
        )) = error
        else {
            panic!("decoder allocation must fail with typed budget evidence: {error:?}");
        };
        assert_eq!(resource.kind, ResourceErrorKind::Budget);
        assert!(resource.requested >= oversized.len());
        assert!(resource.available <= 1024 * 1024);
        assert!(resource.requested > resource.available);
        assert!(
            std::fs::read(root.path().join("output.csv"))
                .unwrap()
                .is_empty()
        );
    }
}

#[test]
fn decode_no_header_keeps_pending_first_row_and_utf8_is_strict() {
    let root = tempfile::tempdir().unwrap();
    let report = decode_file_run(
        root.path(),
        &[b"41\n42\n"],
        "      options: { has_header: false }",
        "        - { name: col_0, type: int }",
        "256M",
        None,
    )
    .unwrap();
    assert_eq!(report.counters.total_count, 2);
    assert_eq!(
        std::fs::read(root.path().join("output.csv")).unwrap(),
        b"col_0\n41\n42\n"
    );
    let error = decode_file_run(
        root.path(),
        &[b"value\n\xff\n"],
        "",
        "        - { name: value, type: string }",
        "256M",
        None,
    )
    .unwrap_err();
    assert!(
        matches!(
            error,
            clinker_plan::error::PipelineError::Format(clinker_format::FormatError::Charset(_))
        ),
        "{error:?}"
    );
    assert!(
        std::fs::read(root.path().join("output.csv"))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn interrupted_source_retains_actual_progress_and_watermarks() {
    use clinker_exec::executor::{PipelineExecutor, PipelineRunParams, WriterRegistry};
    use clinker_exec::source::{RecordSource, SourceInput};
    use clinker_format::FormatError;
    use clinker_record::owned_storage::{ResourceError, SharedStorage};
    struct PartialSource {
        schema: SharedStorage<clinker_record::Schema>,
        row: Option<clinker_record::Record>,
        resource: bool,
        rejected: bool,
        dropped: Arc<std::sync::atomic::AtomicBool>,
    }
    impl RecordSource for PartialSource {
        fn schema(&mut self) -> Result<SharedStorage<clinker_record::Schema>, FormatError> {
            Ok(self.schema.clone())
        }
        fn next_record(&mut self) -> Result<Option<clinker_record::Record>, FormatError> {
            if let Some(row) = self.row.take() {
                return Ok(Some(row));
            }
            if !std::mem::replace(&mut self.rejected, true) {
                let original_value = clinker_record::Value::from("invalid timestamp");
                return Err(FormatError::DeclaredType(Box::new(
                    clinker_format::error::DeclaredTypeFailure {
                        source: "rows".into(),
                        column: 1,
                        field: "event_ts".into(),
                        declared_type: "date_time".into(),
                        original_value: original_value.clone(),
                        original_record: clinker_record::Record::new(
                            self.schema.clone(),
                            vec![original_value],
                        ),
                        message: "invalid timestamp".into(),
                    },
                )));
            }
            Err(if self.resource {
                FormatError::Resource(ResourceError::new(ResourceErrorKind::Cancelled, 42, 7))
            } else {
                FormatError::Interrupted
            })
        }
    }
    impl Drop for PartialSource {
        fn drop(&mut self) {
            self.dropped
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }
    let plan = clinker_plan::config::parse_config(
        r#"
pipeline: { name: interrupted_progress }
error_handling:
  strategy: continue
  dlq: { path: rejected.csv }
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      path: input.csv
      watermark: { column: event_ts }
      schema: [{ name: event_ts, type: date_time }]
  - type: sink
    name: result
    input: rows
    config: { name: result, type: csv, path: output.csv }
"#,
    )
    .unwrap()
    .compile(&clinker_plan::config::CompileContext::default())
    .unwrap();
    for resource in [false, true] {
        for telemetry_mode in 0..3 {
            let schema = clinker_record::SchemaBuilder::new()
                .with_field("event_ts")
                .build();
            let timestamp = chrono::DateTime::from_timestamp(42, 0).unwrap().naive_utc();
            let row = clinker_record::Record::new(
                schema.clone(),
                vec![clinker_record::Value::DateTime(timestamp)],
            );
            let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let (producer, receiver) = telemetry();
            if telemetry_mode == 2 {
                use clinker_exec::telemetry::{
                    AdmissionOutcome, DropReason, SpanFact, SpanName, SpanStatus,
                };
                for status in [SpanStatus::Ok, SpanStatus::Error] {
                    loop {
                        if producer.emit_span(SpanFact {
                            name: SpanName::Transform,
                            status,
                            logical_node: "fill",
                            started_at_unix_nanos: 1,
                            ended_at_unix_nanos: 2,
                        }) == AdmissionOutcome::Dropped(DropReason::Full)
                        {
                            break;
                        }
                    }
                }
            }
            let output = clinker_bench_support::io::SharedBuffer::new();
            let report = PipelineExecutor::run_plan_with_readers_writers(
                &plan,
                [(
                    "rows".into(),
                    SourceInput::Records(Box::new(PartialSource {
                        schema,
                        row: Some(row),
                        resource,
                        rejected: false,
                        dropped: dropped.clone(),
                    })),
                )]
                .into(),
                WriterRegistry {
                    single: [("result".into(), Box::new(output) as Box<dyn Write + Send>)].into(),
                    dlq_sink: Some(Arc::new(clinker_exec::dlq::DiscardingDlqSink)),
                    ..Default::default()
                },
                &PipelineRunParams {
                    telemetry_producer: (telemetry_mode > 0).then_some(producer),
                    ..Default::default()
                },
            )
            .expect("explicit source cancellation is a graceful interrupted outcome");
            assert!(report.interrupted);
            assert_eq!(
                report.counters.total_count, 2,
                "one accepted read and one rejected attempt"
            );
            assert_eq!(report.counters.dlq_count, 1);
            assert_eq!(
                report
                    .per_source_file_watermarks
                    .get(&("rows".into(), "input.csv".into())),
                Some(&Some(42_000_000_000))
            );
            assert!(dropped.load(std::sync::atomic::Ordering::SeqCst));
            if telemetry_mode > 0 {
                let batch = receiver.try_recv_batch().unwrap();
                assert_eq!(
                    batch.metric(clinker_exec::telemetry::MetricKey::SourceInterrupted),
                    1
                );
                assert_eq!(
                    batch.metric(clinker_exec::telemetry::MetricKey::SourceFailed),
                    0
                );
            }
        }
    }
}

#[test]
fn multiple_source_failures_beat_cancellation_in_either_join_order() {
    use clinker_exec::executor::{PipelineExecutor, PipelineRunParams, WriterRegistry};
    use clinker_exec::source::{RecordSource, SourceInput};
    use clinker_format::FormatError;
    use clinker_record::owned_storage::{ResourceError, SharedStorage};
    struct FailingSource {
        kind: ResourceErrorKind,
        dropped: Arc<std::sync::atomic::AtomicUsize>,
    }
    impl RecordSource for FailingSource {
        fn schema(&mut self) -> Result<SharedStorage<clinker_record::Schema>, FormatError> {
            Err(FormatError::Resource(ResourceError::new(self.kind, 42, 7)))
        }
        fn next_record(&mut self) -> Result<Option<clinker_record::Record>, FormatError> {
            panic!("a failed schema cannot produce rows");
        }
    }
    impl Drop for FailingSource {
        fn drop(&mut self) {
            self.dropped
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
    let plan = clinker_plan::config::parse_config(
        r#"
pipeline: { name: source_failure_precedence }
nodes:
  - type: source
    name: first
    config: { name: first, type: csv, path: first.csv, schema: [{ name: id, type: int }] }
  - type: source
    name: second
    config: { name: second, type: csv, path: second.csv, schema: [{ name: id, type: int }] }
  - type: merge
    name: combined
    inputs: [first, second]
    config: { mode: concat }
  - type: sink
    name: out
    input: combined
    config: { name: out, type: csv, path: output.csv }
"#,
    )
    .unwrap()
    .compile(&clinker_plan::config::CompileContext::default())
    .unwrap();
    for kinds in [
        [ResourceErrorKind::Cancelled, ResourceErrorKind::Budget],
        [ResourceErrorKind::Budget, ResourceErrorKind::Cancelled],
    ] {
        let dropped = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let readers = ["first", "second"]
            .into_iter()
            .zip(kinds)
            .map(|(name, kind)| {
                (
                    name.into(),
                    SourceInput::Records(Box::new(FailingSource {
                        kind,
                        dropped: dropped.clone(),
                    })),
                )
            })
            .collect();
        let error = PipelineExecutor::run_plan_with_readers_writers(
            &plan,
            readers,
            WriterRegistry {
                single: [(
                    "out".into(),
                    Box::new(clinker_bench_support::io::SharedBuffer::new())
                        as Box<dyn Write + Send>,
                )]
                .into(),
                ..Default::default()
            },
            &PipelineRunParams::default(),
        )
        .unwrap_err();
        assert!(
            matches!(error, clinker_plan::error::PipelineError::Format(FormatError::Resource(resource))
            if resource.kind == ResourceErrorKind::Budget && resource.requested == 42 && resource.available == 7)
        );
        assert_eq!(
            dropped.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "all readers must be gone before returning failure"
        );
    }
}

#[test]
fn decode_cancelled_run_refuses_before_header_publication() {
    let root = tempfile::tempdir().unwrap();
    let shutdown = ShutdownToken::detached();
    shutdown.request();
    let report = decode_file_run(
        root.path(),
        &[b"value\n42\n"],
        "",
        "        - { name: value, type: int }",
        "256M",
        Some(shutdown),
    )
    .expect("cancellation returns a graceful report");
    assert!(report.interrupted);
    assert_eq!(report.counters.total_count, 0);
    assert!(
        std::fs::read(root.path().join("output.csv"))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn decode_cancelled_source_lifecycle_is_independent_of_telemetry_admission() {
    use clinker_exec::telemetry::{
        AdmissionOutcome, DropReason, MetricKey, SpanFact, SpanName, SpanStatus,
    };
    for full in [false, true] {
        let (producer, receiver) = telemetry();
        if full {
            for status in [SpanStatus::Ok, SpanStatus::Error] {
                loop {
                    let result = producer.emit_span(SpanFact {
                        name: SpanName::Transform,
                        status,
                        logical_node: "fill",
                        started_at_unix_nanos: 1,
                        ended_at_unix_nanos: 2,
                    });
                    if result == AdmissionOutcome::Dropped(DropReason::Full) {
                        break;
                    }
                    assert!(matches!(result, AdmissionOutcome::Accepted { .. }));
                }
            }
        }
        let baseline = producer.snapshot();
        let root = tempfile::tempdir().unwrap();
        let shutdown = ShutdownToken::detached();
        shutdown.request();
        let report = decode_file_run_with_params(
            root.path(),
            &[b"value\n42\n"],
            "",
            "        - { name: value, type: int }",
            "256M",
            &clinker_exec::executor::PipelineRunParams {
                shutdown_token: Some(shutdown),
                telemetry_producer: Some(producer.clone()),
                ..Default::default()
            },
        )
        .expect("cancellation returns a graceful report");
        assert!(report.interrupted);
        assert_eq!(report.counters.total_count, 0);
        assert!(
            std::fs::read(root.path().join("output.csv"))
                .unwrap()
                .is_empty()
        );
        assert_eq!(producer.snapshot().owned_bytes, baseline.owned_bytes);
        if full {
            assert_eq!(producer.snapshot().accepted, baseline.accepted);
        }
        let batch = receiver.try_recv_batch().unwrap();
        assert_eq!(batch.metric(MetricKey::SourceStarted), 1);
        assert_eq!(batch.metric(MetricKey::SourceInterrupted), 1);
        assert_eq!(batch.metric(MetricKey::SourceFailed), 0);
        assert_eq!(batch.metric(MetricKey::SourceCompleted), 0);
        let spans: Vec<_> = batch
            .traces()
            .iter()
            .filter(|span| span.name == SpanName::Source)
            .collect();
        if full {
            assert!(spans.is_empty());
        } else {
            assert_eq!(spans.len(), 1);
            assert_eq!(spans[0].status, SpanStatus::Unset);
            assert!(spans[0].started_at_unix_nanos > 0);
            assert!(spans[0].started_at_unix_nanos <= spans[0].ended_at_unix_nanos);
        }
    }
}

#[test]
fn decode_source_failure_stays_failed_when_shutdown_is_requested() {
    use clinker_exec::executor::{PipelineExecutor, PipelineRunParams, WriterRegistry};
    use clinker_exec::source::{RecordSource, SourceInput};
    use clinker_exec::telemetry::{MetricKey, SpanName, SpanStatus};
    use clinker_format::FormatError;
    use clinker_record::owned_storage::{ResourceError, SharedStorage};
    struct FailingSource {
        error: Option<FormatError>,
        shutdown: ShutdownToken,
    }
    impl RecordSource for FailingSource {
        fn schema(&mut self) -> Result<SharedStorage<clinker_record::Schema>, FormatError> {
            self.shutdown.request();
            Err(self.error.take().expect("schema requested once"))
        }
        fn next_record(&mut self) -> Result<Option<clinker_record::Record>, FormatError> {
            panic!("schema failure must prevent record reads");
        }
    }
    let plan = clinker_plan::config::parse_config(
        r#"
pipeline:
  name: source_failure
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      path: input.csv
      schema: [{ name: value, type: int }]
  - type: sink
    name: result
    input: rows
    config: { name: result, type: csv, path: output.csv }
"#,
    )
    .unwrap()
    .compile(&clinker_plan::config::CompileContext::default())
    .unwrap();
    for telemetry_enabled in [false, true] {
        for failure in [
            FormatError::Resource(ResourceError::new(ResourceErrorKind::Budget, 42, 7)),
            FormatError::Charset("invalid byte".into()),
        ] {
            let expected = failure.to_string();
            let (producer, receiver) = telemetry();
            let shutdown = ShutdownToken::detached();
            let output = clinker_bench_support::io::SharedBuffer::new();
            let error = PipelineExecutor::run_plan_with_readers_writers(
                &plan,
                [(
                    "rows".into(),
                    SourceInput::Records(Box::new(FailingSource {
                        error: Some(failure),
                        shutdown: shutdown.clone(),
                    })),
                )]
                .into(),
                WriterRegistry {
                    single: [(
                        "result".into(),
                        Box::new(output.clone()) as Box<dyn Write + Send>,
                    )]
                    .into(),
                    ..Default::default()
                },
                &PipelineRunParams {
                    shutdown_token: Some(shutdown.clone()),
                    telemetry_producer: telemetry_enabled.then_some(producer),
                    ..Default::default()
                },
            )
            .unwrap_err();
            assert!(shutdown.is_requested());
            let clinker_plan::error::PipelineError::Format(actual) = error else {
                panic!("shutdown must not mask a source failure: {error:?}");
            };
            assert_eq!(actual.to_string(), expected);
            assert!(output.contents().is_empty());
            if telemetry_enabled {
                let batch = receiver.try_recv_batch().unwrap();
                assert_eq!(batch.metric(MetricKey::SourceStarted), 1);
                assert_eq!(batch.metric(MetricKey::SourceFailed), 1);
                assert_eq!(batch.metric(MetricKey::SourceInterrupted), 0);
                assert_eq!(batch.metric(MetricKey::SourceCompleted), 0);
                let spans: Vec<_> = batch
                    .traces()
                    .iter()
                    .filter(|span| span.name == SpanName::Source)
                    .collect();
                assert_eq!(spans.len(), 1);
                assert_eq!(spans[0].status, SpanStatus::Error);
                assert!(spans[0].started_at_unix_nanos > 0);
                assert!(spans[0].started_at_unix_nanos <= spans[0].ended_at_unix_nanos);
            }
        }
    }
}

#[test]
fn allocation_capability_clone_and_query_do_not_allocate() {
    let arb = Arc::new(MemoryArbitrator::with_policy(
        4096,
        0.8,
        0.7,
        Box::new(NoOpPolicy),
    ));
    let weak = Arc::downgrade(&arb);
    let observer = arb.writer_resource_observer();
    let provider = ExecutorResources::new(
        arb.clone(),
        ShutdownToken::detached(),
        None,
        NonZeroUsize::MIN,
        None,
    )
    .unwrap();
    let allocation = provider.allocation();
    let writers = provider.resources();
    let scope = allocation.scope().unwrap();
    let value = clinker_record::FieldStr::try_new(
        "a governed string long enough to require shared heap backing",
        &scope,
    )
    .unwrap();
    let lease = scope.reserve(Layout::new::<[u8; 64]>()).unwrap();
    let foreign =
        clinker_format::preparation::MemoryOnlyResources::new(NonZeroUsize::new(4096).unwrap());
    let foreign_resources = foreign.resources();
    let foreign_lease = foreign_resources
        .allocation()
        .reserve(scope.owner(), Layout::new::<[u8; 64]>())
        .unwrap();
    assert_eq!(lease.owner(), foreign_lease.owner());
    let charged = observer.usage().memory;
    assert!(charged > 64);

    ALLOCATIONS.with(|count| count.set(Some(0)));
    let cloned = allocation.clone();
    let same_adapter = cloned.identity() == writers.allocation().identity();
    let local_bytes = value.unaccounted_heap_size(&cloned);
    let foreign_bytes = value.unaccounted_heap_size(foreign_resources.allocation());
    let local_lease = lease.is_accounted_by(writers.allocation());
    let unrelated_lease = foreign_lease.is_accounted_by(&cloned);
    let allocations = ALLOCATIONS.with(|count| count.replace(None).unwrap());
    assert_eq!(
        allocations, 0,
        "cloning capabilities and borrowed queries allocate nothing"
    );
    assert!(same_adapter);
    assert_eq!(local_bytes, 0);
    assert!(foreign_bytes > 0);
    assert!(local_lease);
    assert!(!unrelated_lease);
    assert_eq!(observer.usage().memory, charged);

    drop(writers);
    drop(provider);
    drop(arb);
    assert!(
        weak.upgrade().is_none(),
        "allocation capabilities must not retain the run"
    );
    assert!(observer.is_closed());
    assert!(!observer.has_managed_handle());
    assert_eq!(observer.usage().memory, charged);
    assert_eq!(
        cloned.scope().err().unwrap().kind,
        ResourceErrorKind::Finalized
    );
    drop(value);
    assert_eq!(observer.usage().memory, 64);
    drop(lease);
    assert_eq!(observer.usage().memory, 0);
}

#[test]
fn allocation_release_after_run_retains_only_live_charge() {
    let arb = Arc::new(MemoryArbitrator::with_policy(
        1024,
        0.8,
        0.7,
        Box::new(NoOpPolicy),
    ));
    let weak = Arc::downgrade(&arb);
    let observer = arb.writer_resource_observer();
    let provider = ExecutorResources::new(
        arb.clone(),
        ShutdownToken::detached(),
        None,
        NonZeroUsize::new(1).unwrap(),
        None,
    )
    .unwrap();
    let allocation = provider.allocation();
    let scope = allocation.scope().unwrap();
    let lease = scope.reserve(Layout::new::<[u8; 64]>()).unwrap();
    assert_eq!(arb.consumer_count(), 1);
    assert!(observer.has_managed_handle());
    drop(provider);
    arb.close_writer_resources();
    assert_eq!(
        arb.consumer_count(),
        0,
        "escaped authority is not a registered run consumer"
    );
    assert!(!observer.has_managed_handle());
    assert_eq!(observer.usage().memory, 64);
    assert_eq!(
        scope.reserve(Layout::new::<u8>()).err().unwrap().kind,
        ResourceErrorKind::Finalized
    );
    drop(arb);
    assert!(
        weak.upgrade().is_none(),
        "the release observer and scope must not retain the run"
    );
    assert!(observer.is_closed());
    assert_eq!(observer.usage().memory, 64);
    assert_eq!(
        allocation.scope().err().unwrap().kind,
        ResourceErrorKind::Finalized
    );
    ALLOCATIONS.with(|count| count.set(Some(0)));
    drop(lease);
    let allocations = ALLOCATIONS.with(|count| count.replace(None).unwrap());
    assert_eq!(allocations, 0, "release after teardown needs no allocation");
    assert_eq!(observer.usage().memory, 0);
    assert_eq!(observer.usage().peak_memory, 64);
}

#[test]
fn allocation_release_after_run_drop_closes_without_explicit_shutdown() {
    let arb = Arc::new(MemoryArbitrator::with_policy(
        1024,
        0.8,
        0.7,
        Box::new(NoOpPolicy),
    ));
    let observer = arb.writer_resource_observer();
    let provider = ExecutorResources::new(
        arb.clone(),
        ShutdownToken::detached(),
        None,
        NonZeroUsize::new(1).unwrap(),
        None,
    )
    .unwrap();
    let scope = provider.allocation().scope().unwrap();
    let lease = scope.reserve(Layout::new::<u64>()).unwrap();
    drop(provider);
    drop(arb);
    assert!(observer.is_closed());
    assert!(!observer.has_managed_handle());
    assert_eq!(observer.usage().memory, 8);
    drop(lease);
    assert_eq!(observer.usage().memory, 0);
}

#[test]
fn allocation_release_after_run_preserves_actual_disk_cleanup_result() {
    for restore_before_drop in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let arb = Arc::new(MemoryArbitrator::with_policy(
            128 * 1024,
            0.8,
            0.7,
            Box::new(NoOpPolicy),
        ));
        let weak = Arc::downgrade(&arb);
        let observer = arb.writer_resource_observer();
        let provider = ExecutorResources::new(
            arb.clone(),
            ShutdownToken::detached(),
            Some(&configured(root.path())),
            NonZeroUsize::new(1).unwrap(),
            None,
        )
        .unwrap();
        let lease = provider
            .allocation()
            .scope()
            .unwrap()
            .reserve(Layout::new::<[u8; 64]>())
            .unwrap();
        let mut stage = provider.resources().scope().unwrap().stage().unwrap();
        stage.write_all(&vec![1; 100 * 1024]).unwrap();
        let prepared = stage.finish().unwrap();
        assert_eq!(observer.usage().disk, 100 * 1024);
        assert_eq!(observer.usage().descriptors, 1);
        let path = std::fs::read_dir(root.path())
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let saved = root.path().join("retained");
        std::fs::rename(&path, &saved).unwrap();
        std::fs::create_dir(&path).unwrap();
        drop(prepared);
        assert_eq!(provider.cleanup_debt_count(), 1);
        drop(provider);
        assert_eq!(arb.retry_writer_cleanup(), 1);
        assert_eq!(arb.consumer_count(), 1);
        if restore_before_drop {
            std::fs::remove_dir(&path).unwrap();
            std::fs::rename(&saved, &path).unwrap();
        }
        drop(arb);
        assert!(weak.upgrade().is_none());
        assert!(observer.is_closed());
        assert!(!observer.has_managed_handle());
        assert_eq!(
            observer.usage().memory,
            64,
            "cleanup metadata has actually been dropped"
        );
        assert_eq!(
            observer.usage().descriptors,
            0,
            "the owned file was closed before debt retention"
        );
        assert_eq!(
            observer.usage().disk,
            if restore_before_drop { 0 } else { 100 * 1024 }
        );
        if restore_before_drop {
            assert!(!path.exists());
        } else {
            assert_eq!(std::fs::metadata(&saved).unwrap().len(), 100 * 1024);
        }
        drop(lease);
        assert_eq!(observer.usage().memory, 0);
    }
}

#[test]
fn allocation_close_release_and_admission_are_serialized() {
    for _ in 0..16 {
        let arb = Arc::new(MemoryArbitrator::with_policy(
            64,
            0.8,
            0.7,
            Box::new(NoOpPolicy),
        ));
        let provider = ExecutorResources::new(
            arb.clone(),
            ShutdownToken::detached(),
            None,
            NonZeroUsize::new(1).unwrap(),
            None,
        )
        .unwrap();
        let scope = provider.allocation().scope().unwrap();
        let lease = scope.reserve(Layout::new::<[u8; 64]>()).unwrap();
        let barrier = std::sync::Barrier::new(3);
        std::thread::scope(|threads| {
            threads.spawn(|| {
                barrier.wait();
                drop(lease);
            });
            threads.spawn(|| {
                barrier.wait();
                match scope.reserve(Layout::new::<[u8; 64]>()) {
                    Ok(lease) => drop(lease),
                    Err(error) => assert!(matches!(
                        error.kind,
                        ResourceErrorKind::Budget | ResourceErrorKind::Finalized
                    )),
                }
            });
            barrier.wait();
            arb.close_writer_resources();
        });
        assert_eq!(arb.writer_resource_usage().memory, 0);
        assert!(arb.writer_resource_usage().peak_memory <= 64);
        assert_eq!(arb.consumer_count(), 0);
        assert_eq!(
            scope.reserve(Layout::new::<u8>()).err().unwrap().kind,
            ResourceErrorKind::Finalized
        );
    }
}

#[test]
fn allocation_shutdown_before_or_after_reservation_preserves_release() {
    for cancel_first in [true, false] {
        let arb = Arc::new(MemoryArbitrator::with_policy(
            64,
            0.8,
            0.7,
            Box::new(NoOpPolicy),
        ));
        let token = ShutdownToken::detached();
        let provider = ExecutorResources::new(
            arb.clone(),
            token.clone(),
            None,
            NonZeroUsize::new(1).unwrap(),
            None,
        )
        .unwrap();
        let scope = provider.allocation().scope().unwrap();
        let observer = arb.writer_resource_observer();
        if cancel_first {
            token.request();
        }
        let lease = scope.reserve(Layout::new::<[u8; 64]>());
        if cancel_first {
            assert_eq!(lease.err().unwrap().kind, ResourceErrorKind::Cancelled);
            assert_eq!(observer.usage().memory, 0);
        } else {
            let lease = lease.unwrap();
            token.request();
            assert_eq!(
                scope.reserve(Layout::new::<u8>()).err().unwrap().kind,
                ResourceErrorKind::Cancelled
            );
            assert_eq!(observer.usage().memory, 64);
            drop(provider);
            drop(arb);
            assert!(observer.is_closed());
            drop(lease);
            assert_eq!(observer.usage().memory, 0);
        }
    }
}

#[derive(Default)]
enum CsvRuntimeFault {
    #[default]
    None,
    InvalidBody,
    Prefix(Arc<std::sync::atomic::AtomicUsize>),
}

#[test]
fn json_runtime_identity_tracer_compiled_memory_and_spill_exact_bytes() {
    use clinker_exec::executor::{PipelineExecutor, PipelineRunParams, WriterRegistry};
    use clinker_exec::telemetry::MetricKey;
    let yaml = r#"
pipeline:
  name: prepared_json
  memory: { limit: 256M }
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: json
      path: input.json
      schema:
        - { name: address.city, type: string }
        - { name: items, type: any }
  - type: sink
    name: result
    input: rows
    config:
      name: result
      type: json
      path: output.json
"#;
    let plan = clinker_plan::config::parse_config(yaml)
        .unwrap()
        .compile(&clinker_plan::config::CompileContext::default())
        .unwrap();
    let city = "x".repeat(100_000);
    let input = format!("[{{\"address\":{{\"city\":\"{city}\"}},\"items\":[7,true,null]}}]");
    let expected =
        format!("[\n{{\"address\":{{\"city\":\"{city}\"}},\"items\":[7,true,null]}}\n]\n");
    for spill in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let destination = root.path().join("output.json");
        let staging = clinker_exec::output::staging::OutputStagingRegistry::default();
        let (_, file) = staging
            .stage_output(
                "result",
                clinker_plan::config::IfExistsPolicy::Error,
                false,
                |_| Ok(destination.clone()),
            )
            .unwrap();
        let registry = WriterRegistry {
            single: [("result".into(), Box::new(file) as Box<dyn Write + Send>)].into(),
            output_staging: staging,
            ..Default::default()
        };
        let readers = [(
            "rows".into(),
            clinker_exec::executor::single_file_reader(
                "input.json",
                Box::new(std::io::Cursor::new(input.as_bytes().to_vec())),
            ),
        )]
        .into();
        let (producer, receiver) = telemetry();
        let params = PipelineRunParams {
            spill_root_dir: spill.then(|| root.path().to_owned()),
            spill_disk_cap_bytes: Some(1024 * 1024),
            telemetry_producer: Some(producer),
            ..Default::default()
        };
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, registry, &params).unwrap();
        assert_eq!(std::fs::read(&destination).unwrap(), expected.as_bytes());
        let mut spills = 0;
        let mut stages = 0;
        while let Some(batch) = receiver.try_recv_batch() {
            spills += batch.metric(MetricKey::WriterSpillCompleted);
            stages += batch.metric(MetricKey::WriterStageCompleted);
        }
        assert!(stages >= 2, "runtime must reach prepared operations");
        assert_eq!(spills > 0, spill, "the spill run must really spill");
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
    }
}

#[derive(Clone, Copy, Default)]
struct NativeBacking {
    pointer: usize,
    bytes: usize,
    admitted_at_allocation: u64,
    admitted_at_deallocation: Option<u64>,
}

#[derive(Clone, Copy)]
struct NativeBackingWatch {
    observer: *const clinker_exec::pipeline::memory::reservation::WriterResourceObserver,
    capture: Option<usize>,
    capture_last: bool,
    backings: [NativeBacking; 3],
}

thread_local! {
    static NATIVE_BACKINGS: std::cell::Cell<Option<NativeBackingWatch>> = const { std::cell::Cell::new(None) };
}

#[test]
fn json_runtime_identity_tracer_factory_writer_and_config_deallocate_before_release() {
    use clinker_format::counting::{CountingWriter, SharedByteCounter};
    use clinker_format::json::writer::{JsonEncoder, JsonEncoderConfig, JsonWriterConfig};
    use clinker_format::preparation::PreparedWriter;
    use clinker_format::splitting::WriterFactory;
    use clinker_record::owned_storage::SharedStorage;
    use clinker_record::{Record, Schema, Value};
    let arb = Arc::new(MemoryArbitrator::with_policy(
        256 * 1024,
        0.8,
        0.7,
        Box::new(NoOpPolicy),
    ));
    let shutdown = ShutdownToken::detached();
    let provider = ExecutorResources::new(
        arb.clone(),
        shutdown.clone(),
        None,
        NonZeroUsize::new(1).unwrap(),
        None,
    )
    .unwrap();
    let resources = provider.resources();
    let scope = resources.scope().unwrap();
    let observer = arb.writer_resource_observer();
    NATIVE_BACKINGS.with(|watch| {
        watch.set(Some(NativeBackingWatch {
            observer: &observer,
            capture: Some(0),
            capture_last: false,
            backings: [NativeBacking::default(); 3],
        }))
    });
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            NATIVE_BACKINGS.with(|watch| watch.set(None));
        }
    }
    let reset = Reset;
    let config = JsonEncoderConfig::new(&JsonWriterConfig::default(), &resources).unwrap();
    let alias = config.clone();
    let captured_resources = resources.clone();
    let make = move |destination, schema| {
        JsonEncoder::from_config(schema, config.clone())?
            .into_boxed_writer(destination, captured_resources.clone())
    };
    let factory_bytes = std::mem::size_of_val(&make);
    NATIVE_BACKINGS.with(|watch| {
        let mut state = watch.get().unwrap();
        state.capture = Some(1);
        watch.set(Some(state));
    });
    let factory = WriterFactory::try_new(make, scope.allocation()).unwrap();
    let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["a.b".into()])));
    let record = Record::new(schema.clone(), vec![Value::Integer(7)]);
    let destination: Box<dyn Write + Send> = Box::new(std::io::sink());
    let counter = SharedByteCounter::new();
    let counting = CountingWriter::new(destination, counter.clone());
    NATIVE_BACKINGS.with(|watch| {
        let mut state = watch.get().unwrap();
        state.capture = Some(2);
        watch.set(Some(state));
    });
    let mut writer = factory.create(counting, schema).unwrap();
    let backing = NATIVE_BACKINGS.with(|watch| watch.get().unwrap().backings);
    assert!(backing[0].bytes > 0);
    assert_eq!(backing[1].bytes, factory_bytes);
    assert_eq!(
        backing[2].bytes,
        std::mem::size_of::<PreparedWriter<CountingWriter<Box<dyn Write + Send>>, JsonEncoder>>()
    );
    assert_eq!(
        observer.usage().memory,
        backing.iter().map(|b| b.bytes as u64).sum::<u64>()
    );
    writer.write_record(&record).unwrap();
    let committed_memory = observer.usage().memory;
    let committed_bytes = counter.bytes_written();
    let changed = Record::new(
        SharedStorage::from_arc(Arc::new(Schema::new(vec!["changed.path".into()]))),
        vec![Value::Integer(9)],
    );
    let pressure = scope
        .reserve(Layout::array::<u8>((arb.limit() - committed_memory) as usize).unwrap())
        .unwrap();
    assert!(
        matches!(writer.write_record(&changed), Err(clinker_format::FormatError::Resource(error)) if error.kind == ResourceErrorKind::Budget)
    );
    assert_eq!(counter.bytes_written(), committed_bytes);
    drop(pressure);
    assert_eq!(observer.usage().memory, committed_memory);
    writer.write_record(&changed).unwrap();
    drop(factory);
    shutdown.request();
    assert!(
        matches!(writer.write_record(&record), Err(clinker_format::FormatError::Resource(error)) if error.kind == ResourceErrorKind::Cancelled)
    );
    drop(writer);
    assert_eq!(observer.usage().memory, backing[0].bytes as u64);
    arb.close_writer_resources();
    assert!(observer.is_closed());
    drop(alias);
    let backing = NATIVE_BACKINGS.with(|watch| watch.get().unwrap().backings);
    for owner in backing {
        assert!(owner.admitted_at_allocation >= owner.bytes as u64);
        assert!(owner.admitted_at_deallocation.unwrap() >= owner.bytes as u64);
    }
    assert_eq!(observer.usage().memory, 0);
    assert_eq!(observer.usage().disk, 0);
    assert_eq!(observer.usage().descriptors, 0);
    assert_eq!(arb.consumer_count(), 0);
    drop(reset);
}

fn csv_runtime_run(
    root: &std::path::Path,
    spill: bool,
    cap: Option<u64>,
    fault: CsvRuntimeFault,
) -> (
    Result<clinker_exec::executor::ExecutionReport, clinker_plan::error::PipelineError>,
    clinker_exec::output::staging::OutputStagingRegistry,
    std::path::PathBuf,
) {
    use clinker_exec::executor::{PipelineExecutor, PipelineRunParams, WriterRegistry};
    let yaml = r#"
pipeline:
  name: prepared_csv
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      path: input.csv
      schema:
        - { name: value, type: string }
  - type: sink
    name: result
    input: rows
    config:
      name: result
      type: csv
      path: output.csv
"#;
    let invalid = matches!(fault, CsvRuntimeFault::InvalidBody);
    let yaml = if invalid {
        yaml.replace(
            "type: csv\n      path: input.csv",
            "type: json\n      path: input.json",
        )
        .replace("type: string", "type: any")
    } else {
        yaml.to_owned()
    };
    let config = clinker_plan::config::parse_config(&yaml).unwrap();
    let plan = config
        .compile(&clinker_plan::config::CompileContext::default())
        .unwrap();
    let destination = root.join("output.csv");
    let staging = clinker_exec::output::staging::OutputStagingRegistry::default();
    let (_, file) = staging
        .stage_output(
            "result",
            clinker_plan::config::IfExistsPolicy::Error,
            false,
            |_| Ok(destination.clone()),
        )
        .unwrap();
    struct PrefixFile {
        file: std::fs::File,
        calls: Arc<std::sync::atomic::AtomicUsize>,
    }
    impl Write for PrefixFile {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                self.file.write(&bytes[..1])
            } else {
                Err(std::io::ErrorKind::BrokenPipe.into())
            }
        }
        fn flush(&mut self) -> std::io::Result<()> {
            panic!("failed delivery must not be retried or flushed")
        }
    }
    let raw: Box<dyn Write + Send> = match fault {
        CsvRuntimeFault::Prefix(calls) => Box::new(PrefixFile { file, calls }),
        _ => Box::new(file),
    };
    let registry = WriterRegistry {
        single: [("result".into(), raw)].into(),
        output_staging: staging.clone(),
        ..Default::default()
    };
    let input = if invalid {
        r#"[{"value":["invalid"]}]"#.to_owned()
    } else {
        format!("value\n{}\n", "x".repeat(100_000))
    };
    let readers = [(
        "rows".into(),
        clinker_exec::executor::single_file_reader(
            "input.csv",
            Box::new(std::io::Cursor::new(input.into_bytes())),
        ),
    )]
    .into();
    let params = PipelineRunParams {
        spill_root_dir: spill.then(|| root.to_owned()),
        spill_disk_cap_bytes: cap,
        ..Default::default()
    };
    (
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, registry, &params),
        staging,
        destination,
    )
}

#[test]
fn csv_runtime_disk_stage_denial_never_publishes_header_or_body() {
    let root = tempfile::tempdir().unwrap();
    let (result, staging, destination) =
        csv_runtime_run(root.path(), true, Some(1), CsvRuntimeFault::None);
    assert!(
        result.is_err(),
        "CSV output must obey the same run spill quota"
    );
    assert!(!destination.exists());
    for partial in staging.partials() {
        assert_eq!(std::fs::metadata(partial.partial_path).unwrap().len(), 0);
    }
}

#[test]
fn csv_runtime_memory_and_spill_files_are_byte_identical() {
    let memory_root = tempfile::tempdir().unwrap();
    let spill_root = tempfile::tempdir().unwrap();
    let (memory, _, memory_path) =
        csv_runtime_run(memory_root.path(), false, None, CsvRuntimeFault::None);
    let (spill, _, spill_path) = csv_runtime_run(
        spill_root.path(),
        true,
        Some(1024 * 1024),
        CsvRuntimeFault::None,
    );
    memory.unwrap();
    spill.unwrap();
    let expected = format!("value\n{}\n", "x".repeat(100_000)).into_bytes();
    assert_eq!(std::fs::read(memory_path).unwrap(), expected);
    assert_eq!(std::fs::read(spill_path).unwrap(), expected);
    assert_eq!(std::fs::read_dir(spill_root.path()).unwrap().count(), 1);
}

#[test]
fn csv_runtime_invalid_body_never_publishes_automatic_header() {
    let root = tempfile::tempdir().unwrap();
    let (result, staging, destination) =
        csv_runtime_run(root.path(), true, None, CsvRuntimeFault::InvalidBody);
    let error = result.unwrap_err().to_string();
    assert!(error.contains("CSV") && error.contains("array"), "{error}");
    assert!(!destination.exists());
    for partial in staging.partials() {
        assert_eq!(std::fs::metadata(partial.partial_path).unwrap().len(), 0);
    }
}

#[test]
fn csv_runtime_partial_destination_failure_does_not_publish_or_retry() {
    let root = tempfile::tempdir().unwrap();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (result, staging, destination) = csv_runtime_run(
        root.path(),
        true,
        None,
        CsvRuntimeFault::Prefix(calls.clone()),
    );
    assert!(result.is_err());
    assert!(!destination.exists());
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    for partial in staging.partials() {
        assert_eq!(std::fs::metadata(partial.partial_path).unwrap().len(), 1);
    }
}

struct CountingAllocator;
thread_local! {
    static ALLOCATIONS: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
    static FAIL_ALLOCATION: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}
// SAFETY: allocation and deallocation delegate to System with unchanged
// layouts. Thread-local scalar counting neither allocates nor crosses threads.
unsafe impl std::alloc::GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        let attempt = ALLOCATIONS
            .try_with(|count| {
                if let Some(n) = count.get() {
                    count.set(Some(n + 1));
                    Some(n + 1)
                } else {
                    None
                }
            })
            .ok()
            .flatten();
        if attempt.is_some()
            && FAIL_ALLOCATION
                .try_with(|fail| fail.get() == attempt)
                .unwrap_or(false)
        {
            return std::ptr::null_mut();
        }
        // SAFETY: the caller supplies a valid allocation layout.
        let pointer = unsafe { std::alloc::System.alloc(layout) };
        let _ = NATIVE_BACKINGS.try_with(|watch| {
            if let Some(mut state) = watch.get()
                && let Some(slot) = state.capture
            {
                // SAFETY: the test guard retains this observer until disabled;
                // reading the warmed resource ledger allocates nothing.
                let memory = unsafe { &*state.observer }.usage().memory;
                if !state.capture_last {
                    state.capture = None;
                }
                state.backings[slot] = NativeBacking {
                    pointer: pointer as usize,
                    bytes: layout.size(),
                    admitted_at_allocation: memory,
                    admitted_at_deallocation: None,
                };
                watch.set(Some(state));
            }
        });
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: std::alloc::Layout) {
        // SAFETY: every allocation above came from System. Observe the ledger
        // after actual deallocation, before the containing owner can release it.
        unsafe { std::alloc::System.dealloc(pointer, layout) }
        let _ = NATIVE_BACKINGS.try_with(|watch| {
            if let Some(mut state) = watch.get() {
                for backing in &mut state.backings {
                    if backing.pointer == pointer as usize
                        && backing.admitted_at_deallocation.is_none()
                    {
                        // SAFETY: the test guard retains the observer through
                        // final deallocation and this read allocates nothing.
                        backing.admitted_at_deallocation =
                            Some(unsafe { &*state.observer }.usage().memory);
                    }
                }
                watch.set(Some(state));
            }
        });
    }
}
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn telemetry() -> (
    clinker_exec::telemetry::TelemetryProducer,
    clinker_exec::telemetry::TelemetryReceiver,
) {
    let config = clinker_plan::config::ClinkerToml::parse(
        r#"
[observability]
arena_bytes = "768KB"
ordinary_lane_bytes = "512KB"
high_severity_lane_bytes = "256KB"
max_batch_bytes = "8KB"
rate_limit_per_second = 100000
rate_limit_burst = 100000
[observability.otlp]
endpoint = "https://collector.invalid"
[observability.otlp.auth]
mode = "none"
"#,
    )
    .unwrap();
    clinker_exec::telemetry::TelemetryArena::reserve(&config.resolve_observability(None).unwrap())
        .unwrap()
}

#[test]
fn first_telemetry_emission_uses_only_startup_allocations() {
    use clinker_exec::telemetry::{AdmissionOutcome, SpanFact, SpanName, SpanStatus};
    let (producer, receiver) = telemetry();
    assert_eq!(producer.snapshot().accepted, 0);
    ALLOCATIONS.with(|count| count.set(Some(0)));
    let result = producer.emit_span(SpanFact {
        name: SpanName::WriterStage,
        status: SpanStatus::Ok,
        logical_node: "writer.stage",
        started_at_unix_nanos: 1,
        ended_at_unix_nanos: 2,
    });
    let allocations = ALLOCATIONS.with(|count| count.replace(None).unwrap());
    assert_eq!(allocations, 0);
    assert!(matches!(result, AdmissionOutcome::Accepted { .. }));
    assert_eq!(receiver.try_recv_batch().unwrap().traces().len(), 1);
}

#[test]
fn first_spill_stage_allocates_only_admitted_progress_and_metadata() {
    let root = tempfile::tempdir().unwrap();
    let arb = Arc::new(MemoryArbitrator::with_policy(
        128 * 1024,
        0.8,
        0.7,
        Box::new(NoOpPolicy),
    ));
    let provider = ExecutorResources::new(
        arb.clone(),
        ShutdownToken::detached(),
        Some(&configured(root.path())),
        NonZeroUsize::new(1).unwrap(),
        None,
    )
    .unwrap();
    let baseline = arb.writer_resource_usage().memory;
    let scope = provider.resources().scope().unwrap();
    ALLOCATIONS.with(|count| count.set(Some(0)));
    let result = scope.stage();
    let allocations = ALLOCATIONS.with(|count| count.replace(None).unwrap());
    assert_eq!(
        allocations, 2,
        "only the admitted progress buffer and stage box"
    );
    drop(result.unwrap());
    assert_eq!(arb.writer_resource_usage().memory, baseline);
    assert_eq!(arb.writer_resource_usage().descriptors, 0);
}

#[test]
fn stage_telemetry_observes_construction_denial_and_allocator_failure() {
    use clinker_exec::telemetry::{MetricKey, SpanName, SpanStatus};
    use clinker_format::preparation::ResourceErrorKind;
    // Deny metadata, deny progress, fail progress allocation, fail stage box.
    for (limit, fail_at) in [
        (1, None),
        (4096, None),
        (128 * 1024, Some(1)),
        (128 * 1024, Some(2)),
    ] {
        let (producer, receiver) = telemetry();
        let arb = Arc::new(MemoryArbitrator::with_policy(
            limit,
            0.8,
            0.7,
            Box::new(NoOpPolicy),
        ));
        let provider = ExecutorResources::new(
            arb.clone(),
            ShutdownToken::detached(),
            None,
            NonZeroUsize::new(1).unwrap(),
            Some(producer),
        )
        .unwrap();
        let scope = provider.resources().scope().unwrap();
        ALLOCATIONS.with(|count| count.set(Some(0)));
        FAIL_ALLOCATION.with(|fail| fail.set(fail_at));
        let result = scope.stage();
        FAIL_ALLOCATION.with(|fail| fail.set(None));
        let allocations = ALLOCATIONS.with(|count| count.replace(None).unwrap());
        if let Some(fail_at) = fail_at {
            assert_eq!(allocations, fail_at);
        }
        assert!(
            matches!(result, Err(clinker_format::FormatError::Resource(error)) if error.kind == if fail_at.is_some() { ResourceErrorKind::Allocation } else { ResourceErrorKind::Budget })
        );
        assert_eq!(arb.writer_resource_usage().memory, 0);
        let batch = receiver.try_recv_batch().unwrap();
        assert_eq!(batch.metric(MetricKey::WriterStageStarted), 1);
        assert_eq!(batch.metric(MetricKey::WriterStageFailed), 1);
        assert_eq!(batch.metric(MetricKey::WriterStageDropped), 0);
        assert_eq!(batch.metric(MetricKey::WriterStageCompleted), 0);
        assert_eq!(
            batch.metric(MetricKey::WriterAdmissionFailed),
            u64::from(fail_at.is_none())
        );
        let spans: Vec<_> = batch
            .traces()
            .iter()
            .filter(|span| span.name == SpanName::WriterStage)
            .collect();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].status, SpanStatus::Error);
    }
}

#[test]
fn stage_telemetry_distinguishes_completion_cancellation_and_abandonment() {
    use clinker_exec::telemetry::{MetricKey, SpanName, SpanStatus};
    for outcome in [
        MetricKey::WriterStageCompleted,
        MetricKey::WriterStageInterrupted,
        MetricKey::WriterStageDropped,
        MetricKey::WriterStageFailed,
    ] {
        let (producer, receiver) = telemetry();
        let arb = Arc::new(MemoryArbitrator::with_policy(
            128 * 1024,
            0.8,
            0.7,
            Box::new(NoOpPolicy),
        ));
        let token = ShutdownToken::detached();
        let provider = ExecutorResources::new(
            arb.clone(),
            token.clone(),
            None,
            NonZeroUsize::new(1).unwrap(),
            Some(producer),
        )
        .unwrap();
        let scope = provider.resources().scope().unwrap();
        let mut stage = scope.stage().unwrap();
        let mut output = Vec::new();
        match outcome {
            MetricKey::WriterStageCompleted => {
                stage.write_all(b"complete").unwrap();
                stage.finish().unwrap().deliver(&mut output).unwrap();
                assert_eq!(output, b"complete");
            }
            MetricKey::WriterStageInterrupted => {
                token.request();
                assert!(stage.write_all(b"cancelled before storage").is_err());
                assert!(stage.finish().is_err());
            }
            MetricKey::WriterStageFailed => {
                assert!(stage.write_all(&[1; 256 * 1024]).is_err());
                assert!(stage.finish().is_err());
            }
            _ => {
                // A shutdown request alone is not an observed cancellation
                // result; abandoning without another operation remains dropped.
                token.request();
                drop(stage);
            }
        }
        assert_eq!(arb.writer_resource_usage().memory, 0);
        let batch = receiver.try_recv_batch().unwrap();
        assert_eq!(batch.metric(MetricKey::WriterStageStarted), 1);
        assert_eq!(batch.metric(outcome), 1);
        assert_eq!(
            [
                MetricKey::WriterStageCompleted,
                MetricKey::WriterStageFailed,
                MetricKey::WriterStageInterrupted,
                MetricKey::WriterStageDropped
            ]
            .into_iter()
            .map(|key| batch.metric(key))
            .sum::<u64>(),
            1
        );
        let spans: Vec<_> = batch
            .traces()
            .iter()
            .filter(|span| span.name == SpanName::WriterStage)
            .collect();
        assert_eq!(spans.len(), 1);
        assert_eq!(
            spans[0].status,
            match outcome {
                MetricKey::WriterStageCompleted => SpanStatus::Ok,
                MetricKey::WriterStageFailed => SpanStatus::Error,
                _ => SpanStatus::Unset,
            }
        );
        assert!(spans[0].started_at_unix_nanos <= spans[0].ended_at_unix_nanos);
        assert_eq!(spans[0].logical_node, "writer.stage");
    }
}

#[test]
fn stage_spill_and_cleanup_telemetry_cannot_block_a_full_arena() {
    use clinker_exec::telemetry::{
        AdmissionOutcome, DropReason, MetricKey, SpanFact, SpanName, SpanStatus,
    };
    for full in [false, true] {
        let (producer, receiver) = telemetry();
        if full {
            for status in [SpanStatus::Ok, SpanStatus::Error] {
                loop {
                    let result = producer.emit_span(SpanFact {
                        name: SpanName::Transform,
                        status,
                        logical_node: "fill",
                        started_at_unix_nanos: 1,
                        ended_at_unix_nanos: 2,
                    });
                    if result == AdmissionOutcome::Dropped(DropReason::Full) {
                        break;
                    }
                    assert!(matches!(result, AdmissionOutcome::Accepted { .. }));
                }
            }
        }
        let baseline = producer.snapshot();
        let token = ShutdownToken::detached();
        let root = tempfile::tempdir().unwrap();
        let arb = Arc::new(MemoryArbitrator::with_policy(
            128 * 1024,
            0.8,
            0.7,
            Box::new(NoOpPolicy),
        ));
        let provider = ExecutorResources::new(
            arb.clone(),
            token.clone(),
            Some(&configured(root.path())),
            NonZeroUsize::new(1).unwrap(),
            Some(producer.clone()),
        )
        .unwrap();
        let mut stage = provider.resources().scope().unwrap().stage().unwrap();
        stage.write_all(&[7; 100 * 1024]).unwrap();
        let mut output = Vec::new();
        stage.finish().unwrap().deliver(&mut output).unwrap();
        assert_eq!(output, [7; 100 * 1024]);
        arb.set_max_spill_bytes(0).unwrap();
        let scope = provider.resources().scope().unwrap();
        let mut refused = scope.stage().unwrap();
        assert!(refused.write_all(&[9; 100 * 1024]).is_err());
        assert!(refused.finish().is_err());
        let mut cancelled = scope.stage().unwrap();
        token.request();
        assert!(cancelled.write_all(b"cancelled").is_err());
        assert!(cancelled.finish().is_err());
        assert_eq!(arb.writer_resource_usage().disk, 0);
        assert_eq!(arb.writer_resource_usage().descriptors, 0);
        assert_eq!(producer.snapshot().owned_bytes, baseline.owned_bytes);
        if full {
            assert_eq!(producer.snapshot().accepted, baseline.accepted);
        }
        let batch = receiver.try_recv_batch().unwrap();
        assert_eq!(batch.metric(MetricKey::WriterStageCompleted), 1);
        assert_eq!(batch.metric(MetricKey::WriterStageFailed), 1);
        assert_eq!(batch.metric(MetricKey::WriterStageInterrupted), 1);
        assert_eq!(batch.metric(MetricKey::WriterSpillStarted), 2);
        assert_eq!(batch.metric(MetricKey::WriterSpillCompleted), 1);
        assert_eq!(batch.metric(MetricKey::WriterSpillFailed), 1);
        assert_eq!(batch.metric(MetricKey::WriterSpillBytes), 100 * 1024);
        assert_eq!(batch.metric(MetricKey::WriterCleanupCompleted), 3);
    }
}

#[test]
fn writer_primitive_telemetry_vocabulary_is_closed_and_serializable() {
    use clinker_exec::telemetry::{MetricKey, SpanName};
    for name in [
        "writer_admission",
        "writer_stage",
        "writer_spill",
        "writer_cleanup",
    ] {
        let span: SpanName = serde_json::from_str(&format!("\"{name}\"")).unwrap();
        assert_eq!(serde_json::to_string(&span).unwrap(), format!("\"{name}\""));
        for outcome in ["started", "completed", "failed", "interrupted"] {
            let key = format!("\"{name}_{outcome}\"");
            let metric: MetricKey = serde_json::from_str(&key).unwrap();
            assert_eq!(serde_json::to_string(&metric).unwrap(), key);
        }
    }
    for name in ["writer_stage_dropped", "writer_spill_bytes"] {
        let metric: MetricKey = serde_json::from_str(&format!("\"{name}\"")).unwrap();
        assert!(MetricKey::ALL.contains(&metric));
    }
}

#[test]
fn stage_disk_refusal_preserves_quota_evidence_without_error_allocation() {
    use clinker_format::preparation::{ResourceError, ResourceErrorKind};
    let (producer, receiver) = telemetry();
    let root = tempfile::tempdir().unwrap();
    let arb = Arc::new(MemoryArbitrator::with_policy(
        128 * 1024,
        0.8,
        0.7,
        Box::new(NoOpPolicy),
    ));
    arb.set_max_spill_bytes(72 * 1024).unwrap();
    let provider = ExecutorResources::new(
        arb.clone(),
        ShutdownToken::detached(),
        Some(&configured(root.path())),
        NonZeroUsize::new(1).unwrap(),
        Some(producer),
    )
    .unwrap();
    let mut stage = provider.resources().scope().unwrap().stage().unwrap();
    stage.write_all(&[7; 72 * 1024]).unwrap();
    assert_eq!(arb.writer_resource_usage().disk, 72 * 1024);
    ALLOCATIONS.with(|count| count.set(Some(0)));
    let result = stage.write(&[9; 1024]);
    let allocations = ALLOCATIONS.with(|count| count.replace(None).unwrap());
    assert!(result.is_err());
    assert_eq!(allocations, 0);
    let expected = ResourceError::new(ResourceErrorKind::DiskQuota, 1024, 0);
    assert_eq!(stage.failure(), Some(expected));
    assert!(
        matches!(stage.finish(), Err(clinker_format::FormatError::Resource(error)) if error == expected)
    );
    assert_eq!(arb.writer_resource_usage().disk, 0);
    assert_eq!(arb.writer_resource_usage().descriptors, 0);
    assert_eq!(
        receiver
            .try_recv_batch()
            .unwrap()
            .metric(clinker_exec::telemetry::MetricKey::WriterStageFailed),
        1
    );
}

fn configured(root: &std::path::Path) -> clinker_exec::executor::ResolvedStorage {
    clinker_exec::executor::ResolvedStorage {
        spill_root_dir: Some(root.to_owned()),
        free_space_warning: None,
        cap_headroom_warning: None,
    }
}

fn saturate_writer_telemetry(producer: &clinker_exec::telemetry::TelemetryProducer) {
    use clinker_exec::telemetry::{AdmissionOutcome, DropReason, SpanFact, SpanName, SpanStatus};
    for status in [SpanStatus::Ok, SpanStatus::Error] {
        loop {
            let outcome = producer.emit_span(SpanFact {
                name: SpanName::Transform,
                status,
                logical_node: "fill",
                started_at_unix_nanos: 1,
                ended_at_unix_nanos: 2,
            });
            if outcome == AdmissionOutcome::Dropped(DropReason::Full) {
                break;
            }
            assert!(matches!(outcome, AdmissionOutcome::Accepted { .. }));
        }
    }
}

fn cancelled_delivery_releases_resources(empty: bool) {
    use clinker_exec::telemetry::{MetricKey, SpanName, SpanStatus};
    use clinker_format::{
        FormatError,
        preparation::{ResourceError, ResourceErrorKind},
    };

    struct CancelOnLastWrite {
        token: ShutdownToken,
        remaining: usize,
        output: Vec<u8>,
    }
    impl Write for CancelOnLastWrite {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.output.extend_from_slice(bytes);
            self.remaining -= bytes.len();
            if self.remaining == 0 {
                self.token.request();
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            panic!("delivery does not flush")
        }
    }
    for (spill, mode) in [false, true]
        .into_iter()
        .flat_map(|spill| (0..3).map(move |mode| (spill, mode)))
    {
        let (producer, receiver) = telemetry();
        if mode == 2 {
            saturate_writer_telemetry(&producer);
        }
        let arena_before = producer.snapshot();
        let root = tempfile::tempdir().unwrap();
        let arb = Arc::new(MemoryArbitrator::with_policy(
            128 * 1024,
            0.8,
            0.7,
            Box::new(NoOpPolicy),
        ));
        let token = ShutdownToken::detached();
        let storage = configured(root.path());
        let provider = ExecutorResources::new(
            arb.clone(),
            token.clone(),
            spill.then_some(&storage),
            NonZeroUsize::new(1).unwrap(),
            (mode != 0).then(|| producer.clone()),
        )
        .unwrap();
        let baseline = arb.writer_resource_usage().memory;
        let payload = vec![
            7;
            if empty {
                0
            } else if spill {
                100 * 1024
            } else {
                1024
            }
        ];
        let mut stage = provider.resources().scope().unwrap().stage().unwrap();
        stage.write_all(&payload).unwrap();
        let prepared = stage.finish().unwrap();
        assert_eq!(prepared.len(), payload.len() as u64);
        if spill && !empty {
            assert_eq!(arb.writer_resource_usage().disk, payload.len() as u64);
            assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
        }
        if empty {
            token.request();
        }
        let mut destination = CancelOnLastWrite {
            token,
            remaining: payload.len(),
            output: Vec::new(),
        };
        assert!(
            matches!(prepared.deliver(&mut destination), Err(FormatError::Resource(error)) if error == ResourceError::new(ResourceErrorKind::Cancelled, 0, 0))
        );
        assert_eq!(destination.output, payload);
        assert_eq!(arb.writer_resource_usage().memory, baseline);
        assert_eq!(arb.writer_resource_usage().disk, 0);
        assert_eq!(arb.writer_resource_usage().descriptors, 0);
        assert_eq!(provider.cleanup_debt_count(), 0);
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
        let arena_after = producer.snapshot();
        assert_eq!(arena_after.owned_bytes, arena_before.owned_bytes);
        assert_eq!(
            arena_after.ordinary_capacity_bytes,
            arena_before.ordinary_capacity_bytes
        );
        assert_eq!(
            arena_after.high_capacity_bytes,
            arena_before.high_capacity_bytes
        );
        if mode == 0 {
            assert!(receiver.try_recv_batch().is_none());
            drop(provider);
            assert_eq!(arb.writer_resource_usage().memory, 0);
            assert_eq!(arb.consumer_count(), 0);
            continue;
        }
        let batch = receiver.try_recv_batch().unwrap();
        assert_eq!(batch.metric(MetricKey::WriterStageStarted), 1);
        assert_eq!(batch.metric(MetricKey::WriterStageInterrupted), 1);
        for key in [
            MetricKey::WriterStageCompleted,
            MetricKey::WriterStageFailed,
            MetricKey::WriterStageDropped,
        ] {
            assert_eq!(batch.metric(key), 0);
        }
        let spans: Vec<_> = batch
            .traces()
            .iter()
            .filter(|span| span.name == SpanName::WriterStage)
            .collect();
        if mode == 1 {
            assert_eq!(spans.len(), 1);
            assert_eq!(spans[0].status, SpanStatus::Unset);
            assert!(spans[0].started_at_unix_nanos > 0);
            assert!(spans[0].started_at_unix_nanos <= spans[0].ended_at_unix_nanos);
        } else {
            assert!(spans.is_empty());
            assert!(arena_after.full_drops > arena_before.full_drops);
        }
        drop(provider);
        assert_eq!(arb.writer_resource_usage().memory, 0);
        assert_eq!(arb.consumer_count(), 0);
    }
}

#[test]
fn empty_delivery_cancellation_is_interrupted_and_releases_resources() {
    cancelled_delivery_releases_resources(true);
}

#[test]
fn final_write_cancellation_is_interrupted_and_releases_resources() {
    cancelled_delivery_releases_resources(false);
}

#[test]
fn empty_finalize_cancellation_never_commits_or_touches_destination() {
    use clinker_format::FormatError;
    use clinker_format::preparation::{
        FormatEncoder, OutputOperation, PreparedWriter, WriterScope,
    };

    struct CancelFinalize {
        token: ShutdownToken,
        commits: usize,
    }
    impl FormatEncoder for CancelFinalize {
        type Pending = ();
        fn prepare(
            &self,
            operation: OutputOperation<'_>,
            _: &mut dyn Write,
            _: &WriterScope,
        ) -> Result<(), FormatError> {
            assert!(matches!(operation, OutputOperation::Finalize));
            self.token.request();
            Ok(())
        }
        fn commit(&mut self, (): ()) {
            self.commits += 1;
        }
    }
    struct Untouched;
    impl Write for Untouched {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            panic!("cancelled empty finalize wrote")
        }
        fn flush(&mut self) -> std::io::Result<()> {
            panic!("cancelled empty finalize flushed")
        }
    }
    for spill in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let arb = Arc::new(MemoryArbitrator::with_policy(
            128 * 1024,
            0.8,
            0.7,
            Box::new(NoOpPolicy),
        ));
        let token = ShutdownToken::detached();
        let storage = configured(root.path());
        let (producer, receiver) = telemetry();
        let provider = ExecutorResources::new(
            arb.clone(),
            token.clone(),
            spill.then_some(&storage),
            NonZeroUsize::new(1).unwrap(),
            Some(producer),
        )
        .unwrap();
        let baseline = arb.writer_resource_usage().memory;
        let mut writer = PreparedWriter::new(
            Untouched,
            CancelFinalize { token, commits: 0 },
            provider.resources(),
        )
        .unwrap();
        assert!(
            matches!(writer.flush(), Err(FormatError::Resource(error)) if error.kind == ResourceErrorKind::Cancelled)
        );
        assert_eq!(writer.encoder().commits, 0);
        drop(writer);
        assert_eq!(arb.writer_resource_usage().memory, baseline);
        assert_eq!(arb.writer_resource_usage().disk, 0);
        assert_eq!(arb.writer_resource_usage().descriptors, 0);
        assert_eq!(provider.cleanup_debt_count(), 0);
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
        let batch = receiver.try_recv_batch().unwrap();
        use clinker_exec::telemetry::MetricKey;
        assert_eq!(batch.metric(MetricKey::WriterStageStarted), 1);
        assert_eq!(batch.metric(MetricKey::WriterStageInterrupted), 1);
        assert_eq!(batch.metric(MetricKey::WriterStageCompleted), 0);
        assert_eq!(batch.metric(MetricKey::WriterStageFailed), 0);
        drop(provider);
        assert_eq!(arb.writer_resource_usage().memory, 0);
        assert_eq!(arb.consumer_count(), 0);
    }
}

#[test]
fn stage_spills_with_all_other_memory_reserved_and_releases_file() {
    let root = tempfile::tempdir().unwrap();
    let arb = Arc::new(MemoryArbitrator::with_policy(
        96 * 1024,
        0.8,
        0.7,
        Box::new(NoOpPolicy),
    ));
    arb.set_max_spill_bytes(1024 * 1024).unwrap();
    let provider = ExecutorResources::new(
        arb.clone(),
        ShutdownToken::detached(),
        Some(&configured(root.path())),
        NonZeroUsize::new(2).unwrap(),
        None,
    )
    .unwrap();
    let baseline = arb.writer_resource_usage().memory;
    let scope = provider.resources().scope().unwrap();
    let mut stage = scope.stage().unwrap();
    let remaining = arb.limit() - arb.writer_resource_usage().memory;
    let pressure = scope
        .reserve(Layout::array::<u8>(remaining as usize).unwrap())
        .unwrap();
    let bytes = vec![42; 150 * 1024];
    stage.write_all(&bytes).unwrap();
    assert_eq!(arb.writer_resource_usage().memory, arb.limit());
    assert_eq!(arb.writer_resource_usage().disk, bytes.len() as u64);
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
    let mut output = Vec::new();
    stage.finish().unwrap().deliver(&mut output).unwrap();
    assert_eq!(output, bytes);
    assert_eq!(arb.writer_resource_usage().disk, 0);
    assert_eq!(arb.writer_resource_usage().descriptors, 0);
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    drop(pressure);
    assert_eq!(arb.writer_resource_usage().memory, baseline);
    drop(scope);
    drop(provider);
    assert_eq!(arb.consumer_count(), 0);
    assert_eq!(arb.writer_resource_usage().memory, 0);
}

#[test]
fn stage_quota_and_cancellation_never_seal_or_touch_destination() {
    let root = tempfile::tempdir().unwrap();
    let arb = Arc::new(MemoryArbitrator::with_policy(
        128 * 1024,
        0.8,
        0.7,
        Box::new(NoOpPolicy),
    ));
    arb.set_max_spill_bytes(20 * 1024).unwrap();
    let token = ShutdownToken::detached();
    let provider = ExecutorResources::new(
        arb.clone(),
        token.clone(),
        Some(&configured(root.path())),
        NonZeroUsize::new(1).unwrap(),
        None,
    )
    .unwrap();
    let baseline = arb.writer_resource_usage().memory;
    let mut stage = provider.resources().scope().unwrap().stage().unwrap();
    assert!(stage.write_all(&vec![7; 100 * 1024]).is_err());
    assert!(stage.finish().is_err());
    assert_eq!(arb.writer_resource_usage().disk, 0);
    assert_eq!(arb.writer_resource_usage().memory, baseline);
    let mut stage = provider.resources().scope().unwrap().stage().unwrap();
    stage.write_all(b"prefix").unwrap();
    token.request();
    assert!(stage.write_all(b"suffix").is_err());
    assert!(stage.finish().is_err());
    assert_eq!(arb.writer_resource_usage().descriptors, 0);
    assert_eq!(arb.writer_resource_usage().memory, baseline);
}

#[test]
fn stage_descriptor_denial_and_limit_changes_are_atomic() {
    let root = tempfile::tempdir().unwrap();
    let arb = Arc::new(MemoryArbitrator::with_policy(
        128 * 1024,
        0.8,
        0.7,
        Box::new(NoOpPolicy),
    ));
    let provider = ExecutorResources::new(
        arb.clone(),
        ShutdownToken::detached(),
        Some(&configured(root.path())),
        NonZeroUsize::new(1).unwrap(),
        None,
    )
    .unwrap();
    let scope = provider.resources().scope().unwrap();
    let mut stage = scope.stage().unwrap();
    assert!(scope.stage().is_err());
    assert!(arb.set_limit(1).is_err());
    assert_eq!(arb.limit(), 128 * 1024);
    stage.write_all(&vec![1; 100 * 1024]).unwrap();
    assert!(arb.set_max_spill_bytes(1).is_err());
    assert_eq!(arb.max_spill_bytes(), u64::MAX);
    drop(stage);
    assert_eq!(arb.writer_resource_usage().disk, 0);
    assert!(scope.stage().is_ok());
}

#[test]
fn stage_failed_unlink_retains_bounded_debt_until_successful_cleanup() {
    let (producer, receiver) = telemetry();
    let root = tempfile::tempdir().unwrap();
    let arb = Arc::new(MemoryArbitrator::with_policy(
        128 * 1024,
        0.8,
        0.7,
        Box::new(NoOpPolicy),
    ));
    let provider = ExecutorResources::new(
        arb.clone(),
        ShutdownToken::detached(),
        Some(&configured(root.path())),
        NonZeroUsize::new(1).unwrap(),
        Some(producer),
    )
    .unwrap();
    let scope = provider.resources().scope().unwrap();
    let mut stage = scope.stage().unwrap();
    stage.write_all(&vec![1; 100 * 1024]).unwrap();
    let prepared = stage.finish().unwrap();
    let path = std::fs::read_dir(root.path())
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    // Closing the handle precedes unlink; inject the unlink fault via a directory
    // at the owned name while retaining the actual byte file for restoration.
    let saved = root.path().join("retained");
    std::fs::rename(&path, &saved).unwrap();
    std::fs::create_dir(&path).unwrap();
    drop(prepared);
    assert_eq!(provider.cleanup_debt_count(), 1);
    assert_eq!(arb.writer_resource_usage().disk, 100 * 1024);
    assert!(
        scope.stage().is_err(),
        "debt occupies the only cleanup slot"
    );
    provider.cleanup();
    assert_eq!(provider.cleanup_debt_count(), 1);
    std::fs::remove_dir(&path).unwrap();
    std::fs::rename(saved, path).unwrap();
    provider.cleanup();
    assert_eq!(provider.cleanup_debt_count(), 0);
    assert_eq!(arb.writer_resource_usage().disk, 0);
    let batch = receiver.try_recv_batch().unwrap();
    assert_eq!(
        batch.metric(clinker_exec::telemetry::MetricKey::WriterCleanupFailed),
        2
    );
    assert_eq!(
        batch.metric(clinker_exec::telemetry::MetricKey::WriterCleanupCompleted),
        1
    );
    assert!(scope.stage().is_ok());
}

#[test]
fn stage_short_readback_refuses_incomplete_prepared_bytes() {
    let (producer, receiver) = telemetry();
    let root = tempfile::tempdir().unwrap();
    let arb = Arc::new(MemoryArbitrator::with_policy(
        128 * 1024,
        0.8,
        0.7,
        Box::new(NoOpPolicy),
    ));
    let provider = ExecutorResources::new(
        arb.clone(),
        ShutdownToken::detached(),
        Some(&configured(root.path())),
        NonZeroUsize::new(1).unwrap(),
        Some(producer),
    )
    .unwrap();
    let mut stage = provider.resources().scope().unwrap().stage().unwrap();
    stage.write_all(&vec![1; 100 * 1024]).unwrap();
    let prepared = stage.finish().unwrap();
    let path = std::fs::read_dir(root.path())
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .unwrap()
        .set_len(0)
        .unwrap();
    let mut output = Vec::new();
    assert!(prepared.deliver(&mut output).is_err());
    assert!(output.is_empty());
    assert_eq!(arb.writer_resource_usage().disk, 0);
    let batch = receiver.try_recv_batch().unwrap();
    assert_eq!(
        batch.metric(clinker_exec::telemetry::MetricKey::WriterStageFailed),
        1
    );
    assert_eq!(
        batch.metric(clinker_exec::telemetry::MetricKey::WriterStageDropped),
        0
    );
}

#[test]
fn stage_relative_and_long_configured_roots_preserve_exact_storage_directory() {
    let cwd = std::env::current_dir().unwrap();
    let relative = tempfile::Builder::new()
        .prefix("writer-relative-")
        .tempdir_in(&cwd)
        .unwrap();
    let relative_path = std::path::Path::new(relative.path().file_name().unwrap());
    let long = tempfile::tempdir().unwrap();
    let mut long_path = long.path().to_owned();
    for _ in 0..20 {
        long_path.push("several-components-for-path-budget");
    }
    std::fs::create_dir_all(&long_path).unwrap();
    for (configured_path, actual_path) in [
        (relative_path, relative.path()),
        (long_path.as_path(), long_path.as_path()),
    ] {
        let arb = Arc::new(MemoryArbitrator::with_policy(
            1024 * 1024,
            0.8,
            0.7,
            Box::new(NoOpPolicy),
        ));
        let provider = ExecutorResources::new(
            arb.clone(),
            ShutdownToken::detached(),
            Some(&configured(configured_path)),
            NonZeroUsize::new(1).unwrap(),
            None,
        )
        .unwrap();
        let mut stage = provider.resources().scope().unwrap().stage().unwrap();
        stage.write_all(&vec![9; 100 * 1024]).unwrap();
        assert_eq!(std::fs::read_dir(actual_path).unwrap().count(), 1);
        let mut output = Vec::new();
        stage.finish().unwrap().deliver(&mut output).unwrap();
        assert_eq!(output, vec![9; 100 * 1024]);
        assert_eq!(std::fs::read_dir(actual_path).unwrap().count(), 0);
        drop(provider);
        assert_eq!(arb.writer_resource_usage().memory, 0);
        assert_eq!(arb.consumer_count(), 0);
    }
}

#[test]
fn stage_cleanup_debt_outlives_provider_and_remains_retryable() {
    let root = tempfile::tempdir().unwrap();
    let arb = Arc::new(MemoryArbitrator::with_policy(
        128 * 1024,
        0.8,
        0.7,
        Box::new(NoOpPolicy),
    ));
    let provider = ExecutorResources::new(
        arb.clone(),
        ShutdownToken::detached(),
        Some(&configured(root.path())),
        NonZeroUsize::new(1).unwrap(),
        None,
    )
    .unwrap();
    assert_eq!(
        arb.retry_writer_cleanup(),
        0,
        "an early cleanup must not detach a live run's owner"
    );
    let mut stage = provider.resources().scope().unwrap().stage().unwrap();
    stage.write_all(&vec![1; 100 * 1024]).unwrap();
    let prepared = stage.finish().unwrap();
    let path = std::fs::read_dir(root.path())
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let saved = root.path().join("retained");
    std::fs::rename(&path, &saved).unwrap();
    std::fs::create_dir(&path).unwrap();
    drop(prepared);
    drop(provider);
    assert_eq!(arb.retry_writer_cleanup(), 1);
    assert_eq!(arb.writer_resource_usage().disk, 100 * 1024);
    assert_eq!(arb.consumer_count(), 1, "debt still owns admitted metadata");
    std::fs::remove_dir(&path).unwrap();
    std::fs::rename(saved, path).unwrap();
    assert_eq!(arb.retry_writer_cleanup(), 0);
    assert_eq!(arb.writer_resource_usage().disk, 0);
    assert_eq!(arb.writer_resource_usage().memory, 0);
    assert_eq!(arb.consumer_count(), 0);
}
use std::{alloc::Layout, io::Write, num::NonZeroUsize, sync::Arc};

#[test]
fn stage_competing_grants_never_oversubscribe() {
    let arb = Arc::new(MemoryArbitrator::with_policy(
        1024,
        0.8,
        0.7,
        Box::new(NoOpPolicy),
    ));
    let provider = ExecutorResources::new(
        arb.clone(),
        ShutdownToken::detached(),
        None,
        NonZeroUsize::new(4).unwrap(),
        None,
    )
    .unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(8));
    std::thread::scope(|threads| {
        for _ in 0..8 {
            let resources = provider.resources();
            let barrier = barrier.clone();
            threads.spawn(move || {
                let scope = resources.scope().unwrap();
                barrier.wait();
                let grant = scope.reserve(Layout::from_size_align(600, 1).unwrap());
                barrier.wait();
                drop(grant);
            });
        }
    });
    assert!(arb.writer_resource_usage().peak_memory <= 1024);
    assert_eq!(arb.writer_resource_usage().memory, 0);
    assert_eq!(arb.consumer_count(), 1);
    drop(provider);
    assert_eq!(arb.consumer_count(), 0);
}

#[test]
fn stage_memory_sealed_bytes_match_standalone() {
    let arb = Arc::new(MemoryArbitrator::with_policy(
        128 * 1024,
        0.8,
        0.7,
        Box::new(NoOpPolicy),
    ));
    let provider = ExecutorResources::new(
        arb.clone(),
        ShutdownToken::detached(),
        None,
        NonZeroUsize::new(4).unwrap(),
        None,
    )
    .unwrap();
    let mut stage = provider.resources().scope().unwrap().stage().unwrap();
    stage.write_all(b"exact bytes\n").unwrap();
    let mut destination = Vec::new();
    stage.finish().unwrap().deliver(&mut destination).unwrap();
    assert_eq!(destination, b"exact bytes\n");
    assert_eq!(arb.writer_resource_usage().memory, 0);
}

#[test]
fn decode_allocator_refusal_is_typed_at_admitted_reader_growth() {
    use clinker_format::{
        FormatReader,
        csv::{CsvReader, CsvReaderConfig},
        preparation::{DecodeWorkspace, TextStorage},
    };
    use clinker_record::owned_storage::AllocationResources;
    use std::sync::atomic::Ordering::SeqCst;
    let input = b"parts,nested\nlong_long_long_long_long\\;part;tail,\"[{\"\"long_long_long_long_key\"\":[null,\"\"long_long_long_long_long_value\"\"]}]\"\n";
    let mut failures = 0;
    for fail_allocator_at in 1..200 {
        let authority = Arc::new(DecodeFaultAuthority {
            fail_allocator_at,
            ..Default::default()
        });
        let config = CsvReaderConfig {
            charset: clinker_format::charset::Charset::Latin1,
            split_values: vec![
                clinker_format::multi_value::SplitValues {
                    field: "parts".into(),
                    delimiter: ";".into(),
                    escape: "\\".into(),
                    json: false,
                },
                clinker_format::multi_value::SplitValues {
                    field: "nested".into(),
                    delimiter: ";".into(),
                    escape: String::new(),
                    json: true,
                },
            ],
            ..Default::default()
        };
        let result = (|| {
            let mut reader = CsvReader::from_reader_admitted(
                input.as_slice(),
                config,
                DecodeWorkspace::new(AllocationResources::new(authority.clone()))?,
                TextStorage::Shared,
            )?;
            reader.next_record()
        })();
        FAIL_ALLOCATION.with(|fail| fail.set(None));
        let attempts = ALLOCATIONS.with(|count| count.replace(None));
        let reached = authority.calls.load(SeqCst) >= fail_allocator_at;
        if reached {
            assert!(attempts.is_some_and(|count| count > 0));
            assert!(
                matches!(&result, Err(clinker_format::FormatError::Resource(error)) if error.kind == ResourceErrorKind::Allocation),
                "allocation {fail_allocator_at}: {result:?}"
            );
            failures += 1;
        } else {
            assert!(result.as_ref().unwrap().is_some());
        }
        drop(result);
        assert_eq!(
            authority.used.load(SeqCst),
            0,
            "allocation {fail_allocator_at}"
        );
        if !reached {
            assert!(
                failures > 20,
                "header, schema, Latin-1 scratch, split and nested owners"
            );
            return;
        }
    }
    panic!("allocator enumeration did not terminate");
}

#[test]
fn decode_concurrent_source_handoff_retains_charge_until_last_alias() {
    use clinker_exec::source::{
        RecordSource,
        multi_file::{FileSlot, MultiFileFormatReader},
    };
    use clinker_format::{
        FormatReader,
        csv::{CsvReader, CsvReaderConfig},
        preparation::{DecodeWorkspace, TextStorage},
    };
    use clinker_record::{Value, owned_storage::AllocationResources};
    use std::sync::atomic::Ordering::SeqCst;
    let authority = Arc::new(DecodeFaultAuthority::default());
    let resources = AllocationResources::new(authority.clone());
    let first = "first-source-owned-value-".repeat(20);
    let second = "second-source-owned-value-".repeat(20);
    let mut reader: Box<dyn FormatReader> = Box::new(MultiFileFormatReader::new(
        vec![
            FileSlot::new(
                "first.csv",
                Box::new(std::io::Cursor::new(format!("value\n{first}\n"))),
            ),
            FileSlot::new(
                "second.csv",
                Box::new(std::io::Cursor::new(format!("value\n{second}\n"))),
            ),
        ],
        Box::new(move |source| {
            Ok(Box::new(CsvReader::from_reader_admitted(
                source.open()?,
                CsvReaderConfig::default(),
                DecodeWorkspace::new(resources.clone())?,
                TextStorage::Shared,
            )?))
        }),
    ));
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    let (released, proceed) = std::sync::mpsc::sync_channel(0);
    std::thread::scope(|threads| {
        let producer = threads.spawn(move || {
            let row = RecordSource::next_record(&mut reader).unwrap().unwrap();
            sender.send(row).unwrap();
            // Wait until the consumer has detached a leaf before replacing the
            // physical reader and destroying all producer-side ownership.
            proceed.recv().unwrap();
            let row = RecordSource::next_record(&mut reader).unwrap().unwrap();
            assert_eq!(row.get("value"), Some(&Value::from(second.as_str())));
            assert!(RecordSource::next_record(&mut reader).unwrap().is_none());
        });
        let row = receiver.recv().unwrap();
        let leaf = row.get("value").unwrap().clone();
        let another = leaf.clone();
        let before = authority.used.load(SeqCst);
        drop(row);
        assert!(
            authority.used.load(SeqCst) < before,
            "record slots release while the leaf remains live"
        );
        released.send(()).unwrap();
        producer.join().unwrap();
        let Value::String(text) = &leaf else {
            panic!("text")
        };
        assert_eq!(text.as_str(), first);
        assert_eq!(text.legacy_heap_size(), 0);
        let only_leaf = authority.used.load(SeqCst);
        assert!(only_leaf >= first.len());
        drop(leaf);
        assert_eq!(
            authority.used.load(SeqCst),
            only_leaf,
            "one shared alias still owns exactly the same charge"
        );
        drop(another);
        assert_eq!(
            authority.used.load(SeqCst),
            0,
            "the final alias releases the allocation"
        );
    });
}

#[test]
fn decode_multi_record_pending_growth_refuses_admission_and_allocator() {
    use clinker_format::envelope::{
        EnvelopeConfig, EnvelopeExtract, EnvelopeFieldType, EnvelopeSection,
    };
    use clinker_format::{
        Column, FormatReader,
        charset::Charset,
        multi_record::{CsvDialect, MultiRecordReader, MultiRecordSpec},
        preparation::{DecodeWorkspace, TextStorage},
        schema::{Discriminator, RecordType},
    };
    use clinker_record::owned_storage::AllocationResources;
    use std::sync::atomic::Ordering::SeqCst;
    let text = "long-admitted-cell-".repeat(20);
    let tag = "long-admitted-discriminator-".repeat(3);
    let input = format!("kind,label\nH,{text}\n{tag},{text}\n");
    for allocator in [false, true] {
        let mut failures = 0;
        for fail_at in 1..300 {
            let authority = Arc::new(DecodeFaultAuthority {
                fail_at: if allocator { 0 } else { fail_at },
                fail_allocator_at: if allocator { fail_at } else { 0 },
                ..Default::default()
            });
            let record_type = |id: &str, tag: &str| RecordType {
                id: id.into(),
                tag: tag.into(),
                description: None,
                parent: None,
                join_key: None,
                columns: vec![
                    Column::bare("kind", cxl::typecheck::Type::String),
                    Column::bare("label", cxl::typecheck::Type::String),
                ],
            };
            let spec = MultiRecordSpec {
                discriminator: Discriminator {
                    start: None,
                    width: None,
                    field: Some("kind".into()),
                },
                record_types: vec![record_type("metadata", "H"), record_type("detail", &tag)],
                structure: vec![],
                // A duplicate extraction request keeps pre-scan open until
                // the body row becomes the retained pending lookahead.
                header_tags: vec!["H".into(), "H".into()],
            };
            let envelope = EnvelopeConfig {
                sections: indexmap::IndexMap::from([(
                    "manifest".into(),
                    EnvelopeSection {
                        extract: EnvelopeExtract::RecordType("H".into()),
                        fields: indexmap::IndexMap::from([(
                            "label".into(),
                            EnvelopeFieldType::String,
                        )]),
                    },
                )]),
            };
            let result = (|| -> Result<(), clinker_format::FormatError> {
                let mut reader = MultiRecordReader::new_csv_admitted(
                    input.as_bytes(),
                    spec,
                    CsvDialect {
                        delimiter: b',',
                        quote_char: b'"',
                        has_header: true,
                    },
                    Charset::Latin1,
                    DecodeWorkspace::new(AllocationResources::new(authority.clone()))?,
                    TextStorage::Shared,
                )?;
                let sections = reader.prepare_document(&envelope)?;
                let row = reader.next_record()?.unwrap();
                assert_eq!(
                    row.get("label"),
                    Some(&clinker_record::Value::from(text.as_str()))
                );
                assert!(reader.next_record()?.is_none());
                drop(reader);
                drop(sections);
                drop(row);
                Ok(())
            })();
            FAIL_ALLOCATION.with(|fail| fail.set(None));
            let attempts = ALLOCATIONS.with(|count| count.replace(None));
            let reached = authority.calls.load(SeqCst) >= fail_at;
            if reached {
                if allocator {
                    assert!(attempts.is_some_and(|count| count > 0));
                }
                let kind = if allocator {
                    ResourceErrorKind::Allocation
                } else {
                    ResourceErrorKind::Budget
                };
                assert!(
                    matches!(&result, Err(clinker_format::FormatError::Resource(error)) if error.kind == kind),
                    "boundary {fail_at}, allocator={allocator}: {result:?}"
                );
                failures += 1;
            } else {
                result.as_ref().unwrap();
            }
            drop(result);
            assert_eq!(
                authority.used.load(SeqCst),
                0,
                "boundary {fail_at}, allocator={allocator}"
            );
            if !reached {
                break;
            }
        }
        assert!(
            failures > 30 && failures < 299,
            "all metadata/header/discriminator/cell/lookahead/section/final boundaries must execute"
        );
    }
}

#[test]
fn decode_multi_record_resource_refusal_aborts_despite_continue() {
    let oversized = "x".repeat(2 * 1024 * 1024);
    let schema = r#"        discriminator: { field: kind }
        records:
          - id: metadata
            tag: H
            columns:
              - { name: kind, type: string }
              - { name: label, type: string }
          - id: detail
            tag: D
            columns:
              - { name: kind, type: string }
              - { name: label, type: string }
      envelope:
        sections:
          manifest:
            extract: { record_type: H }
            fields:
              label: string"#;
    for (has_header, input) in [
        (true, format!("kind,{oversized}\nH,ok\nD,ok\n")),
        (false, format!("{oversized},ok\n")),
        (false, format!("H,{oversized}\nD,ok\n")),
        (false, format!("D,{oversized}\n")),
    ] {
        let root = tempfile::tempdir().unwrap();
        let options =
            format!("      options: {{ has_header: {has_header}, encoding: iso-8859-1 }}");
        let error = decode_file_run(
            root.path(),
            &[input.as_bytes()],
            &options,
            schema,
            "1M",
            None,
        )
        .unwrap_err();
        assert!(
            matches!(error, clinker_plan::error::PipelineError::Format(clinker_format::FormatError::Resource(error)) if error.kind == ResourceErrorKind::Budget)
        );
        assert!(
            std::fs::read(root.path().join("output.csv"))
                .unwrap()
                .is_empty()
        );
    }
}

fn xml_runtime_run(
    root: &std::path::Path,
    spill: bool,
    cap: Option<u64>,
    fault: CsvRuntimeFault,
    envelope: bool,
) -> (
    Result<clinker_exec::executor::ExecutionReport, clinker_plan::error::PipelineError>,
    clinker_exec::output::staging::OutputStagingRegistry,
    std::path::PathBuf,
) {
    use clinker_exec::executor::{PipelineExecutor, PipelineRunParams, WriterRegistry};
    let yaml = r##"
pipeline:
  name: prepared_xml
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      path: input.csv
      schema:
        - { name: value, type: string }
  - type: transform
    name: nested
    input: rows
    config:
      cxl: |
        emit value = {"@kind": "entry", "#text": value, child: [1, 2]}
  - type: sink
    name: result
    input: nested
    config:
      name: result
      type: xml
      path: output.xml
"##;
    let invalid = matches!(fault, CsvRuntimeFault::InvalidBody);
    let yaml = if invalid {
        yaml.replace(
            "type: csv\n      path: input.csv",
            "type: json\n      path: input.json",
        )
        .replace("type: string", "type: any")
    } else {
        yaml.to_owned()
    };
    let yaml = if envelope {
        yaml.replace(
            "type: csv\n      path: input.csv",
            r#"type: json
      path: input.json
      options:
        record_path: records
      envelope:
        sections:
          Opening:
            extract: { json_pointer: "/Opening" }
            fields:
              id: int
          Closing:
            extract: { json_pointer: "/Closing" }
            fields:
              status: string"#,
        )
        .replace(
            "      path: output.xml",
            r#"      path: output.xml
      reconstruct_envelope: true
      options:
        envelope:
          header_from_doc: Opening
          footer_from_doc: Closing
          footer_record_count_field: rows"#,
        )
    } else {
        yaml
    };
    let config = clinker_plan::config::parse_config(&yaml).unwrap();
    let plan = config
        .compile(&clinker_plan::config::CompileContext::default())
        .unwrap();
    let destination = root.join("output.xml");
    let staging = clinker_exec::output::staging::OutputStagingRegistry::default();
    let (_, file) = staging
        .stage_output(
            "result",
            clinker_plan::config::IfExistsPolicy::Error,
            false,
            |_| Ok(destination.clone()),
        )
        .unwrap();
    struct PrefixFile {
        file: std::fs::File,
        calls: Arc<std::sync::atomic::AtomicUsize>,
    }
    impl Write for PrefixFile {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                self.file.write(&bytes[..1])
            } else {
                Err(std::io::ErrorKind::BrokenPipe.into())
            }
        }
        fn flush(&mut self) -> std::io::Result<()> {
            panic!("failed delivery must not be retried or flushed")
        }
    }
    let raw: Box<dyn Write + Send> = match fault {
        CsvRuntimeFault::Prefix(calls) => Box::new(PrefixFile { file, calls }),
        _ => Box::new(file),
    };
    let registry = WriterRegistry {
        single: [("result".into(), raw)].into(),
        output_staging: staging.clone(),
        ..Default::default()
    };
    let input = if envelope {
        format!(
            r#"{{"Opening":{{"id":7}},"Closing":{{"status":"done"}},"records":[{{"value":"{}"}}]}}"#,
            "x".repeat(100_000)
        )
    } else if invalid {
        r#"[{"value":["invalid"]}]"#.to_owned()
    } else {
        format!("value\n{}\n", "x".repeat(100_000))
    };
    let readers = [(
        "rows".into(),
        clinker_exec::executor::single_file_reader(
            "input.csv",
            Box::new(std::io::Cursor::new(input.into_bytes())),
        ),
    )]
    .into();
    let (producer, receiver) = telemetry();
    let params = PipelineRunParams {
        telemetry_producer: Some(producer),
        spill_root_dir: spill.then(|| root.to_owned()),
        spill_disk_cap_bytes: cap,
        ..Default::default()
    };
    let result = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, registry, &params);
    if result.is_ok() {
        use clinker_exec::telemetry::MetricKey;
        let mut spills = 0;
        let mut stages = 0;
        while let Some(batch) = receiver.try_recv_batch() {
            spills += batch.metric(MetricKey::WriterSpillCompleted);
            stages += batch.metric(MetricKey::WriterStageCompleted);
        }
        assert!(stages >= 2, "runtime must reach prepared XML operations");
        assert_eq!(spills > 0, spill, "spill qualification must really spill");
    }
    (result, staging, destination)
}

#[test]
fn xml_runtime_disk_stage_denial_never_publishes_root_or_body() {
    let root = tempfile::tempdir().unwrap();
    let (result, staging, destination) =
        xml_runtime_run(root.path(), true, Some(1), CsvRuntimeFault::None, false);
    assert!(
        matches!(&result, Err(clinker_plan::error::PipelineError::Format(clinker_format::FormatError::Resource(error))) if error.kind == ResourceErrorKind::DiskQuota),
        "XML operations must retain typed disk refusal: {result:?}"
    );
    assert!(!destination.exists());
    for partial in staging.partials() {
        assert_eq!(std::fs::metadata(partial.partial_path).unwrap().len(), 0);
    }
}

#[test]
fn xml_runtime_nested_cxl_memory_and_spill_files_match_exact_bytes() {
    for envelope in [false, true] {
        for spill in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let (result, _, destination) =
                xml_runtime_run(root.path(), spill, None, CsvRuntimeFault::None, envelope);
            result.unwrap();
            let text = "x".repeat(100_000);
            let record = format!(
                r#"<Record><value kind="entry">{text}<child>1</child><child>2</child></value></Record>"#
            );
            let expected = if envelope {
                format!(
                    "<Root><Document><header><id>7</id></header>{record}<footer><status>done</status><rows>1</rows></footer></Document></Root>"
                )
            } else {
                format!("<Root>{record}</Root>")
            };
            assert_eq!(std::fs::read(&destination).unwrap(), expected.as_bytes());
            assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
        }
    }
}

#[test]
fn xml_runtime_factory_writer_and_config_deallocate_before_release() {
    use clinker_format::counting::{CountingWriter, SharedByteCounter};
    use clinker_format::preparation::PreparedWriter;
    use clinker_format::splitting::WriterFactory;
    use clinker_format::xml::writer::{XmlEncoder, XmlEncoderConfig, XmlWriterConfig};
    use clinker_record::owned_storage::SharedStorage;
    use clinker_record::{Record, Schema, Value};
    let arb = Arc::new(MemoryArbitrator::with_policy(
        256 * 1024,
        0.8,
        0.7,
        Box::new(NoOpPolicy),
    ));
    let shutdown = ShutdownToken::detached();
    let provider = ExecutorResources::new(
        arb.clone(),
        shutdown.clone(),
        None,
        NonZeroUsize::new(1).unwrap(),
        None,
    )
    .unwrap();
    let resources = provider.resources();
    let scope = resources.scope().unwrap();
    let observer = arb.writer_resource_observer();
    NATIVE_BACKINGS.with(|watch| {
        watch.set(Some(NativeBackingWatch {
            observer: &observer,
            capture: Some(0),
            capture_last: true,
            backings: [NativeBacking::default(); 3],
        }))
    });
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            NATIVE_BACKINGS.with(|watch| watch.set(None));
        }
    }
    let reset = Reset;
    let config = XmlEncoderConfig::new((&XmlWriterConfig::default()).into(), &resources).unwrap();
    NATIVE_BACKINGS.with(|watch| {
        let mut state = watch.get().unwrap();
        state.capture = None;
        state.capture_last = false;
        watch.set(Some(state));
    });
    let config_memory = observer.usage().memory;
    let alias = config.clone();
    let captured_resources = resources.clone();
    let make = move |destination, schema| {
        XmlEncoder::from_config(schema, config.clone())?
            .into_boxed_writer(destination, captured_resources.clone())
    };
    let factory_bytes = std::mem::size_of_val(&make);
    NATIVE_BACKINGS.with(|watch| {
        let mut state = watch.get().unwrap();
        state.capture = Some(1);
        watch.set(Some(state));
    });
    let factory = WriterFactory::try_new(make, scope.allocation()).unwrap();
    let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["a.b".into()])));
    let record = Record::new(schema.clone(), vec![Value::Integer(7)]);
    let destination: Box<dyn Write + Send> = Box::new(std::io::sink());
    let counter = SharedByteCounter::new();
    let counting = CountingWriter::new(destination, counter.clone());
    NATIVE_BACKINGS.with(|watch| {
        let mut state = watch.get().unwrap();
        state.capture = Some(2);
        watch.set(Some(state));
    });
    let mut writer = factory.create(counting, schema).unwrap();
    let backing = NATIVE_BACKINGS.with(|watch| watch.get().unwrap().backings);
    assert!(backing[0].bytes > 0);
    assert_eq!(backing[1].bytes, factory_bytes);
    assert_eq!(
        backing[2].bytes,
        std::mem::size_of::<PreparedWriter<CountingWriter<Box<dyn Write + Send>>, XmlEncoder>>()
    );
    assert_eq!(
        observer.usage().memory,
        config_memory + backing[1].bytes as u64 + backing[2].bytes as u64
    );
    writer.write_record(&record).unwrap();
    let committed_memory = observer.usage().memory;
    let committed_bytes = counter.bytes_written();
    let changed = Record::new(
        SharedStorage::from_arc(Arc::new(Schema::new(vec!["changed.path".into()]))),
        vec![Value::Integer(9)],
    );
    let pressure = scope
        .reserve(Layout::array::<u8>((arb.limit() - committed_memory) as usize).unwrap())
        .unwrap();
    assert!(
        matches!(writer.write_record(&changed), Err(clinker_format::FormatError::Resource(error)) if error.kind == ResourceErrorKind::Budget)
    );
    assert_eq!(counter.bytes_written(), committed_bytes);
    drop(pressure);
    assert_eq!(observer.usage().memory, committed_memory);
    writer.write_record(&changed).unwrap();
    drop(factory);
    shutdown.request();
    assert!(
        matches!(writer.write_record(&record), Err(clinker_format::FormatError::Resource(error)) if error.kind == ResourceErrorKind::Cancelled)
    );
    drop(writer);
    assert_eq!(observer.usage().memory, config_memory);
    arb.close_writer_resources();
    assert!(observer.is_closed());
    drop(alias);
    let backing = NATIVE_BACKINGS.with(|watch| watch.get().unwrap().backings);
    for owner in backing {
        assert!(owner.admitted_at_allocation >= owner.bytes as u64);
        assert!(owner.admitted_at_deallocation.unwrap() >= owner.bytes as u64);
    }
    assert_eq!(observer.usage().memory, 0);
    assert_eq!(observer.usage().disk, 0);
    assert_eq!(observer.usage().descriptors, 0);
    assert_eq!(arb.consumer_count(), 0);
    drop(reset);
}

#[test]
fn nested_runtime_pressure_and_delivery_outcomes_ignore_telemetry_admission() {
    use clinker_exec::telemetry::{MetricKey, SpanName, SpanStatus};
    use clinker_format::json::writer::{JsonEncoder, JsonWriterConfig};
    use clinker_format::xml::writer::{XmlEncoder, XmlWriterConfig};
    use clinker_format::{FormatError, FormatWriterHandle};
    use clinker_record::{Record, Schema, Value, owned_storage::SharedStorage};
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    struct Destination {
        bytes: Arc<Mutex<Vec<u8>>>,
        calls: Arc<AtomicUsize>,
        token: ShutdownToken,
        prefix_failure: bool,
        cancel_at: Option<usize>,
    }
    impl Write for Destination {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let calls = self.calls.fetch_add(1, Ordering::SeqCst);
            if self.prefix_failure && calls > 0 {
                return Err(std::io::ErrorKind::BrokenPipe.into());
            }
            let n = if self.prefix_failure {
                3.min(bytes.len())
            } else {
                bytes.len()
            };
            let mut output = self.bytes.lock().unwrap();
            output.extend_from_slice(&bytes[..n]);
            if self.cancel_at == Some(output.len()) {
                self.token.request();
            }
            Ok(n)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    for xml in [false, true] {
        let text = "x".repeat(100_000);
        let record_bytes = if xml {
            format!("<Root><Record><value>{text}</value></Record>")
        } else {
            format!("[\n{{\"value\":\"{text}\"}}")
        };
        let complete = format!("{record_bytes}{}", if xml { "</Root>" } else { "\n]\n" });
        for spill in [false, true] {
            for mode in 0..3 {
                for fault in ["none", "pressure", "disk", "descriptor", "prefix", "cancel"] {
                    if !spill && matches!(fault, "disk" | "descriptor" | "pressure") {
                        continue;
                    }
                    let root = tempfile::tempdir().unwrap();
                    let arb = Arc::new(MemoryArbitrator::with_policy(
                        512 * 1024,
                        0.8,
                        0.7,
                        Box::new(NoOpPolicy),
                    ));
                    if fault == "disk" {
                        arb.set_max_spill_bytes(0).unwrap();
                    }
                    let token = ShutdownToken::detached();
                    let (producer, receiver) = telemetry();
                    if mode == 2 {
                        saturate_writer_telemetry(&producer);
                    }
                    let arena = producer.snapshot();
                    let storage = configured(root.path());
                    let provider = ExecutorResources::new(
                        arb.clone(),
                        token.clone(),
                        spill.then_some(&storage),
                        NonZeroUsize::new(1).unwrap(),
                        (mode != 0).then(|| producer.clone()),
                    )
                    .unwrap();
                    let baseline = arb.writer_resource_usage().memory;
                    let bytes = Arc::new(Mutex::new(Vec::new()));
                    let calls = Arc::new(AtomicUsize::new(0));
                    let destination = Destination {
                        bytes: bytes.clone(),
                        calls: calls.clone(),
                        token,
                        prefix_failure: fault == "prefix",
                        cancel_at: (fault == "cancel").then_some(record_bytes.len()),
                    };
                    let schema =
                        SharedStorage::from_arc(Arc::new(Schema::new(vec!["value".into()])));
                    let record =
                        Record::new(schema.clone(), vec![Value::String(text.as_str().into())]);
                    let resources = provider.resources();
                    let mut writer: FormatWriterHandle = if xml {
                        XmlEncoder::new(schema, &XmlWriterConfig::default(), resources.clone())
                            .unwrap()
                            .into_boxed_writer(destination, resources.clone())
                            .unwrap()
                    } else {
                        JsonEncoder::new(schema, &JsonWriterConfig::default(), resources.clone())
                            .unwrap()
                            .into_boxed_writer(destination, resources.clone())
                            .unwrap()
                    };
                    let scope = resources.scope().unwrap();
                    let pressure = (fault == "pressure").then(|| {
                        scope
                            .reserve(
                                Layout::array::<u8>(
                                    (arb.limit() - arb.writer_resource_usage().memory - 96 * 1024)
                                        as usize,
                                )
                                .unwrap(),
                            )
                            .unwrap()
                    });
                    let descriptor = (fault == "descriptor").then(|| scope.stage().unwrap());
                    let result = writer.write_record(&record);
                    let success = matches!(fault, "none" | "pressure");
                    if success {
                        result.unwrap();
                        writer.flush_bytes().unwrap();
                        assert_eq!(*bytes.lock().unwrap(), record_bytes.as_bytes());
                        writer.flush().unwrap();
                        writer.flush().unwrap();
                        assert_eq!(*bytes.lock().unwrap(), complete.as_bytes());
                    } else {
                        let error = result.unwrap_err();
                        match fault {
                            "disk" => assert!(
                                matches!(error, FormatError::Resource(error) if error.kind == ResourceErrorKind::DiskQuota)
                            ),
                            "descriptor" => assert!(
                                matches!(error, FormatError::Resource(error) if error.kind == ResourceErrorKind::DescriptorQuota)
                            ),
                            "prefix" => assert!(
                                matches!(error, FormatError::Io(error) if error.kind() == std::io::ErrorKind::BrokenPipe)
                            ),
                            "cancel" => assert!(
                                matches!(error, FormatError::Resource(error) if error.kind == ResourceErrorKind::Cancelled)
                            ),
                            _ => unreachable!(),
                        }
                        let expected = match fault {
                            "prefix" => &record_bytes.as_bytes()[..3],
                            "cancel" => record_bytes.as_bytes(),
                            _ => b"",
                        };
                        assert_eq!(*bytes.lock().unwrap(), expected);
                        if matches!(fault, "prefix" | "cancel") {
                            let attempts = calls.load(Ordering::SeqCst);
                            assert!(
                                matches!(writer.flush(), Err(FormatError::Resource(error)) if error.kind == ResourceErrorKind::DeliveryPoisoned)
                            );
                            assert_eq!(calls.load(Ordering::SeqCst), attempts);
                        }
                    }
                    drop(descriptor);
                    drop(pressure);
                    drop(writer);
                    drop(scope);
                    drop(resources);
                    assert_eq!(
                        arb.writer_resource_usage().memory,
                        baseline,
                        "{xml}/{spill}/{mode}/{fault}"
                    );
                    assert_eq!(arb.writer_resource_usage().disk, 0);
                    assert_eq!(arb.writer_resource_usage().descriptors, 0);
                    assert_eq!(provider.cleanup_debt_count(), 0);
                    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
                    assert_eq!(producer.snapshot().owned_bytes, arena.owned_bytes);
                    if mode == 2 {
                        assert_eq!(producer.snapshot().accepted, arena.accepted);
                    }
                    if mode == 0 {
                        assert!(receiver.try_recv_batch().is_none());
                    } else {
                        let batch = receiver.try_recv_batch().unwrap();
                        assert_eq!(
                            batch.metric(MetricKey::WriterStageCompleted),
                            if success { 2 } else { 0 }
                        );
                        assert_eq!(
                            batch.metric(MetricKey::WriterStageInterrupted),
                            u64::from(fault == "cancel")
                        );
                        assert_eq!(
                            batch.metric(MetricKey::WriterStageFailed),
                            u64::from(matches!(fault, "disk" | "descriptor")),
                            "{xml}/{spill}/{mode}/{fault}"
                        );
                        // Destination errors belong to Sink; the storage owner
                        // abandons unconsumed bytes without inventing a storage failure.
                        assert_eq!(
                            batch.metric(MetricKey::WriterStageDropped),
                            u64::from(matches!(fault, "prefix" | "descriptor"))
                        );
                        if success {
                            assert_eq!(batch.metric(MetricKey::WriterSpillCompleted) > 0, spill);
                        }
                        let spans: Vec<_> = batch
                            .traces()
                            .iter()
                            .filter(|span| span.name == SpanName::WriterStage)
                            .collect();
                        if mode == 2 {
                            assert!(spans.is_empty());
                        } else if fault != "descriptor" {
                            assert_eq!(spans.len(), if success { 2 } else { 1 });
                            for span in spans {
                                assert!(
                                    span.started_at_unix_nanos > 0
                                        && span.started_at_unix_nanos <= span.ended_at_unix_nanos
                                );
                                assert_eq!(
                                    span.status,
                                    if success {
                                        SpanStatus::Ok
                                    } else if matches!(fault, "cancel" | "prefix") {
                                        SpanStatus::Unset
                                    } else {
                                        SpanStatus::Error
                                    }
                                );
                            }
                        }
                    }
                    drop(provider);
                    assert_eq!(arb.writer_resource_usage().memory, 0);
                    assert_eq!(arb.consumer_count(), 0);
                }
            }
        }
    }
}

#[test]
fn nested_runtime_sink_reports_exact_outcomes_without_teardown_retry() {
    use clinker_exec::executor::{PipelineExecutor, PipelineRunParams, WriterRegistry};
    use clinker_exec::telemetry::{MetricKey, SpanName, SpanStatus};
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    struct Destination {
        bytes: Arc<Mutex<Vec<u8>>>,
        calls: Arc<AtomicUsize>,
        fault: &'static str,
        token: ShutdownToken,
    }
    impl Write for Destination {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fault == "prefix" && call != 0 {
                return Err(std::io::ErrorKind::BrokenPipe.into());
            }
            let n = if self.fault == "prefix" {
                3
            } else {
                bytes.len()
            };
            self.bytes.lock().unwrap().extend_from_slice(&bytes[..n]);
            if self.fault == "cancel" {
                self.token.request();
            }
            Ok(n)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            assert_eq!(
                self.fault, "none",
                "failed or cancelled destinations must not flush"
            );
            Ok(())
        }
    }
    for format in ["json", "xml"] {
        for (full, fault) in [false, true]
            .into_iter()
            .flat_map(|full| ["none", "prefix", "cancel"].map(|fault| (full, fault)))
        {
            let root = tempfile::tempdir().unwrap();
            let yaml = format!(
                r#"
pipeline:
  name: exact_delivery
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      path: input.csv
      schema:
        - {{ name: value, type: string }}
  - type: sink
    name: result
    input: rows
    config:
      name: result
      type: {format}
      path: output.{format}
"#
            );
            let plan = clinker_plan::config::parse_config(&yaml)
                .unwrap()
                .compile(&clinker_plan::config::CompileContext::default())
                .unwrap();
            let bytes = Arc::new(Mutex::new(Vec::new()));
            let calls = Arc::new(AtomicUsize::new(0));
            let token = ShutdownToken::detached();
            let registry = WriterRegistry {
                single: [(
                    "result".into(),
                    Box::new(Destination {
                        bytes: bytes.clone(),
                        calls: calls.clone(),
                        fault,
                        token: token.clone(),
                    }) as Box<dyn Write + Send>,
                )]
                .into(),
                ..Default::default()
            };
            let readers = [(
                "rows".into(),
                clinker_exec::executor::single_file_reader(
                    "input.csv",
                    Box::new(std::io::Cursor::new(
                        format!("value\n{}\n", "x".repeat(100_000)).into_bytes(),
                    )),
                ),
            )]
            .into();
            let (producer, receiver) = telemetry();
            if full {
                saturate_writer_telemetry(&producer);
            }
            let arena = producer.snapshot();
            let params = PipelineRunParams {
                shutdown_token: Some(token),
                spill_root_dir: Some(root.path().to_owned()),
                telemetry_producer: Some(producer.clone()),
                ..Default::default()
            };
            let result =
                PipelineExecutor::run_plan_with_readers_writers(&plan, readers, registry, &params);
            let value = "x".repeat(100_000);
            let complete = if format == "json" {
                format!("[\n{{\"value\":\"{value}\"}}\n]\n")
            } else {
                format!("<Root><Record><value>{value}</value></Record></Root>")
            };
            let (accepted, expected_counts, status) = match fault {
                "prefix" => {
                    assert!(result.is_err());
                    assert_eq!(
                        calls.load(Ordering::SeqCst),
                        2,
                        "{format}: teardown must not retry"
                    );
                    (3, [1, 1, 0, 1, 3, 0, 0], SpanStatus::Error)
                }
                "cancel" => {
                    let report = result.unwrap();
                    assert_eq!(report.per_source_record_counts["rows"], 1);
                    assert!(report.interrupted);
                    assert_eq!(report.counters.records_written, 0);
                    assert_eq!(calls.load(Ordering::SeqCst), 1);
                    let n = clinker_format::preparation::PROGRESS_BYTES / 2;
                    (n, [1, 0, 0, 0, n as u64, 0, 1], SpanStatus::Unset)
                }
                _ => {
                    let report = result.unwrap();
                    assert_eq!(report.per_source_record_counts["rows"], 1);
                    assert!(!report.interrupted);
                    assert_eq!(report.counters.records_written, 1);
                    (
                        complete.len(),
                        [1, 0, 1, 0, complete.len() as u64, 1, 0],
                        SpanStatus::Ok,
                    )
                }
            };
            assert_eq!(*bytes.lock().unwrap(), &complete.as_bytes()[..accepted]);
            assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
            let mut counts = [0; 7];
            let mut sink_spans = 0;
            let mut sources = [0; 4];
            let keys = [
                MetricKey::SinkStarted,
                MetricKey::SinkFailed,
                MetricKey::SinkRecords,
                MetricKey::SinkErrors,
                MetricKey::SinkBytes,
                MetricKey::SinkCompleted,
                MetricKey::SinkInterrupted,
            ];
            while let Some(batch) = receiver.try_recv_batch() {
                for (index, key) in [
                    MetricKey::SourceStarted,
                    MetricKey::SourceCompleted,
                    MetricKey::SourceInterrupted,
                    MetricKey::SourceFailed,
                ]
                .iter()
                .enumerate()
                {
                    sources[index] += batch.metric(*key);
                }
                for (index, key) in keys.iter().enumerate() {
                    counts[index] += batch.metric(*key);
                }
                for span in batch
                    .traces()
                    .iter()
                    .filter(|span| span.name == SpanName::Sink)
                {
                    sink_spans += 1;
                    assert_eq!(span.status, status);
                    assert!(
                        span.started_at_unix_nanos > 0
                            && span.started_at_unix_nanos <= span.ended_at_unix_nanos
                    );
                }
            }
            assert_eq!(
                counts, expected_counts,
                "{format}/{fault}: count only the accepted byte prefix"
            );
            assert_eq!(sources[0], 1);
            assert_eq!(sources[1] + sources[2], 1);
            assert_eq!(sources[3], 0);
            assert!(sink_spans <= 1);
            let after = producer.snapshot();
            assert_eq!(after.owned_bytes, arena.owned_bytes);
            if full {
                assert_eq!(sink_spans, 0);
                assert_eq!(after.accepted, arena.accepted);
            } else if sink_spans == 0 {
                // Even with free byte capacity, the fixed slot inventory may
                // fill during source admission. Outcomes remain exact when
                // that denies the terminal span; never enlarge the arena.
                assert!(
                    after.contention_drops > arena.contention_drops
                        || after.full_drops > arena.full_drops,
                    "{format}/{fault}: {after:?} vs {arena:?}"
                );
            }
        }
    }
}

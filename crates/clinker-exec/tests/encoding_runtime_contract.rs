//! Text format coverage through compiled plans and physical files.

use std::collections::BTreeSet;
use std::io::Write;
use std::sync::Arc;

use clinker_exec::dlq::{DiscardingDlqSink, DlqSink};
use clinker_exec::executor::{
    ExecutionReport, PipelineExecutor, PipelineRunParams, WriterRegistry,
};
use clinker_exec::source::{SourceInput, multi_file::FileSlot};
use clinker_plan::config::{CompileContext, parse_config};

#[path = "common/dlq_sink.rs"]
mod dlq_sink;

use dlq_sink::{CollectingDlqSink, DlqRow};

// This list is intentionally independent of the loops that execute the matrix.
// Each row includes an ordered source and an intervening rejected record.
const EXPECTED_READER_ROWS: &[&str] = &[
    "utf-8/single/header/file",
    "utf-8/single/header/files",
    "utf-8/single/no-header/file",
    "utf-8/single/no-header/files",
    "utf-8/multi/header/file",
    "utf-8/multi/header/files",
    "utf-8/multi/no-header/file",
    "utf-8/multi/no-header/files",
    "utf-8/envelope/header/file",
    "utf-8/envelope/header/files",
    "utf-8/envelope/no-header/file",
    "utf-8/envelope/no-header/files",
    "iso-8859-1/single/header/file",
    "iso-8859-1/single/header/files",
    "iso-8859-1/single/no-header/file",
    "iso-8859-1/single/no-header/files",
    "iso-8859-1/multi/header/file",
    "iso-8859-1/multi/header/files",
    "iso-8859-1/multi/no-header/file",
    "iso-8859-1/multi/no-header/files",
    "iso-8859-1/envelope/header/file",
    "iso-8859-1/envelope/header/files",
    "iso-8859-1/envelope/no-header/file",
    "iso-8859-1/envelope/no-header/files",
];

fn pipeline(encoding: &str, mode: &str, header: bool) -> String {
    let schema = if mode == "single" {
        let (key, label) = if header {
            ("key", "label")
        } else {
            ("col_0", "col_1")
        };
        format!(
            "        - {{ name: key, source_name: {key}, type: int }}\n        - {{ name: label, source_name: {label}, type: string }}"
        )
    } else {
        r#"        discriminator: { field: marker }
        records:
          - id: metadata
            tag: Hé
            columns:
              - { name: marker, type: string }
              - { name: batch, type: string }
          - id: detail
            tag: Dé
            columns:
              - { name: marker, type: string }
              - { name: key, type: int }
              - { name: label, type: string }"#
            .into()
    };
    let envelope = if mode == "envelope" {
        r#"      envelope:
        sections:
          manifest:
            extract: { record_type: Hé }
            fields:
              batch: string
"#
    } else {
        ""
    };
    let transform = if mode == "envelope" {
        r#"  - type: transform
    name: attach
    input: rows
    config:
      cxl: "emit document_batch = $doc.manifest.batch"
"#
    } else {
        ""
    };
    let upstream = if mode == "envelope" { "attach" } else { "rows" };
    format!(
        r#"
pipeline:
  name: csv_reader_contract
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
      dlq_granularity: record
      options: {{ encoding: {encoding}, has_header: {header} }}
      sort_order: [key]
      schema:
{schema}
{envelope}{transform}  - type: sink
    name: out
    input: {upstream}
    config:
      name: out
      type: csv
      path: output.csv
"#
    )
}

/// Run `yaml` over physical input files: the report, the Sink's bytes, and
/// the dead-letter rows.
fn run_files(yaml: &str, inputs: &[Vec<u8>]) -> (ExecutionReport, Vec<u8>, Vec<DlqRow>) {
    let root = tempfile::tempdir().unwrap();
    let plan = parse_config(yaml)
        .unwrap()
        .compile(&CompileContext::default())
        .unwrap();
    let files = inputs
        .iter()
        .enumerate()
        .map(|(index, bytes)| {
            let path = root.path().join(format!("input-{index}.csv"));
            std::fs::write(&path, bytes).unwrap();
            FileSlot::new(path.clone(), Box::new(std::fs::File::open(path).unwrap()))
        })
        .collect();
    let output = root.path().join("output.csv");
    let sink = CollectingDlqSink::new();
    let writers = WriterRegistry {
        single: [(
            "out".into(),
            Box::new(std::fs::File::create(&output).unwrap()) as Box<dyn Write + Send>,
        )]
        .into(),
        dlq_sink: Some(Arc::clone(&sink) as Arc<dyn DlqSink>),
        ..Default::default()
    };
    let report = PipelineExecutor::run_plan_with_readers_writers(
        &plan,
        [("rows".into(), SourceInput::Files(files))].into(),
        writers,
        &PipelineRunParams::default(),
    )
    .unwrap();
    (report, std::fs::read(output).unwrap(), sink.rows())
}

#[test]
fn reader_csv_expected_rows_equal_executed_rows() {
    let expected: BTreeSet<_> = EXPECTED_READER_ROWS.iter().copied().collect();
    assert_eq!(expected.len(), 24);
    let mut executed = BTreeSet::new();
    for encoding in ["utf-8", "iso-8859-1"] {
        for mode in ["single", "multi", "envelope"] {
            for header in [true, false] {
                for file_count in [1, 2] {
                    let id = format!(
                        "{encoding}/{mode}/{}/{}",
                        if header { "header" } else { "no-header" },
                        if file_count == 1 { "file" } else { "files" }
                    );
                    let mut inputs = Vec::new();
                    for index in 0..file_count {
                        let mut text = String::new();
                        if header {
                            text.push_str(if mode == "single" {
                                "key,label\n"
                            } else {
                                "marker,key,label\n"
                            });
                        }
                        if mode == "envelope" {
                            text.push_str(if index == 0 {
                                "Hé,Café\n"
                            } else {
                                "Hé,Crème\n"
                            });
                        }
                        text.push_str(match (mode, index) {
                            ("single", 0) => "1,Café\nbad,refusé\n2,Crème\n",
                            ("single", _) => "3,Café\nbad,refusé\n4,Crème\n",
                            (_, 0) => "Dé,1,Café\nXé,0,refusé\nDé,2,Crème\n",
                            (_, _) => "Dé,3,Café\nXé,0,refusé\nDé,4,Crème\n",
                        });
                        let bytes = if encoding == "utf-8" {
                            // Each physical reader must independently strip its leading BOM.
                            [b"\xef\xbb\xbf".as_slice(), text.as_bytes()].concat()
                        } else {
                            text.chars()
                                .map(|ch| u8::try_from(ch as u32).unwrap())
                                .collect()
                        };
                        inputs.push(bytes);
                    }
                    let (report, output, dlq) =
                        run_files(&pipeline(encoding, mode, header), &inputs);
                    let first = match mode {
                        "single" => "key,label\n1,Café\n2,Crème\n",
                        "multi" => {
                            "record_type,marker,batch,key,label\ndetail,Dé,,1,Café\ndetail,Dé,,2,Crème\n"
                        }
                        "envelope" => {
                            "record_type,marker,batch,key,label,document_batch\ndetail,Dé,,1,Café,Café\ndetail,Dé,,2,Crème,Café\n"
                        }
                        _ => unreachable!(),
                    };
                    let second = match mode {
                        "single" => "3,Café\n4,Crème\n",
                        "multi" => "detail,Dé,,3,Café\ndetail,Dé,,4,Crème\n",
                        "envelope" => "detail,Dé,,3,Café,Crème\ndetail,Dé,,4,Crème,Crème\n",
                        _ => unreachable!(),
                    };
                    let expected_bytes = if file_count == 1 {
                        first.to_string()
                    } else {
                        format!("{first}{second}")
                    };
                    assert_eq!(output, expected_bytes.as_bytes(), "{id}");
                    assert_eq!(report.counters.total_count, 3 * file_count as u64, "{id}");
                    assert_eq!(
                        report.counters.records_written,
                        2 * file_count as u64,
                        "{id}"
                    );
                    assert_eq!(report.counters.dlq_count, file_count as u64, "{id}");
                    assert_eq!(dlq.len(), file_count, "{id}");
                    for (index, row) in dlq.iter().enumerate() {
                        assert_eq!(
                            row.category(),
                            Some(if mode == "single" {
                                "type_coercion_failure"
                            } else {
                                "structural_validation"
                            }),
                            "{id}"
                        );
                        assert!(
                            row.source_file().ends_with(&format!("input-{index}.csv")),
                            "{id}: {row:?}"
                        );
                        if mode == "single" {
                            assert_eq!(row.field("key"), Some("bad"), "{id}: {row:?}");
                            assert_eq!(row.field("label"), Some("refusé"), "{id}");
                        } else {
                            assert_eq!(
                                row.field("_cxl_dlq_source_record"),
                                Some(r#"["Xé","0","refusé"]"#),
                                "{id}"
                            );
                        }
                    }
                    assert!(executed.insert(id));
                }
            }
        }
    }
    assert_eq!(
        executed.iter().map(String::as_str).collect::<BTreeSet<_>>(),
        expected
    );
}

#[test]
fn reader_csv_multi_record_repeated_values_remain_rejected() {
    let yaml = pipeline("utf-8", "multi", false);
    for (yaml, code) in [
        (
            yaml.replace(
                "      schema:",
                "      split_values: [{ field: label, delimiter: ';' }]\n      schema:",
            ),
            "E358",
        ),
        (
            yaml.replace(
                "name: label, type: string",
                "name: label, type: string, multiple: true",
            ),
            "E361",
        ),
    ] {
        let diagnostics = parse_config(&yaml)
            .unwrap()
            .compile(&CompileContext::default())
            .unwrap_err();
        assert!(
            diagnostics.iter().any(|diagnostic| diagnostic.code == code),
            "{diagnostics:?}"
        );
    }
}

#[test]
fn reader_csv_rejected_aliases_preserve_fields_before_and_after_failure() {
    // Decode-time ownership is covered at the coercion layer by schema_coerce.rs tests csv_multirecord_projection_moves_nested_owners_and_logical_names and csv_multirecord_projection_uses_local_repeated_text_policy.
    let yaml = pipeline("utf-8", "single", true).replace(
        "        - { name: key, source_name: key, type: int }\n        - { name: label, source_name: label, type: string }",
        "        - { name: label, source_name: raw_label, type: string }\n        - { name: key, source_name: raw_key, type: int }\n        - { name: tail, source_name: raw_tail, type: string }",
    );
    let before = "before-invalid-decoded-value-".repeat(20);
    let after = "after-invalid-decoded-value-".repeat(20);
    let input = format!("raw_label,raw_key,raw_tail\n{before},bad,{after}\nok,1,end\n");
    let (report, output, dlq) = run_files(&yaml, &[input.into_bytes()]);
    assert_eq!(output, b"label,key,tail\nok,1,end\n");
    assert_eq!(report.counters.total_count, 2);
    assert_eq!(report.counters.dlq_count, 1);
    let row = &dlq[0];
    assert_eq!(row.field("key"), Some("bad"));
    assert_eq!(row.field("label"), Some(before.as_str()));
    assert_eq!(row.field("tail"), Some(after.as_str()));
}

#[test]
fn reader_csv_no_header_rejection_preserves_unaliased_fields() {
    let yaml = pipeline("utf-8", "single", false)
        .replace("sort_order: [key]", "sort_order: [col_1]")
        .replace(
            "        - { name: key, source_name: col_0, type: int }\n        - { name: label, source_name: col_1, type: string }",
            "        - { name: col_0, type: string }\n        - { name: col_1, type: int }\n        - { name: col_2, type: string }",
        );
    let (report, output, dlq) = run_files(&yaml, &[b"before,bad,after\nok,1,end\n".to_vec()]);
    assert_eq!(output, b"col_0,col_1,col_2\nok,1,end\n");
    assert_eq!(report.counters.total_count, 2);
    assert_eq!(report.counters.dlq_count, 1);
    let row = &dlq[0];
    assert_eq!(row.field("col_0"), Some("before"));
    assert_eq!(row.field("col_1"), Some("bad"));
    assert_eq!(row.field("col_2"), Some("after"));
}

#[test]
fn reader_csv_latin1_bom_shaped_data_survives_each_physical_file() {
    for header in [true, false] {
        let yaml = pipeline("iso-8859-1", "single", header)
            .replace("type: int", "type: string")
            .replace("source_name: key", "source_name: ï»¿key");
        let input = if header {
            b"\xef\xbb\xbfkey,label\nfirst,one\n".as_slice()
        } else {
            b"\xef\xbb\xbffirst,one\n".as_slice()
        };
        let (report, output, _) = run_files(&yaml, &[input.to_vec(), input.to_vec()]);
        assert_eq!(report.counters.total_count, 2);
        assert_eq!(report.counters.dlq_count, 0);
        assert_eq!(
            output,
            if header {
                "key,label\nfirst,one\nfirst,one\n".as_bytes()
            } else {
                "key,label\nï»¿first,one\nï»¿first,one\n".as_bytes()
            }
        );
    }
    let yaml = pipeline("iso-8859-1", "multi", false).replace("tag: Dé", "tag: ï»¿Dé");
    let input = b"\xef\xbb\xbfD\xe9,1,Caf\xe9\n".to_vec();
    let (report, output, _) = run_files(&yaml, &[input.clone(), input]);
    assert_eq!(report.counters.total_count, 2);
    assert_eq!(report.counters.dlq_count, 0);
    assert_eq!(
        output,
        "record_type,marker,batch,key,label\ndetail,ï»¿Dé,,1,Café\ndetail,ï»¿Dé,,1,Café\n"
            .as_bytes()
    );
}

#[test]
fn csv_charset_memory_and_observed_disk_spill_have_identical_bytes() {
    use clinker_exec::telemetry::{MetricKey, TelemetryArena};
    use clinker_plan::config::ClinkerToml;
    let policy = ClinkerToml::parse(
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
    .unwrap()
    .resolve_observability(None)
    .unwrap();
    for (encoding, unit) in [
        ("utf-8", "é".as_bytes()),
        ("iso-8859-1", b"\xe9".as_slice()),
    ] {
        let yaml = pipeline("utf-8", "single", true).replace(
            "      path: output.csv",
            &format!("      path: output.csv\n      options: {{ encoding: {encoding} }}"),
        );
        let plan = parse_config(&yaml)
            .unwrap()
            .compile(&CompileContext::default())
            .unwrap();
        let input = format!("key,label\n1,{}\n", "é".repeat(100_000));
        let mut expected = b"key,label\n1,".to_vec();
        // Repeating an independently literal encoded cell also checks actual
        // expansion size: UTF-8 needs two bytes, true Latin-1 needs one.
        expected.extend(unit.repeat(100_000));
        expected.push(b'\n');
        for spill in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let (producer, receiver) = TelemetryArena::reserve(&policy).unwrap();
            let destination = root.path().join("output.csv");
            let writers = WriterRegistry {
                single: [(
                    "out".into(),
                    Box::new(std::fs::File::create(&destination).unwrap()) as Box<dyn Write + Send>,
                )]
                .into(),
                dlq_sink: Some(Arc::new(DiscardingDlqSink)),
                ..Default::default()
            };
            let readers = [(
                "rows".into(),
                clinker_exec::executor::single_file_reader(
                    "input.csv",
                    Box::new(std::io::Cursor::new(input.as_bytes().to_vec())),
                ),
            )]
            .into();
            let params = PipelineRunParams {
                spill_root_dir: spill.then(|| root.path().to_owned()),
                telemetry_producer: Some(producer),
                ..Default::default()
            };
            let report =
                PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
                    .unwrap();
            assert_eq!(report.counters.total_count, 1);
            assert_eq!(report.counters.records_written, 1);
            assert_eq!(report.counters.dlq_count, 0);
            assert_eq!(
                std::fs::read(&destination).unwrap(),
                expected,
                "{encoding}, spill={spill}"
            );
            let mut completed = 0;
            let mut bytes = 0;
            while let Some(batch) = receiver.try_recv_batch() {
                completed += batch.metric(MetricKey::WriterSpillCompleted);
                bytes += batch.metric(MetricKey::WriterSpillBytes);
                assert_eq!(batch.metric(MetricKey::WriterSpillFailed), 0);
            }
            if spill {
                assert!(
                    completed > 0,
                    "configured storage must really spill: {encoding}"
                );
                assert!(
                    bytes >= expected.len() as u64,
                    "spill byte evidence: {encoding}, {bytes}"
                );
            } else {
                assert_eq!(completed, 0);
                assert_eq!(bytes, 0);
            }
            assert_eq!(
                std::fs::read_dir(root.path()).unwrap().count(),
                1,
                "spill cleanup leaves only the destination"
            );
        }
    }
}

#[test]
fn nested_failed_second_file_preserves_source_and_sink_count_prefixes() {
    use clinker_exec::progress::RunProgress;
    use clinker_exec::telemetry::{MetricKey, TelemetryArena};
    use clinker_plan::config::ClinkerToml;
    const MODES: &[&str] = &[
        "json-array",
        "json-ndjson",
        "json-body",
        "xml-ordinary",
        "xml-body",
        "xml-envelope",
        "xml-prescan",
    ];
    let policy = ClinkerToml::parse(
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
    .unwrap()
    .resolve_observability(None)
    .unwrap();
    let mut executed = BTreeSet::new();
    for mode in MODES {
        let xml = mode.starts_with("xml");
        let format = if xml { "xml" } else { "json" };
        let options = match *mode {
            "json-ndjson" => "      options: { format: ndjson }\n",
            "json-body" => "      options: { record_path: items }\n",
            "xml-body" | "xml-envelope" | "xml-prescan" => {
                "      options: { record_path: Root/items/row }\n"
            }
            _ => "",
        };
        let envelope = if matches!(*mode, "xml-envelope" | "xml-prescan") {
            "      envelope:\n        sections:\n          manifest:\n            extract: { xml_path: /Root/manifest }\n            fields: { batch: int }\n"
        } else {
            ""
        };
        let transform = if *mode == "xml-prescan" {
            "  - type: transform\n    name: attach\n    input: rows\n    config:\n      cxl: |\n        emit id = id\n        emit batch = $doc.manifest.batch\n"
        } else {
            ""
        };
        let upstream = if *mode == "xml-prescan" {
            "attach"
        } else {
            "rows"
        };
        let yaml = format!(
            r#"
pipeline:
  name: native_failure_counts
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: {format}
      path: input.{format}
{options}{envelope}      schema: [{{ name: id, type: int }}]
{transform}  - type: sink
    name: out
    input: {upstream}
    config:
      name: out
      type: json
      path: output.json
      options: {{ format: ndjson }}
"#
        );
        let plan = parse_config(&yaml)
            .unwrap()
            .compile(&CompileContext::default())
            .unwrap();
        let valid: &[u8] = match *mode {
            "json-array" => b"[{\"id\":1}]",
            "json-ndjson" => b"{\"id\":1}\n",
            "json-body" => b"{\"items\":[{\"id\":1}]}",
            "xml-ordinary" => b"<row><id>1</id></row>",
            _ => b"<Root><items><row><id>1</id></row></items><manifest><batch>7</batch></manifest></Root>",
        };
        let expected: &[u8] = if *mode == "xml-prescan" {
            b"{\"id\":1,\"batch\":7}\n"
        } else {
            b"{\"id\":1}\n"
        };
        for variant in ["unsupported-second", "malformed-second", "malformed-late"] {
            let late = variant == "malformed-late";
            let padding = " ".repeat(32 * 1024);
            let mut corrupt = match *mode {
                "json-array" => format!("[{{\"id\":1}},{padding}{{\"id\":2}}]").into_bytes(),
                "json-ndjson" => format!("{{\"id\":1}}\n{padding}{{\"id\":2}}\n").into_bytes(),
                "json-body" => format!("{{\"items\":[{{\"id\":1}},{padding}{{\"id\":2}}]}}").into_bytes(),
                "xml-ordinary" => format!("<row><id>1</id>{padding}<tail>2</tail></row>").into_bytes(),
                _ => format!("<Root><manifest><batch>7</batch></manifest><items><row><id>1</id></row>{padding}<row><id>2</id></row></items></Root>").into_bytes(),
            };
            let offset = corrupt.iter().rposition(|byte| *byte == b'2').unwrap();
            corrupt[offset] = 0xff;
            let invalid: &[u8] = if variant == "unsupported-second" {
                b"\xff\xfe\0\0"
            } else {
                b"\xef\xbb\xbf\xff"
            };
            let inputs = if late {
                vec![corrupt.as_slice()]
            } else {
                vec![valid, invalid]
            };
            let expected = if late && matches!(*mode, "xml-ordinary" | "xml-prescan") {
                b"".as_slice()
            } else {
                expected
            };
            let count = u64::from(!expected.is_empty());
            let root = tempfile::tempdir().unwrap();
            let mut files = Vec::new();
            for (index, bytes) in inputs.iter().enumerate() {
                let path = root.path().join(format!("input-{index}.{format}"));
                std::fs::write(&path, bytes).unwrap();
                files.push(FileSlot::new(
                    path.clone(),
                    Box::new(std::fs::File::open(path).unwrap()),
                ));
            }
            let path = root.path().join("output.json");
            let writers = WriterRegistry {
                single: [(
                    "out".into(),
                    Box::new(std::fs::File::create(&path).unwrap()) as Box<dyn Write + Send>,
                )]
                .into(),
                ..Default::default()
            };
            let progress = RunProgress::new();
            let (producer, receiver) = TelemetryArena::reserve(&policy).unwrap();
            let params = PipelineRunParams {
                progress: Some(progress.clone()),
                telemetry_producer: Some(producer),
                ..Default::default()
            };
            let error = PipelineExecutor::run_plan_with_readers_writers(
                &plan,
                [("rows".into(), SourceInput::Files(files))].into(),
                writers,
                &params,
            )
            .unwrap_err();
            assert!(
                matches!(error, clinker_plan::PipelineError::Format(_)),
                "{mode}: {error:?}"
            );
            assert_eq!(progress.sample().records_read, count, "{mode}/{variant}");
            assert_eq!(std::fs::read(&path).unwrap(), expected, "{mode}");
            let mut records = 0;
            let mut bytes = 0;
            let mut source_failed = 0;
            while let Some(batch) = receiver.try_recv_batch() {
                records += batch.metric(MetricKey::SinkRecords);
                bytes += batch.metric(MetricKey::SinkBytes);
                source_failed += batch.metric(MetricKey::SourceFailed);
                assert_eq!(batch.metric(MetricKey::SinkErrors), 0, "{mode}");
            }
            assert_eq!(
                (records, bytes, source_failed),
                (count, expected.len() as u64, 1),
                "{mode}"
            );
        }
        assert!(executed.insert(*mode));
    }
    assert_eq!(executed, MODES.iter().copied().collect());
}

#[test]
fn physical_failed_files_preserve_source_population_sink_bytes_and_lifecycle() {
    use clinker_exec::progress::RunProgress;
    use clinker_exec::telemetry::{MetricKey, TelemetryArena};
    use clinker_plan::config::ClinkerToml;
    let policy = ClinkerToml::parse(
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
    .unwrap()
    .resolve_observability(None)
    .unwrap();
    for format in ["fixed_width", "swift"] {
        let schema = if format == "fixed_width" {
            "[{ name: number, type: int, start: 0, width: 2 }]"
        } else {
            "[{ name: block, type: string }, { name: tag, type: string }, { name: value, type: string }]"
        };
        let yaml = format!(
            r#"pipeline: {{ name: physical_failure_counts }}
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: {format}
      path: input.dat
      schema: {schema}
  - type: sink
    name: out
    input: rows
    config:
      name: out
      type: json
      path: output.json
      options: {{ format: ndjson }}
"#
        );
        let plan = parse_config(&yaml)
            .unwrap()
            .compile(&CompileContext::default())
            .unwrap();
        let valid: &[u8] = if format == "fixed_width" {
            b"01\n"
        } else {
            b"{4:\n:20:one\n-}"
        };
        let first: &[u8] = if format == "fixed_width" {
            b"{\"number\":1}\n"
        } else {
            b"{\"block\":\"4\",\"tag\":\"20\",\"value\":\"one\"}\n"
        };
        let continuing =
            parse_config(&yaml.replace("nodes:", "error_handling:\n  strategy: continue\nnodes:"))
                .unwrap()
                .compile(&CompileContext::default())
                .unwrap();
        for variant in [
            "first",
            "second",
            "late",
            "unsupported-second",
            "numeric-continue",
        ] {
            if variant == "numeric-continue" && format == "swift" {
                continue;
            }
            let invalid: &[u8] = if variant == "numeric-continue" {
                b"XX\n03\n"
            } else if variant == "unsupported-second" {
                b"\xff\xfe\0\0"
            } else if format == "fixed_width" {
                b"\xff1\n"
            } else {
                b"{4:\n:20:\xff\n-}"
            };
            let inputs = match variant {
                "second" | "unsupported-second" => vec![valid.to_vec(), invalid.to_vec()],
                "numeric-continue" => vec![[valid, invalid].concat()],
                "late" if format == "fixed_width" => vec![[valid, invalid].concat()],
                "late" => vec![
                    [
                        b"{4:\n:20:one\n:21:".as_slice(),
                        &vec![b'x'; 32768],
                        b"\xff\n-}",
                    ]
                    .concat(),
                ],
                _ => vec![invalid.to_vec()],
            };
            let expected = if variant.ends_with("second")
                || variant == "numeric-continue"
                || (variant == "late" && format == "fixed_width")
            {
                first
            } else {
                b""
            };
            let count = u64::from(!expected.is_empty());
            let root = tempfile::tempdir().unwrap();
            let mut files = Vec::new();
            for (index, bytes) in inputs.iter().enumerate() {
                let path = root.path().join(format!("input-{index}.dat"));
                std::fs::write(&path, bytes).unwrap();
                files.push(FileSlot::new(
                    path.clone(),
                    Box::new(std::fs::File::open(path).unwrap()),
                ));
            }
            let output = root.path().join("output.json");
            let writers = WriterRegistry {
                single: [(
                    "out".into(),
                    Box::new(std::fs::File::create(&output).unwrap()) as Box<dyn Write + Send>,
                )]
                .into(),
                ..Default::default()
            };
            let progress = RunProgress::new();
            let (producer, receiver) = TelemetryArena::reserve(&policy).unwrap();
            let params = PipelineRunParams {
                progress: Some(progress.clone()),
                telemetry_producer: Some(producer),
                ..Default::default()
            };
            let error = PipelineExecutor::run_plan_with_readers_writers(
                if variant == "numeric-continue" {
                    &continuing
                } else {
                    &plan
                },
                [("rows".into(), SourceInput::Files(files))].into(),
                writers,
                &params,
            )
            .unwrap_err();
            assert!(
                matches!(error, clinker_plan::PipelineError::Format(_)),
                "{format}/{variant}: {error:?}"
            );
            assert_eq!(progress.sample().records_read, count, "{format}/{variant}");
            assert_eq!(
                std::fs::read(&output).unwrap(),
                expected,
                "{format}/{variant}"
            );
            let (mut records, mut bytes, mut started, mut failed, mut completed) = (0, 0, 0, 0, 0);
            while let Some(batch) = receiver.try_recv_batch() {
                records += batch.metric(MetricKey::SinkRecords);
                bytes += batch.metric(MetricKey::SinkBytes);
                started += batch.metric(MetricKey::SourceStarted);
                failed += batch.metric(MetricKey::SourceFailed);
                completed += batch.metric(MetricKey::SourceCompleted);
                assert_eq!(batch.metric(MetricKey::SinkErrors), 0);
                assert_eq!(batch.metric(MetricKey::SourceInterrupted), 0);
            }
            assert_eq!(
                (records, bytes, started, failed, completed),
                (count, expected.len() as u64, 1, 1, 0),
                "{format}/{variant}"
            );
        }
    }
}

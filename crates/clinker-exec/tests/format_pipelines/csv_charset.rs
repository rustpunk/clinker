//! End-to-end CSV `encoding` handling in the shared format-pipeline harness.
//!
//! Single-schema and multi-record sources decode declared charsets through a
//! compiled pipeline, including document sections and strict invalid input.

use std::collections::HashMap;
use std::io::Cursor;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{PipelineExecutor, PipelineRunParams};
use clinker_plan::config::{CompileContext, parse_config};

/// Drive a CSV pipeline over raw input bytes, returning the output bytes (so a
/// non-UTF-8 input is fed faithfully) or a stringified run/compile error.
fn run_csv(yaml: &str, source_name: &str, out_name: &str, input: &[u8]) -> Result<Vec<u8>, String> {
    let config = parse_config(yaml).map_err(|e| format!("parse: {e:?}"))?;
    let plan = config
        .compile(&CompileContext::default())
        .map_err(|e| format!("compile: {e:?}"))?;

    let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
        source_name.to_string(),
        clinker_exec::executor::single_file_reader("in.csv", Box::new(Cursor::new(input.to_vec()))),
    )]);

    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        out_name.to_string(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);

    let params = PipelineRunParams {
        execution_id: "e".to_string(),
        batch_id: "b".to_string(),
        ..Default::default()
    };

    PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
        .map_err(|e| format!("run: {e:?}"))?;
    Ok(buf.contents())
}

/// A single-schema CSV whose one body field carries the Latin-1 high byte for
/// `é`. The bytes are not valid UTF-8, so the run only succeeds when the
/// declared `iso-8859-1` charset is actually applied.
fn latin1_input() -> Vec<u8> {
    let mut bytes = b"name\nCaf".to_vec();
    bytes.push(0xE9);
    bytes.push(b'\n');
    bytes
}

#[test]
fn csv_latin1_source_decodes_high_bytes_end_to_end() {
    let yaml = r#"
pipeline:
  name: csv_latin1
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      options:
        encoding: iso-8859-1
      schema:
        - { name: name, type: string }
  - type: sink
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let out = run_csv(yaml, "src", "out", &latin1_input()).expect("latin1 CSV run");
    let text = String::from_utf8(out).expect("CSV output is UTF-8 after decode");
    assert!(
        text.contains("Café"),
        "declared iso-8859-1 high byte must decode to 'é': {text}"
    );
}

#[test]
fn csv_unsupported_encoding_fails_at_startup() {
    let yaml = r#"
pipeline:
  name: csv_bad_encoding
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      options:
        encoding: shift_jis
      schema:
        - { name: name, type: string }
  - type: sink
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let err = run_csv(yaml, "src", "out", b"name\nAlice\n")
        .expect_err("an unsupported CSV encoding must fail the run");
    assert!(
        err.contains("shift_jis") && err.contains("iso-8859-1"),
        "error should name the unsupported charset and the supported set: {err}"
    );
}

fn multi_record_pipeline(encoding: &str, has_header: bool) -> String {
    format!(
        r#"
pipeline:
  name: csv_document_charset
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      options:
        encoding: {encoding}
        has_header: {has_header}
      schema:
        discriminator: {{ field: marker }}
        records:
          - id: metadata
            tag: Hé
            columns:
              - {{ name: marker, type: string }}
              - {{ name: batch_id, type: string }}
          - id: detail
            tag: Dé
            columns:
              - {{ name: marker, type: string }}
              - {{ name: label, type: string }}
      envelope:
        sections:
          manifest:
            extract: {{ record_type: Hé }}
            fields:
              batch_id: string
  - type: transform
    name: attach
    input: src
    config:
      cxl: |
        emit batch = $doc.manifest.batch_id
  - type: sink
    name: out
    input: attach
    config:
      name: out
      type: csv
      path: out.csv
"#
    )
}

const MULTI_RECORD_OUTPUT: &[u8] =
    "record_type,marker,batch_id,label,batch\ndetail,Dé,,Crème,Café\n".as_bytes();

#[test]
fn multi_record_csv_ingest_tracer() {
    let out = run_csv(
        &multi_record_pipeline("iso-8859-1", false),
        "src",
        "out",
        b"H\xe9,Caf\xe9\nD\xe9,Cr\xe8me\n",
    )
    .expect("Latin-1 document pipeline executes");
    assert_eq!(out, MULTI_RECORD_OUTPUT);
}

#[test]
fn multi_record_csv_charset_and_header_modes_have_exact_output() {
    for (encoding, body, header) in [
        (
            "utf-8",
            "Hé,Café\nDé,Crème\n".as_bytes(),
            "márker,label\n".as_bytes(),
        ),
        (
            "iso-8859-1",
            b"H\xe9,Caf\xe9\nD\xe9,Cr\xe8me\n".as_slice(),
            b"m\xe1rker,label\n".as_slice(),
        ),
    ] {
        for has_header in [false, true] {
            let mut input = Vec::new();
            if has_header {
                input.extend_from_slice(header);
            }
            input.extend_from_slice(body);
            let yaml = multi_record_pipeline(encoding, has_header);
            assert_eq!(
                run_csv(&yaml, "src", "out", &input).unwrap(),
                MULTI_RECORD_OUTPUT,
                "{encoding}, has_header={has_header}"
            );
            if encoding == "utf-8" {
                let mut with_bom = b"\xef\xbb\xbf".to_vec();
                with_bom.extend_from_slice(&input);
                assert_eq!(
                    run_csv(&yaml, "src", "out", &with_bom).unwrap(),
                    MULTI_RECORD_OUTPUT,
                    "UTF-8 BOM, has_header={has_header}"
                );
            }
        }
    }
}

#[test]
fn multi_record_csv_malformed_utf8_is_not_a_structural_rejection() {
    for (has_header, input) in [
        (
            true,
            b"marker,lab\xffel\nH\xc3\xa9,Cafe\nD\xc3\xa9,ok\n".as_slice(),
        ),
        (false, b"H\xc3\xa9,Caf\xff\nD\xc3\xa9,ok\n".as_slice()),
        (false, b"H\xc3\xa9,Cafe\nD\xc3\xa9,bad\xff\n".as_slice()),
    ] {
        let error = run_csv(
            &multi_record_pipeline("utf-8", has_header),
            "src",
            "out",
            input,
        )
        .expect_err("invalid UTF-8 must fail even in skipped or captured rows");
        assert!(error.contains("UTF-8"), "{error}");
        assert!(!error.contains("Structural"), "{error}");
    }
}

#[test]
fn multi_record_csv_unsupported_encoding_remains_rejected() {
    let error = run_csv(
        &multi_record_pipeline("shift_jis", false),
        "src",
        "out",
        b"H,x\nD,y\n",
    )
    .expect_err("unsupported encoding");
    assert!(
        error.contains("shift_jis") && error.contains("iso-8859-1"),
        "{error}"
    );
}

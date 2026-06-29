//! End-to-end CSV `encoding` handling.
//!
//! Covers the observable behaviors of a declared CSV source `encoding`:
//! (1) a single-schema source declaring `iso-8859-1` decodes high bytes
//! correctly through to output; (2) an unsupported encoding fails the run at
//! startup with a precise error naming it and the supported set; (3) a
//! multi-record CSV source (a `records:` schema) rejects a non-UTF-8 encoding
//! at startup rather than silently dropping it (the multi-record backend is
//! UTF-8-only).

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
  - type: output
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
  - type: output
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

#[test]
fn csv_multi_record_rejects_non_utf8_encoding_at_startup() {
    // A multi-record CSV source (a `records:` schema) is decoded by the
    // UTF-8-only multi-record backend, so a declared non-UTF-8 `encoding` is
    // rejected at startup instead of being silently dropped.
    let yaml = r#"
pipeline:
  name: csv_mr_latin1
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
        - { name: record_type, type: string }
        - { name: batch_id, type: string }
      format_schema:
        discriminator: { field: record_type }
        records:
          - { id: header, tag: H, fields: [ { name: record_type }, { name: batch_id } ] }
          - { id: detail, tag: D, fields: [ { name: record_type }, { name: batch_id } ] }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let err = run_csv(yaml, "src", "out", b"H,x\nD,y\n")
        .expect_err("multi-record CSV must reject a non-UTF-8 encoding");
    assert!(
        err.contains("multi-record") && err.contains("utf-8"),
        "error should explain the multi-record CSV limitation: {err}"
    );
}

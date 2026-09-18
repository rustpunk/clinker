//! Production-path proof for CXL native nested construction: pipeline YAML is
//! parsed and compiled, a real transform evaluates maps/arrays/comprehensions,
//! and both recursive writers receive the resulting neutral value.

use crate::common;

use std::collections::HashMap;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{PipelineRunParams, SourceReaders};

const PIPELINE: &str = r##"
pipeline:
  name: nested_cxl_write
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      path: ./in.csv
      options:
        has_header: true
      schema:
        - { name: first, type: string }
        - { name: second, type: string }
  - type: transform
    name: construct
    input: rows
    config:
      cxl: |
        emit payload = {
          "@kind": "event",
          "#text": "before",
          item: [{"@id": item, "#text": item.to_string()} for item in [first.to_int(), second.to_int()] if item > 0],
          tail: "after"
        }
  - type: sink
    name: json_out
    input: construct
    config:
      name: json_out
      type: json
      path: ./out.json
      include_unmapped: false
      options:
        format: ndjson
  - type: sink
    name: xml_out
    input: construct
    config:
      name: xml_out
      type: xml
      path: ./out.xml
      include_unmapped: false
"##;

#[test]
fn cxl_nested_values_reach_json_and_xml_writers_exactly() {
    let config = clinker_plan::config::parse_config(PIPELINE).expect("pipeline parses");
    let readers: SourceReaders = HashMap::from([(
        "rows".to_string(),
        clinker_exec::executor::single_file_reader(
            "in.csv",
            Box::new(std::io::Cursor::new(b"first,second\n2,-1\n".to_vec())),
        ),
    )]);
    let json = SharedBuffer::new();
    let xml = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([
        (
            "json_out".to_string(),
            Box::new(json.clone()) as Box<dyn std::io::Write + Send>,
        ),
        (
            "xml_out".to_string(),
            Box::new(xml.clone()) as Box<dyn std::io::Write + Send>,
        ),
    ]);
    let params = PipelineRunParams {
        execution_id: "nested-cxl-e2e".into(),
        batch_id: "batch-1".into(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    };

    common::run_config(&config, readers, writers, &params).expect("pipeline runs");

    assert_eq!(
        json.as_string(),
        "{\"payload\":{\"@kind\":\"event\",\"#text\":\"before\",\"item\":[{\"@id\":2,\"#text\":\"2\"}],\"tail\":\"after\"}}\n"
    );
    assert_eq!(
        xml.as_string(),
        "<Root><Record><payload kind=\"event\">before<item id=\"2\">2</item><tail>after</tail></payload></Record></Root>"
    );
}

#[test]
fn nested_modes_plain_correlated_split_and_fanout_keep_literal_documents() {
    use clinker_exec::executor::{PipelineExecutor, WriterRegistry};
    use clinker_exec::source::{SourceInput, multi_file::FileSlot};
    use clinker_plan::CompileContext;
    use std::io::{Cursor, Write};
    use std::sync::Arc;

    fn document(format: &str, ids: &[usize]) -> String {
        // Independent literal records, including column order and absence of
        // engine-stamped correlation columns. Only document framing is joined.
        const JSON: [&str; 4] = [r#"{"id":1,"group":"a"}"#, r#"{"id":2,"group":"b"}"#, r#"{"id":3,"group":"a"}"#, r#"{"id":4,"group":"b"}"#];
        const XML: [&str; 4] = ["<Record><id>1</id><group>a</group></Record>", "<Record><id>2</id><group>b</group></Record>", "<Record><id>3</id><group>a</group></Record>", "<Record><id>4</id><group>b</group></Record>"];
        if format == "json" { format!("[\n{}\n]\n", ids.iter().map(|id| JSON[id - 1]).collect::<Vec<_>>().join(",\n")) }
        else { format!("<Root>{}</Root>", ids.iter().map(|id| XML[id - 1]).collect::<String>()) }
    }
    for format in ["json", "xml"] {
        for mode in ["plain", "correlation", "record-split", "byte-split", "fanout"] {
            let root = tempfile::tempdir().unwrap();
            let output_path = root.path().join(if mode == "fanout" { format!("out_{{source_file}}.{format}") } else { format!("out.{format}") });
            let quoted_path = serde_json::to_string(&output_path.to_string_lossy()).unwrap();
            let correlation = if mode == "correlation" { "      correlation_key: group\n" } else { "" };
            let split = match mode { "record-split" => "      split: { max_records: 2 }\n", "byte-split" if format == "json" => "      split: { max_bytes: 32 }\n", "byte-split" => "      split: { max_bytes: 70 }\n", _ => "" };
            let yaml = format!(r#"
pipeline:
  name: native_routing
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      glob: ./*.csv
      files: {{ on_no_match: skip }}
{correlation}      schema:
        - {{ name: id, type: int }}
        - {{ name: group, type: string }}
  - type: sink
    name: out
    input: rows
    config:
      name: out
      type: {format}
      path: {quoted_path}
      sort_order: [id]
{split}"#);
            let config = clinker_plan::config::parse_config(&yaml).unwrap();
            let mut context = CompileContext::new(root.path());
            context.allow_absolute_paths = true;
            let plan = config.compile(&context).unwrap_or_else(|e| panic!("{format}/{mode}: {e:?}"));
            let readers = [("rows".to_owned(), SourceInput::Files(vec![
                FileSlot::new("a.csv", Box::new(Cursor::new(b"id,group\n2,b\n1,a\n".to_vec()))),
                FileSlot::new("b.csv", Box::new(Cursor::new(b"id,group\n4,b\n3,a\n".to_vec()))),
            ]))].into();
            let outputs = [SharedBuffer::new(), SharedBuffer::new()];
            let registry = if mode == "fanout" {
                WriterRegistry { fan_out: [("out".to_owned(), [(Arc::<str>::from("a.csv"), Box::new(outputs[0].clone()) as Box<dyn Write + Send>), (Arc::<str>::from("b.csv"), Box::new(outputs[1].clone()) as Box<dyn Write + Send>)].into())].into(), ..Default::default() }
            } else { WriterRegistry { single: [("out".into(), Box::new(outputs[0].clone()) as Box<dyn Write + Send>)].into(), ..Default::default() } };
            let report = PipelineExecutor::run_plan_with_readers_writers_in_context(&plan, readers, registry, &PipelineRunParams::default(), context).unwrap_or_else(|e| panic!("{format}/{mode}: {e:?}"));
            assert_eq!(report.counters.records_written, 4, "{format}/{mode}");
            assert_eq!(report.counters.dlq_count, 0, "{format}/{mode}");
            assert_eq!(report.per_source_record_counts["rows"], 4, "{format}/{mode}");
            let actual = if mode.ends_with("split") {
                let mut paths: Vec<_> = std::fs::read_dir(root.path()).unwrap().map(|entry| entry.unwrap().path()).filter(|path| path.extension().is_some_and(|ext| ext == format)).collect();
                paths.sort();
                paths.into_iter().map(|path| std::fs::read_to_string(path).unwrap()).collect::<Vec<_>>()
            } else if mode == "fanout" { outputs.iter().map(SharedBuffer::as_string).collect() }
            else { vec![outputs[0].as_string()] };
            let expected = if matches!(mode, "plain" | "correlation") { vec![document(format, &[1,2,3,4])] } else { vec![document(format, &[1,2]), document(format, &[3,4])] };
            assert_eq!(actual, expected, "{format}/{mode}");
        }
    }
}

#[test]
fn nested_modes_envelope_rejects_conflicting_routing_before_execution() {
    for format in ["json", "xml"] {
        for mode in ["split", "fanout", "document-dlq", "correlation"] {
            let source = match mode { "document-dlq" => "      dlq_granularity: document\n", "correlation" => "      correlation_key: id\n", _ => "" };
            let path = if mode == "fanout" { "out_{source_file}" } else { "output" };
            let split = if mode == "split" { "      split: { max_records: 2 }\n" } else { "" };
            let yaml = format!(r#"
pipeline:
  name: incompatible_native_envelope
error_handling:
  strategy: continue
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: json
      glob: ./*.json
      files: {{ on_no_match: skip }}
{source}      schema: [{{ name: id, type: int }}]
  - type: sink
    name: out
    input: rows
    config:
      name: out
      type: {format}
      path: {path}.{format}
      reconstruct_envelope: true
{split}      options:
        envelope:
          footer_record_count_field: rows
"#);
            let error = clinker_plan::config::parse_config(&yaml).expect_err("incompatible framing cannot reach execution");
            let message = error.to_string();
            assert!(message.contains("E347"), "{format}/{mode}: {message}");
            assert!(message.contains("out") && message.contains("reconstruct_envelope"), "{message}");
        }
    }
}

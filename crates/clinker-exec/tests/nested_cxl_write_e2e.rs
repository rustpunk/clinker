//! Production-path proof for CXL native nested construction: pipeline YAML is
//! parsed and compiled, a real transform evaluates maps/arrays/comprehensions,
//! and both recursive writers receive the resulting neutral value.

mod common;

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
  - type: output
    name: json_out
    input: construct
    config:
      name: json_out
      type: json
      path: ./out.json
      include_unmapped: false
      options:
        format: ndjson
  - type: output
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
        "{\"payload\":{\"@kind\":\"event\",\"#text\":\"before\",\"item\":[{\"@id\":2,\"#text\":\"2\"}],\"tail\":\"after\"}}"
    );
    assert_eq!(
        xml.as_string(),
        "<Root><Record><payload kind=\"event\">before<item id=\"2\">2</item><tail>after</tail></payload></Record></Root>"
    );
}

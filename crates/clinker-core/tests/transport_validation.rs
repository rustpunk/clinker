//! Config-time validation of the transport-vs-format YAML split.
//!
//! `transport:` selects WHERE records come from; `type:` (the format)
//! selects how the bytes decode. A file transport (the default) must
//! declare exactly one file matcher (`path`/`glob`/`regex`/`paths`), and
//! the rule is enforced at parse/validate time so a misconfigured source
//! fails before discovery runs.

use clinker_core::config::parse_config;

fn pipeline_with_source(source_body: &str) -> String {
    format!(
        r#"
pipeline:
  name: transport_validation
nodes:
  - type: source
    name: src
    config:
      name: src
{source_body}
      schema:
        - {{ name: id, type: int }}
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
"#
    )
}

#[test]
fn explicit_file_transport_with_one_matcher_parses() {
    let yaml =
        pipeline_with_source("      transport: file\n      type: csv\n      path: ./data/in.csv");
    let config = parse_config(&yaml).expect("explicit file transport with one matcher must parse");
    let src = config.source_configs().next().unwrap();
    assert!(matches!(
        src.transport,
        clinker_core::config::SourceTransport::File
    ));
    assert_eq!(src.path.as_deref(), Some("./data/in.csv"));
}

#[test]
fn omitted_transport_defaults_to_file_and_parses_byte_identically() {
    // No `transport:` key — defaults to File. Byte-identical to every
    // pre-existing file pipeline.
    let with =
        pipeline_with_source("      transport: file\n      type: csv\n      path: ./data/in.csv");
    let without = pipeline_with_source("      type: csv\n      path: ./data/in.csv");

    let c_with = parse_config(&with).unwrap();
    let c_without = parse_config(&without).unwrap();

    let s_with = c_with.source_configs().next().unwrap();
    let s_without = c_without.source_configs().next().unwrap();
    assert_eq!(s_with.transport, s_without.transport);
    assert!(matches!(
        s_without.transport,
        clinker_core::config::SourceTransport::File
    ));
}

#[test]
fn file_transport_with_no_matcher_is_rejected_at_config_time() {
    let yaml = pipeline_with_source("      type: csv");
    let err = parse_config(&yaml).expect_err("file source with no matcher must fail validation");
    let msg = err.to_string();
    assert!(msg.contains("E211"), "expected E211 diagnostic, got: {msg}");
    assert!(msg.contains("no matcher"), "got: {msg}");
}

#[test]
fn file_transport_with_two_matchers_is_rejected_at_config_time() {
    let yaml =
        pipeline_with_source("      type: csv\n      path: ./a.csv\n      glob: \"./*.csv\"");
    let err = parse_config(&yaml).expect_err("file source with two matchers must fail validation");
    let msg = err.to_string();
    assert!(msg.contains("E210"), "expected E210 diagnostic, got: {msg}");
    assert!(msg.contains("path"), "got: {msg}");
    assert!(msg.contains("glob"), "got: {msg}");
}

#[test]
fn rest_transport_parses_with_nested_kind_and_pagination() {
    let body = "      type: json\n      options:\n        format: array\n      transport:\n        kind: rest\n        url: https://api.example.com/v1/rows\n        max_pages: 25\n        pagination:\n          strategy: link_header";
    let config = parse_config(&pipeline_with_source(body)).expect("rest transport must parse");
    let src = config.source_configs().next().unwrap();
    match &src.transport {
        clinker_core::config::SourceTransport::Rest(c) => {
            assert_eq!(c.url, "https://api.example.com/v1/rows");
            assert_eq!(c.max_pages, 25);
            assert!(matches!(
                c.pagination,
                clinker_core::config::RestPagination::LinkHeader
            ));
        }
        other => panic!("expected rest transport, got {other:?}"),
    }
}

#[test]
fn network_transport_with_file_matcher_is_rejected() {
    // A `path:` on a rest transport is dead config — the source reads from
    // its endpoint, not the filesystem.
    let body = "      type: json\n      path: ./oops.json\n      transport:\n        kind: rest\n        url: https://api.example.com/r\n        max_pages: 5";
    let err = parse_config(&pipeline_with_source(body))
        .expect_err("network transport with file matcher must fail");
    let msg = err.to_string();
    assert!(msg.contains("E219"), "expected E219 diagnostic, got: {msg}");
}

#[test]
fn rest_transport_with_csv_format_is_rejected() {
    // REST decodes response bodies through the declared format, which must
    // be json or xml.
    let body = "      type: csv\n      transport:\n        kind: rest\n        url: https://api.example.com/r\n        max_pages: 5";
    let err = parse_config(&pipeline_with_source(body))
        .expect_err("rest transport with csv format must fail");
    let msg = err.to_string();
    assert!(msg.contains("E220"), "expected E220 diagnostic, got: {msg}");
}

#[test]
fn unknown_scalar_transport_is_rejected() {
    // The bare-scalar transport form accepts only `file`; an unknown
    // scalar (or a database name that is not a built transport) fails to
    // deserialize with a message naming the rest nested form.
    let body = "      type: json\n      path: ./oops.json\n      transport: postgres\n      options:\n        format: array";
    let err = parse_config(&pipeline_with_source(body))
        .expect_err("unknown scalar transport must fail to parse");
    let msg = err.to_string();
    assert!(
        msg.contains("unknown transport") || msg.contains("postgres"),
        "expected unknown-transport diagnostic, got: {msg}"
    );
}

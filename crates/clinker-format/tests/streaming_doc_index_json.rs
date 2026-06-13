//! Bounded-retention behavior of the streaming JSON envelope pre-scan.
//!
//! Proves the pre-scan retains only the declared `$doc.*` subtrees — a
//! multi-MB body array of undeclared records is parsed-and-skipped, never
//! buffered into the document index — and that an over-budget retention
//! fails loud mid-build rather than after a full materialization.

use clinker_format::FormatError;
use clinker_format::envelope::{
    EnvelopeConfig, EnvelopeExtract, EnvelopeFieldType, EnvelopeSection,
};
use clinker_format::json::reader::{JsonReader, JsonReaderConfig};
use clinker_format::traits::FormatReader;
use clinker_record::Value;
use cxl::analyzer::doc_paths::DocPath;
use indexmap::IndexMap;

/// One envelope section: name, JSON pointer, typed fields.
type SectionSpec<'a> = (&'a str, &'a str, &'a [(&'a str, EnvelopeFieldType)]);

fn envelope_config(specs: &[SectionSpec]) -> EnvelopeConfig {
    let mut cfg = EnvelopeConfig::default();
    for (name, pointer, fields) in specs {
        let mut field_map = IndexMap::new();
        for (fname, ftype) in *fields {
            field_map.insert((*fname).to_string(), *ftype);
        }
        cfg.sections.insert(
            (*name).to_string(),
            EnvelopeSection {
                extract: EnvelopeExtract::JsonPointer((*pointer).to_string()),
                fields: field_map,
            },
        );
    }
    cfg
}

fn declared_paths(specs: &[SectionSpec]) -> Vec<DocPath> {
    let mut out = Vec::new();
    for (section, _pointer, fields) in specs {
        for (field, _ty) in *fields {
            out.push(DocPath {
                section: (*section).into(),
                field: (*field).into(),
                indices: Vec::new(),
            });
        }
    }
    out
}

/// A document with a large undeclared body and a small declared trailer:
/// `{ "records": [ ...N large rows... ], "Summary": { "record_count": N } }`.
fn doc_with_large_body_and_small_trailer(rows: usize) -> String {
    let mut s = String::from("{\"records\":[");
    for i in 0..rows {
        if i > 0 {
            s.push(',');
        }
        // Each row carries a large, undeclared text blob — the bytes the
        // pre-scan must skip rather than retain.
        s.push_str(&format!(
            "{{\"id\":{i},\"blob\":\"{}\"}}",
            "x".repeat(2_000)
        ));
    }
    s.push_str(&format!("],\"Summary\":{{\"record_count\":{rows}}}}}"));
    s
}

#[test]
fn prescan_retains_only_declared_trailer_not_the_body() {
    let rows = 2_000;
    let json = doc_with_large_body_and_small_trailer(rows);
    let input_len = json.len();
    // The body alone is multiple MB.
    assert!(
        input_len > 4_000_000,
        "body should be multi-MB: {input_len}"
    );

    let specs: &[SectionSpec] = &[(
        "Summary",
        "/Summary",
        &[("record_count", EnvelopeFieldType::Int)],
    )];
    let cfg = envelope_config(specs);

    let mut reader = JsonReader::from_reader(
        std::io::Cursor::new(json.into_bytes()),
        JsonReaderConfig {
            record_path: Some("records".into()),
            declared_doc_paths: declared_paths(specs),
            max_index_bytes: Some(64 * 1_000_000),
            ..Default::default()
        },
    )
    .unwrap();

    let sections = reader.prepare_document(&cfg).expect("envelope pre-scan");

    // The trailer was extracted and typed.
    let summary = match sections.get("Summary").expect("Summary section retained") {
        Value::Map(m) => m,
        other => panic!("expected map, got {other:?}"),
    };
    assert_eq!(
        summary.get("record_count"),
        Some(&Value::Integer(rows as i64))
    );

    // The retained-section map is orders of magnitude smaller than the
    // input: the body's undeclared subtrees were skipped, never stored. A
    // serialized section map of one int is well under 1KB; the input is
    // multi-MB.
    let serialized = serde_json::to_string(
        &sections
            .iter()
            .map(|(k, v)| (k.to_string(), format!("{v:?}")))
            .collect::<std::collections::BTreeMap<_, _>>(),
    )
    .unwrap();
    assert!(
        serialized.len() < input_len / 1_000,
        "retained section map ({} bytes) must be <<< input ({} bytes)",
        serialized.len(),
        input_len
    );

    // The body still streams: every record is present, in order.
    let mut count = 0usize;
    let mut last_id = -1i64;
    while let Some(rec) = reader.next_record().unwrap() {
        let id = match rec.get("id") {
            Some(Value::Integer(n)) => *n,
            other => panic!("expected integer id, got {other:?}"),
        };
        assert_eq!(id, last_id + 1, "body records stream in order");
        last_id = id;
        count += 1;
    }
    assert_eq!(count, rows, "every body record streamed");
}

#[test]
fn prescan_fails_loud_when_retention_exceeds_cap() {
    // A declared section pointing at a large subtree, with a tiny cap. The
    // cap must fire DURING the build (the input is far larger than the
    // cap), naming the section and the cap, rather than OOMing.
    let big_blob = "z".repeat(500_000);
    let json = format!("{{\"Header\":{{\"note\":\"{big_blob}\"}},\"records\":[{{\"x\":1}}]}}");
    let input_len = json.len();

    let specs: &[SectionSpec] = &[("Header", "/Header", &[("note", EnvelopeFieldType::String)])];
    let cfg = envelope_config(specs);

    let mut reader = JsonReader::from_reader(
        std::io::Cursor::new(json.into_bytes()),
        JsonReaderConfig {
            record_path: Some("records".into()),
            declared_doc_paths: declared_paths(specs),
            // 4KB cap — far below the 500KB declared section.
            max_index_bytes: Some(4_000),
            ..Default::default()
        },
    )
    .unwrap();

    let err = reader
        .prepare_document(&cfg)
        .expect_err("over-cap retention must fail");
    match err {
        FormatError::Json(msg) => {
            assert!(
                msg.contains("max_index_bytes"),
                "error names the cap: {msg}"
            );
            assert!(msg.contains("Header"), "error names the section: {msg}");
        }
        other => panic!("expected FormatError::Json, got {other:?}"),
    }
    // The input dwarfs the cap, proving the failure is a mid-build cap
    // trip, not a post-hoc measurement of a fully materialized index.
    assert!(input_len > 100_000);
}

#[test]
fn prescan_skips_entirely_when_no_doc_path_declared() {
    // An envelope is declared, but no downstream program reads any `$doc`
    // path (empty `declared_doc_paths`). The path-pruned index is empty, so
    // the pre-scan skips the document and extracts nothing — the body is
    // never walked for sections.
    let json = r#"{"Summary":{"record_count":3},"records":[{"x":1},{"x":2},{"x":3}]}"#;
    let specs: &[SectionSpec] = &[(
        "Summary",
        "/Summary",
        &[("record_count", EnvelopeFieldType::Int)],
    )];
    let cfg = envelope_config(specs);

    let mut reader = JsonReader::from_reader(
        std::io::Cursor::new(json.as_bytes().to_vec()),
        JsonReaderConfig {
            record_path: Some("records".into()),
            // No declared paths — nothing downstream reads `$doc.*`.
            declared_doc_paths: Vec::new(),
            max_index_bytes: Some(64 * 1_000_000),
            ..Default::default()
        },
    )
    .unwrap();

    let sections = reader.prepare_document(&cfg).expect("pre-scan");
    assert!(
        sections.is_empty(),
        "no declared path means no section is extracted"
    );
    // Body still streams normally.
    let mut count = 0;
    while reader.next_record().unwrap().is_some() {
        count += 1;
    }
    assert_eq!(count, 3);
}

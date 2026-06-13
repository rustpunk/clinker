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

    // The retained bytes are orders of magnitude smaller than the input:
    // the body's undeclared subtrees were skipped, never stored. Measuring
    // the section map's own heap footprint (the real retained memory, not a
    // serialized proxy) makes the skip-not-just-correct-output guarantee
    // load-bearing — a single retained int sits far below input/1000.
    let retained: usize = sections.iter().map(|(k, v)| k.len() + v.heap_size()).sum();
    assert!(
        retained < input_len / 1_000,
        "retained section heap ({retained} bytes) must be <<< input ({input_len} bytes)"
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
fn prescan_fails_loud_mid_build_on_a_single_oversized_section() {
    // ONE declared section far larger than the cap. The cap must fire DURING
    // that section's deserialization — before the whole subtree
    // materializes — naming the section and the cap, rather than OOMing.
    // This is the single-section guarantee: the cap is not a post-hoc
    // measurement of an already-built section, it aborts the build.
    let huge_array_elems: String = (0..50_000)
        .map(|i| format!("\"row-{i}-{}\"", "y".repeat(40)))
        .collect::<Vec<_>>()
        .join(",");
    let json =
        format!("{{\"Header\":{{\"rows\":[{huge_array_elems}]}},\"records\":[{{\"x\":1}}]}}");
    let input_len = json.len();
    // The single declared section is multiple MB on its own.
    assert!(
        input_len > 2_000_000,
        "section should be multi-MB: {input_len}"
    );

    let specs: &[SectionSpec] = &[("Header", "/Header", &[("rows", EnvelopeFieldType::String)])];
    let cfg = envelope_config(specs);

    let mut reader = JsonReader::from_reader(
        std::io::Cursor::new(json.into_bytes()),
        JsonReaderConfig {
            record_path: Some("records".into()),
            declared_doc_paths: declared_paths(specs),
            // 8KB cap — far below the multi-MB declared section.
            max_index_bytes: Some(8_000),
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
            assert!(
                msg.contains("mid-parse"),
                "error states the abort is mid-parse: {msg}"
            );
        }
        other => panic!("expected FormatError::Json, got {other:?}"),
    }
}

#[test]
fn prescan_fails_loud_when_cumulative_sections_exceed_cap() {
    // Two declared sections that each fit, but whose cumulative retained
    // bytes cross the cap — proves the running total is charged across the
    // single streaming pass, not reset per section.
    let chunk = "q".repeat(3_000);
    let json = format!(
        "{{\"A\":{{\"v\":\"{chunk}\"}},\"B\":{{\"v\":\"{chunk}\"}},\"records\":[{{\"x\":1}}]}}"
    );
    let specs: &[SectionSpec] = &[
        ("A", "/A", &[("v", EnvelopeFieldType::String)]),
        ("B", "/B", &[("v", EnvelopeFieldType::String)]),
    ];
    let cfg = envelope_config(specs);

    let mut reader = JsonReader::from_reader(
        std::io::Cursor::new(json.into_bytes()),
        JsonReaderConfig {
            record_path: Some("records".into()),
            declared_doc_paths: declared_paths(specs),
            // 4KB cap: each 3KB section fits alone, but the pair (6KB) does not.
            max_index_bytes: Some(4_000),
            ..Default::default()
        },
    )
    .unwrap();

    let err = reader
        .prepare_document(&cfg)
        .expect_err("cumulative over-cap retention must fail");
    assert!(matches!(err, FormatError::Json(msg) if msg.contains("max_index_bytes")));
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

#[test]
fn prescan_skips_cleanly_for_top_level_array_document() {
    // The document is a top-level JSON array, but a `$doc` section is
    // declared. The section pointer cannot resolve through an array root, so
    // the pre-scan yields no section and does NOT abort the run — matching
    // `serde_json::Value::pointer` returning `None` for a non-object root.
    let json = r#"[{"x":1},{"x":2}]"#;
    let specs: &[SectionSpec] = &[(
        "Summary",
        "/Summary",
        &[("record_count", EnvelopeFieldType::Int)],
    )];
    let cfg = envelope_config(specs);

    let mut reader = JsonReader::from_reader(
        std::io::Cursor::new(json.as_bytes().to_vec()),
        JsonReaderConfig {
            declared_doc_paths: declared_paths(specs),
            max_index_bytes: Some(64 * 1_000_000),
            ..Default::default()
        },
    )
    .unwrap();

    let sections = reader
        .prepare_document(&cfg)
        .expect("a non-object root is a graceful miss, not an error");
    assert!(
        sections.is_empty(),
        "no section resolves through an array root"
    );

    // The top-level array still streams as the body.
    let mut count = 0;
    while reader.next_record().unwrap().is_some() {
        count += 1;
    }
    assert_eq!(count, 2);
}

#[test]
fn prescan_skips_cleanly_when_pointer_descends_through_non_object() {
    // The pointer `/meta/Summary` descends through `meta`, which is a scalar,
    // not an object. The intermediate cannot be descended, so the section is
    // a graceful miss — not a mid-stream hard error.
    let json = r#"{"meta": 42, "records":[{"x":1}]}"#;
    let specs: &[SectionSpec] = &[(
        "Summary",
        "/meta/Summary",
        &[("record_count", EnvelopeFieldType::Int)],
    )];
    let cfg = envelope_config(specs);

    let mut reader = JsonReader::from_reader(
        std::io::Cursor::new(json.as_bytes().to_vec()),
        JsonReaderConfig {
            record_path: Some("records".into()),
            declared_doc_paths: declared_paths(specs),
            max_index_bytes: Some(64 * 1_000_000),
            ..Default::default()
        },
    )
    .unwrap();

    let sections = reader
        .prepare_document(&cfg)
        .expect("a non-object intermediate is a graceful miss, not an error");
    assert!(
        sections.is_empty(),
        "no section resolves through a scalar intermediate"
    );
}

#[test]
fn prescan_resolves_array_index_pointer_segment() {
    // RFC 6901 array-index segment: `/data/0/Summary` selects element 0 of
    // the `data` array, then its `Summary` object. The old full-tree pointer
    // resolved this; the streaming walk must too (seq descent), not error.
    let json = r#"{"data":[{"Summary":{"record_count":7}},{"other":1}],"records":[{"x":1}]}"#;
    let specs: &[SectionSpec] = &[(
        "Summary",
        "/data/0/Summary",
        &[("record_count", EnvelopeFieldType::Int)],
    )];
    let cfg = envelope_config(specs);

    let mut reader = JsonReader::from_reader(
        std::io::Cursor::new(json.as_bytes().to_vec()),
        JsonReaderConfig {
            record_path: Some("records".into()),
            declared_doc_paths: declared_paths(specs),
            max_index_bytes: Some(64 * 1_000_000),
            ..Default::default()
        },
    )
    .unwrap();

    let sections = reader
        .prepare_document(&cfg)
        .expect("array-index pointer resolves");
    let summary = match sections
        .get("Summary")
        .expect("Summary resolved via /data/0")
    {
        Value::Map(m) => m,
        other => panic!("expected map, got {other:?}"),
    };
    assert_eq!(summary.get("record_count"), Some(&Value::Integer(7)));
}

#[test]
fn prescan_prunes_the_unread_section_in_a_mixed_envelope() {
    // The envelope declares two sections, but only one has a declared `$doc`
    // path. The read section is present; the unread one is pruned — absent
    // from the output, never materialized.
    let json = r#"{"Head":{"batch_id":"B-1"},"Foot":{"record_count":5},"records":[{"x":1}]}"#;
    let head_spec: SectionSpec = ("Head", "/Head", &[("batch_id", EnvelopeFieldType::String)]);
    let foot_spec: SectionSpec = ("Foot", "/Foot", &[("record_count", EnvelopeFieldType::Int)]);
    // Both sections are declared in the envelope config...
    let cfg = envelope_config(&[head_spec, foot_spec]);
    // ...but only `Foot` is referenced by a `$doc` path.
    let declared = declared_paths(&[foot_spec]);

    let mut reader = JsonReader::from_reader(
        std::io::Cursor::new(json.as_bytes().to_vec()),
        JsonReaderConfig {
            record_path: Some("records".into()),
            declared_doc_paths: declared,
            max_index_bytes: Some(64 * 1_000_000),
            ..Default::default()
        },
    )
    .unwrap();

    let sections = reader.prepare_document(&cfg).expect("pre-scan");
    assert!(
        sections.contains_key("Foot"),
        "the read section is retained"
    );
    assert!(
        !sections.contains_key("Head"),
        "the unread section is pruned, not materialized"
    );
}

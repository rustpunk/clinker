//! Bounded-retention and bounded-memory behavior of the streaming XML reader.
//!
//! Proves the envelope pre-scan retains only the declared `$doc.*` section
//! subtrees — a multi-MB body of undeclared records is event-walked and
//! dropped, never flattened into the document index — that an over-budget
//! retention fails loud mid-build rather than after a full materialization,
//! and that the body itself streams element-at-a-time from a re-opened
//! path-backed source with no whole-document buffer.

use clinker_format::FormatError;
use clinker_format::ReopenableSource;
use clinker_format::envelope::{
    EnvelopeConfig, EnvelopeExtract, EnvelopeFieldType, EnvelopeSection,
};
use clinker_format::traits::FormatReader;
use clinker_format::xml::reader::{XmlReader, XmlReaderConfig};
use clinker_record::Value;
use cxl::analyzer::doc_paths::DocPath;
use indexmap::IndexMap;
use std::io::Write;

/// One envelope section: name, slash-path, typed fields.
type SectionSpec<'a> = (&'a str, &'a str, &'a [(&'a str, EnvelopeFieldType)]);

fn envelope_config(specs: &[SectionSpec]) -> EnvelopeConfig {
    let mut cfg = EnvelopeConfig::default();
    for (name, xpath, fields) in specs {
        let mut field_map = IndexMap::new();
        for (fname, ftype) in *fields {
            field_map.insert((*fname).to_string(), *ftype);
        }
        cfg.sections.insert(
            (*name).to_string(),
            EnvelopeSection {
                extract: EnvelopeExtract::XmlPath((*xpath).to_string()),
                fields: field_map,
            },
        );
    }
    cfg
}

fn declared_paths(specs: &[SectionSpec]) -> Vec<DocPath> {
    let mut out = Vec::new();
    for (section, _xpath, fields) in specs {
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

fn reader(xml: String, specs: &[SectionSpec], record_path: &str, cap: usize) -> XmlReader {
    XmlReader::from_reader(
        std::io::Cursor::new(xml.into_bytes()),
        XmlReaderConfig {
            record_path: Some(record_path.into()),
            declared_doc_paths: declared_paths(specs),
            max_index_bytes: Some(cap),
            ..Default::default()
        },
    )
    .expect("XML buffer read")
}

/// A unique temp path under the OS temp dir, namespaced by pid + thread so
/// concurrent test threads never collide on a fixed name.
fn temp_path(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "clinker-{tag}-{}-{:?}.xml",
        std::process::id(),
        std::thread::current().id()
    ))
}

/// A document with a large undeclared body and a small declared trailer:
/// `<doc><records>...N large records...</records><Summary>...</Summary></doc>`.
fn doc_with_large_body_and_small_trailer(rows: usize) -> String {
    let mut s = String::from("<doc><records>");
    for i in 0..rows {
        // Each record carries a large, undeclared text blob — the bytes the
        // pre-scan must drop rather than retain.
        s.push_str(&format!(
            "<record><id>{i}</id><blob>{}</blob></record>",
            "x".repeat(2_000)
        ));
    }
    s.push_str(&format!(
        "</records><Summary><record_count>{rows}</record_count></Summary></doc>"
    ));
    s
}

fn unwrap_map(value: &Value) -> &IndexMap<Box<str>, Value> {
    match value {
        Value::Map(m) => m,
        other => panic!("expected map, got {other:?}"),
    }
}

#[test]
fn prescan_retains_only_declared_trailer_not_the_body() {
    let rows = 2_000;
    let xml = doc_with_large_body_and_small_trailer(rows);
    let input_len = xml.len();
    // The body alone is multiple MB.
    assert!(
        input_len > 4_000_000,
        "body should be multi-MB: {input_len}"
    );

    let specs: &[SectionSpec] = &[(
        "Summary",
        "/doc/Summary",
        &[("record_count", EnvelopeFieldType::Int)],
    )];
    let cfg = envelope_config(specs);

    let mut reader = reader(xml, specs, "doc/records/record", 64 * 1_000_000);
    let sections = reader.prepare_document(&cfg).expect("envelope pre-scan");

    // The trailer was extracted and typed.
    let summary = unwrap_map(sections.get("Summary").expect("Summary section retained"));
    assert_eq!(
        summary.get("record_count"),
        Some(&Value::Integer(rows as i64))
    );

    // The retained bytes are orders of magnitude smaller than the input: the
    // body's undeclared subtrees were dropped, never stored. Measuring the
    // section map's own heap footprint (the real retained memory, not a
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
    // that section's flattening — before the whole subtree materializes —
    // naming the section and the cap, rather than OOMing. This is the
    // single-section guarantee: the cap aborts the build, it is not a
    // post-hoc measurement of an already-built section.
    let huge_body: String = (0..50_000)
        .map(|i| format!("<row>row-{i}-{}</row>", "y".repeat(40)))
        .collect();
    let xml = format!(
        "<doc><Header>{huge_body}</Header><records><record><x>1</x></record></records></doc>"
    );
    let input_len = xml.len();
    // The single declared section is multiple MB on its own.
    assert!(
        input_len > 2_000_000,
        "section should be multi-MB: {input_len}"
    );

    let specs: &[SectionSpec] = &[(
        "Header",
        "/doc/Header",
        &[("row", EnvelopeFieldType::String)],
    )];
    let cfg = envelope_config(specs);

    // 8KB cap — far below the multi-MB declared section.
    let mut reader = reader(xml, specs, "doc/records/record", 8_000);

    let err = reader
        .prepare_document(&cfg)
        .expect_err("over-cap retention must fail");
    match err {
        FormatError::Xml(msg) => {
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
        other => panic!("expected FormatError::Xml, got {other:?}"),
    }
}

#[test]
fn prescan_fails_loud_when_cumulative_sections_exceed_cap() {
    // Two declared sections that each fit, but whose cumulative retained
    // bytes cross the cap — proves the running total is charged across the
    // single streaming pass, not reset per section.
    let chunk = "q".repeat(3_000);
    let xml = format!(
        "<doc><A><v>{chunk}</v></A><B><v>{chunk}</v></B>\
         <records><record><x>1</x></record></records></doc>"
    );
    let specs: &[SectionSpec] = &[
        ("A", "/doc/A", &[("v", EnvelopeFieldType::String)]),
        ("B", "/doc/B", &[("v", EnvelopeFieldType::String)]),
    ];
    let cfg = envelope_config(specs);

    // 4KB cap: each 3KB section fits alone, but the pair (6KB) does not.
    let mut reader = reader(xml, specs, "doc/records/record", 4_000);

    let err = reader
        .prepare_document(&cfg)
        .expect_err("cumulative over-cap retention must fail");
    assert!(matches!(err, FormatError::Xml(msg) if msg.contains("max_index_bytes")));
}

#[test]
fn prescan_skips_entirely_when_no_doc_path_declared() {
    // An envelope is declared, but no downstream program reads any `$doc`
    // path (empty `declared_doc_paths`). The path-pruned index is empty, so
    // the pre-scan skips the document and extracts nothing — the body is
    // never walked for sections.
    let xml = r#"<doc><Summary><record_count>3</record_count></Summary><records><record><x>1</x></record><record><x>2</x></record><record><x>3</x></record></records></doc>"#;
    let specs: &[SectionSpec] = &[(
        "Summary",
        "/doc/Summary",
        &[("record_count", EnvelopeFieldType::Int)],
    )];
    let cfg = envelope_config(specs);

    let mut reader = XmlReader::from_reader(
        std::io::Cursor::new(xml.as_bytes().to_vec()),
        XmlReaderConfig {
            record_path: Some("doc/records/record".into()),
            // No declared paths — nothing downstream reads `$doc.*`.
            declared_doc_paths: Vec::new(),
            max_index_bytes: Some(64 * 1_000_000),
            ..Default::default()
        },
    )
    .expect("XML buffer read");

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
fn prescan_skips_cleanly_for_absent_section() {
    // A section the config declares but the XML doesn't carry is absent from
    // the returned map and does NOT abort the run — CXL resolves a missing
    // section to `Value::Null`.
    let xml = r#"<doc><records><record><x>1</x></record></records></doc>"#.to_string();
    let specs: &[SectionSpec] = &[(
        "Trailer",
        "/doc/Trailer",
        &[("count", EnvelopeFieldType::Int)],
    )];
    let cfg = envelope_config(specs);

    let mut reader = reader(xml, specs, "doc/records/record", 64 * 1_000_000);
    let sections = reader
        .prepare_document(&cfg)
        .expect("an absent section is a graceful miss, not an error");
    assert!(
        sections.is_empty(),
        "no section resolves for an absent path"
    );
}

#[test]
fn prescan_skips_cleanly_when_path_traverses_non_matching_structure() {
    // The declared path `/doc/meta/Summary` never appears — `meta` is not a
    // child of `doc` in this document. The path simply never matches the
    // descent stack, so the section is a graceful miss, not a hard error.
    let xml = r#"<doc><other><Summary><record_count>7</record_count></Summary></other><records><record><x>1</x></record></records></doc>"#.to_string();
    let specs: &[SectionSpec] = &[(
        "Summary",
        "/doc/meta/Summary",
        &[("record_count", EnvelopeFieldType::Int)],
    )];
    let cfg = envelope_config(specs);

    let mut reader = reader(xml, specs, "doc/records/record", 64 * 1_000_000);
    let sections = reader
        .prepare_document(&cfg)
        .expect("a non-matching path is a graceful miss, not an error");
    assert!(
        sections.is_empty(),
        "no section resolves through a non-matching structure"
    );
}

#[test]
fn prescan_prunes_the_unread_section_in_a_mixed_envelope() {
    // The envelope declares two sections, but only one has a declared `$doc`
    // path. The read section is present; the unread one is pruned — absent
    // from the output, never materialized.
    let xml = r#"<doc><Head><batch_id>B-1</batch_id></Head><Foot><record_count>5</record_count></Foot><records><record><x>1</x></record></records></doc>"#.to_string();
    let head_spec: SectionSpec = (
        "Head",
        "/doc/Head",
        &[("batch_id", EnvelopeFieldType::String)],
    );
    let foot_spec: SectionSpec = (
        "Foot",
        "/doc/Foot",
        &[("record_count", EnvelopeFieldType::Int)],
    );
    // Both sections are declared in the envelope config...
    let cfg = envelope_config(&[head_spec, foot_spec]);
    // ...but only `Foot` is referenced by a `$doc` path.
    let declared = declared_paths(&[foot_spec]);

    let mut reader = XmlReader::from_reader(
        std::io::Cursor::new(xml.into_bytes()),
        XmlReaderConfig {
            record_path: Some("doc/records/record".into()),
            declared_doc_paths: declared,
            max_index_bytes: Some(64 * 1_000_000),
            ..Default::default()
        },
    )
    .expect("XML buffer read");

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

#[test]
fn prescan_preserves_namespaces_attributes_and_cdata() {
    // The streaming pre-scan must map element names, child attributes, and
    // CData exactly as the body reader does: a namespace-stripped section
    // element, an attribute on a child element (prefixed and `.`-joined),
    // and a CData child payload all coerce into the declared section fields
    // byte-identically to body extraction.
    let xml = r#"<doc><ns:Meta><tag kind="run"></tag><note><![CDATA[a & b]]></note></ns:Meta><records><record><x>1</x></record></records></doc>"#.to_string();
    let specs: &[SectionSpec] = &[(
        "Meta",
        "/doc/Meta",
        &[
            ("tag.@kind", EnvelopeFieldType::String),
            ("note", EnvelopeFieldType::String),
        ],
    )];
    let cfg = envelope_config(specs);

    let mut reader = reader(xml, specs, "doc/records/record", 64 * 1_000_000);
    let sections = reader.prepare_document(&cfg).expect("pre-scan");
    let meta = unwrap_map(sections.get("Meta").expect("Meta retained"));
    assert_eq!(meta.get("tag.@kind"), Some(&Value::String("run".into())));
    assert_eq!(meta.get("note"), Some(&Value::String("a & b".into())));
}

#[test]
fn prescan_retains_the_matched_section_elements_own_attributes() {
    // The matched section element carries its own attributes
    // (`<Summary id="5" run="R-1">…`). The streaming pre-scan must seed those
    // as bare `@attr` fields of the section — exactly as the body reader seeds
    // a record's start-tag attributes — so `$doc.Summary.@id` resolves. Child
    // element attributes are still captured under their `.`-joined prefix.
    let xml = r#"<doc><Summary id="5" run="R-1"><line tag="net"></line><record_count>3</record_count></Summary><records><record><x>1</x></record></records></doc>"#.to_string();
    let specs: &[SectionSpec] = &[(
        "Summary",
        "/doc/Summary",
        &[
            ("@id", EnvelopeFieldType::Int),
            ("@run", EnvelopeFieldType::String),
            ("line.@tag", EnvelopeFieldType::String),
            ("record_count", EnvelopeFieldType::Int),
        ],
    )];
    let cfg = envelope_config(specs);

    let mut reader = reader(xml, specs, "doc/records/record", 64 * 1_000_000);
    let sections = reader.prepare_document(&cfg).expect("pre-scan");
    let summary = unwrap_map(sections.get("Summary").expect("Summary retained"));
    // The matched element's OWN attributes — the #505 regression.
    assert_eq!(summary.get("@id"), Some(&Value::Integer(5)));
    assert_eq!(summary.get("@run"), Some(&Value::String("R-1".into())));
    // Child-element attributes and child text still captured alongside.
    assert_eq!(summary.get("line.@tag"), Some(&Value::String("net".into())));
    assert_eq!(summary.get("record_count"), Some(&Value::Integer(3)));
}

/// A multi-MB body streams element-at-a-time from a path-backed source: the
/// body walks one `<record>` at a time and re-opening by path means no
/// whole-document byte buffer is held either.
#[test]
fn large_record_body_streams_without_collecting_or_buffering() {
    // ~1.5 MB document of ~50k small records, written to a real file so the
    // reader re-opens by path (the production shape) rather than buffering
    // bytes.
    let rows = 50_000usize;
    let mut xml = String::from("<doc><records>");
    for i in 0..rows {
        xml.push_str(&format!(
            "<record><id>{i}</id><name>row-{i}</name></record>"
        ));
    }
    xml.push_str("</records></doc>");
    assert!(
        xml.len() > 1_000_000,
        "body should be > 1 MB: {}",
        xml.len()
    );

    let path = temp_path("large-body");
    std::fs::File::create(&path)
        .unwrap()
        .write_all(xml.as_bytes())
        .unwrap();

    let mut reader = XmlReader::from_source(
        ReopenableSource::path(&path),
        XmlReaderConfig {
            record_path: Some("doc/records/record".into()),
            ..Default::default()
        },
    )
    .unwrap();

    // Reading the first record must not require materializing the rest: it
    // streams one element. Then every record streams in order.
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
    let _ = std::fs::remove_file(&path);
    assert_eq!(count, rows, "every body record streamed exactly once");
}

/// A path-backed input rewritten between the body open (at construction) and
/// the envelope pre-scan (at `prepare_document`) is caught: the pre-scan
/// refuses to splice an envelope from new bytes onto a body parsed from old
/// ones, and fails loud instead of silently mismatching.
#[test]
fn envelope_prescan_rejects_a_file_changed_between_passes() {
    let path = temp_path("changed-between-passes");
    let original = r#"<doc><Summary><record_count>2</record_count></Summary><records><record><x>1</x></record><record><x>2</x></record></records></doc>"#;
    std::fs::File::create(&path)
        .unwrap()
        .write_all(original.as_bytes())
        .unwrap();

    let specs: &[SectionSpec] = &[(
        "Summary",
        "/doc/Summary",
        &[("record_count", EnvelopeFieldType::Int)],
    )];
    let cfg = envelope_config(specs);

    // Construction opens the body and snapshots the file's identity.
    let mut reader = XmlReader::from_source(
        ReopenableSource::path(&path),
        XmlReaderConfig {
            record_path: Some("doc/records/record".into()),
            declared_doc_paths: declared_paths(specs),
            max_index_bytes: Some(64 * 1_000_000),
            ..Default::default()
        },
    )
    .unwrap();

    // An external producer rewrites the file to different content before the
    // pre-scan re-opens it.
    std::fs::File::create(&path)
        .unwrap()
        .write_all(br#"<doc><Summary><record_count>9999</record_count></Summary><records><record><x>7</x></record><record><x>8</x></record><record><x>9</x></record></records></doc>"#)
        .unwrap();

    let err = reader
        .prepare_document(&cfg)
        .expect_err("a file changed between the two passes must fail loud");
    let _ = std::fs::remove_file(&path);
    match err {
        FormatError::Io(e) => assert!(
            e.to_string().contains("changed between"),
            "error names the mid-run change: {e}"
        ),
        other => panic!("expected an Io change error, got {other:?}"),
    }
}

/// A malformed document (an unclosed element) surfaces a hard
/// [`FormatError::Xml`] from the streaming walk rather than silently
/// truncating the record stream — the pull parser reports the parse failure,
/// it is not swallowed.
#[test]
fn malformed_unclosed_element_surfaces_xml_error() {
    // `<name>` is never closed before EOF.
    let xml = r#"<doc><records><record><id>1</id><name>unterminated</record></records></doc>"#;
    let mut reader = XmlReader::from_reader(
        std::io::Cursor::new(xml.as_bytes().to_vec()),
        XmlReaderConfig {
            record_path: Some("doc/records/record".into()),
            ..Default::default()
        },
    )
    .expect("construction reads no bytes; a parse error surfaces while streaming");

    // Drive the reader until it either errors or EOFs. A well-formedness
    // violation must produce an `Xml` error, never a clean `None` that hides
    // the truncation.
    let mut saw_error = false;
    loop {
        match reader.next_record() {
            Ok(Some(_)) => continue,
            Ok(None) => break,
            Err(FormatError::Xml(_)) => {
                saw_error = true;
                break;
            }
            Err(other) => panic!("expected FormatError::Xml, got {other:?}"),
        }
    }
    assert!(
        saw_error,
        "an unclosed element must surface as FormatError::Xml, not silently truncate"
    );
}

/// A document truncated *inside an open record element* fails loud rather
/// than emitting the partial fields read so far. The record's own closing
/// tag never arrives, so the reader must surface a [`FormatError::Xml`]
/// naming the cut element — not a silently short record.
#[test]
fn truncation_inside_a_record_surfaces_xml_error() {
    // `<name>` (and the enclosing `<record>`) are never closed before EOF.
    let xml = r#"<doc><records><record><id>1</id><name>partial"#;
    let mut reader = XmlReader::from_reader(
        std::io::Cursor::new(xml.as_bytes().to_vec()),
        XmlReaderConfig {
            record_path: Some("doc/records/record".into()),
            ..Default::default()
        },
    )
    .expect("construction reads no bytes");

    // The truncated first record must never surface as a record: driving the
    // reader yields a hard `Xml` error, not a partial `Some` or a clean `None`.
    let mut saw_partial_record = false;
    let err = loop {
        match reader.next_record() {
            Ok(Some(_)) => {
                saw_partial_record = true;
                continue;
            }
            Ok(None) => panic!("a truncated record must not end cleanly"),
            Err(e) => break e,
        }
    };
    assert!(
        !saw_partial_record,
        "no partial record may be emitted from a truncated document"
    );
    match err {
        FormatError::Xml(msg) => {
            assert!(
                msg.contains("record.name"),
                "error names the cut element path: {msg}"
            );
            assert!(
                msg.contains("ended before its closing tag"),
                "error states the input ended before the close: {msg}"
            );
        }
        other => panic!("expected FormatError::Xml, got {other:?}"),
    }
}

/// A document truncated *inside a non-matching sibling subtree the reader is
/// skipping* fails loud rather than ending cleanly. The navigation-time
/// subtree skip must not treat EOF as a valid end of the skipped element.
#[test]
fn truncation_inside_a_skipped_subtree_surfaces_xml_error() {
    // `<meta>` is a sibling the record-path navigation skips; the document is
    // cut off inside it, before `<meta>` (or `<info>`) closes.
    let xml = r#"<doc><meta><info>partial"#;
    let mut reader = XmlReader::from_reader(
        std::io::Cursor::new(xml.as_bytes().to_vec()),
        XmlReaderConfig {
            record_path: Some("doc/records/record".into()),
            ..Default::default()
        },
    )
    .expect("construction reads no bytes");

    let err = reader
        .next_record()
        .expect_err("a subtree skip that hits EOF before its close must fail");
    match err {
        FormatError::Xml(msg) => {
            assert!(
                msg.contains("skipping element") && msg.contains("meta"),
                "error names the skipped element: {msg}"
            );
            assert!(
                msg.contains("ended before its closing tag"),
                "error states the input ended before the close: {msg}"
            );
        }
        other => panic!("expected FormatError::Xml, got {other:?}"),
    }
}

/// A document truncated *between records*, after a complete record but before
/// the record container and root close, streams the complete record and then
/// fails loud when the reader tries to advance past it — the unclosed
/// ancestors are a truncated document, not an exhausted record set.
#[test]
fn truncation_between_records_surfaces_xml_error() {
    // One complete `<record>`, then EOF with `<records>` and `<doc>` still open.
    let xml = r#"<doc><records><record><x>1</x></record>"#;
    let mut reader = XmlReader::from_reader(
        std::io::Cursor::new(xml.as_bytes().to_vec()),
        XmlReaderConfig {
            record_path: Some("doc/records/record".into()),
            ..Default::default()
        },
    )
    .expect("construction reads no bytes");

    // The complete record streams first — streaming does not buffer the whole
    // file to pre-validate it.
    let first = reader
        .next_record()
        .expect("the complete record streams")
        .expect("one record");
    assert_eq!(first.get("x"), Some(&Value::Integer(1)));

    // Advancing past it hits EOF with two ancestors still open.
    let err = reader
        .next_record()
        .expect_err("EOF under an unclosed root is a truncated document");
    match err {
        FormatError::Xml(msg) => {
            assert!(
                msg.contains("2 element(s) were still open"),
                "error counts the open ancestors: {msg}"
            );
        }
        other => panic!("expected FormatError::Xml, got {other:?}"),
    }
}

/// A document truncated *inside a declared envelope section* fails the
/// envelope pre-scan rather than returning that section's partial fields — no
/// partial `$doc` metadata is produced from a cut-off section.
#[test]
fn truncation_inside_an_envelope_section_surfaces_xml_error() {
    // `<record_count>` (and the enclosing `<Summary>`) are never closed.
    let xml = r#"<doc><Summary><record_count>5"#.to_string();
    let specs: &[SectionSpec] = &[(
        "Summary",
        "/doc/Summary",
        &[("record_count", EnvelopeFieldType::Int)],
    )];
    let cfg = envelope_config(specs);

    let mut reader = reader(xml, specs, "doc/records/record", 64 * 1_000_000);
    let err = reader
        .prepare_document(&cfg)
        .expect_err("a section cut off before its close must fail the pre-scan");
    match err {
        FormatError::Xml(msg) => {
            assert!(
                msg.contains("envelope section") && msg.contains("Summary"),
                "error names the cut section: {msg}"
            );
            assert!(
                msg.contains("ended before its closing tag"),
                "error states the input ended before the close: {msg}"
            );
        }
        other => panic!("expected FormatError::Xml, got {other:?}"),
    }
}

/// A document truncated *during the envelope pre-scan but outside any declared
/// section* still fails loud. The top-level streaming walk must not accept an
/// EOF that leaves elements open, or a declared section that would have
/// appeared later is silently missed.
#[test]
fn truncation_during_prescan_outside_a_section_surfaces_xml_error() {
    // The declared `Summary` section sits at the document tail, but the input
    // is cut off inside the body before it ever appears.
    let xml = r#"<doc><records><record><x>1</x></record>"#.to_string();
    let specs: &[SectionSpec] = &[(
        "Summary",
        "/doc/Summary",
        &[("record_count", EnvelopeFieldType::Int)],
    )];
    let cfg = envelope_config(specs);

    let mut reader = reader(xml, specs, "doc/records/record", 64 * 1_000_000);
    let err = reader
        .prepare_document(&cfg)
        .expect_err("a pre-scan that hits EOF with open elements must fail");
    match err {
        FormatError::Xml(msg) => {
            assert!(
                msg.contains("envelope pre-scan"),
                "error identifies the pre-scan: {msg}"
            );
            assert!(
                msg.contains("doc/records"),
                "error names the open descent path: {msg}"
            );
        }
        other => panic!("expected FormatError::Xml, got {other:?}"),
    }
}

/// A deeply nested document (200+ levels) streams to completion without
/// crashing: clinker's XML walk tracks depth with heap `Vec`s and `usize`
/// counters, never native recursion, so a deep document cannot overflow the
/// stack the way a recursive descent parser would.
#[test]
fn deeply_nested_document_streams_without_stack_overflow() {
    const DEPTH: usize = 250;
    // Build `<doc><records><record>` then DEPTH nested `<n>` wrappers around a
    // leaf value, closing them all back out.
    let mut xml = String::from("<doc><records><record>");
    for _ in 0..DEPTH {
        xml.push_str("<n>");
    }
    xml.push_str("<leaf>deep</leaf>");
    for _ in 0..DEPTH {
        xml.push_str("</n>");
    }
    xml.push_str("</record></records></doc>");

    let mut reader = XmlReader::from_reader(
        std::io::Cursor::new(xml.into_bytes()),
        XmlReaderConfig {
            record_path: Some("doc/records/record".into()),
            ..Default::default()
        },
    )
    .expect("construction reads no bytes");

    // The single deeply-nested record streams cleanly — the flattened leaf is
    // present under its `.`-joined nesting prefix, and the iterative walk never
    // recurses, so no stack overflow.
    let rec = reader
        .next_record()
        .expect("deep nesting streams, never crashes")
        .expect("one record");
    let leaf_key: String = std::iter::repeat_n("n", DEPTH)
        .chain(std::iter::once("leaf"))
        .collect::<Vec<_>>()
        .join(".");
    assert_eq!(rec.get(&leaf_key), Some(&Value::String("deep".into())));
    assert!(reader.next_record().unwrap().is_none());
}

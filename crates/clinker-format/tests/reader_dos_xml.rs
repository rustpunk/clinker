//! Pathological-XML safety tests for the streaming XML reader.
//!
//! These pin the guarantees on adversarial XML the reader must never inflate or
//! resolve unsafely:
//!
//! * Entity-expansion (billion-laughs) and external-entity (XXE) payloads stay
//!   inert — quick-xml does not expand DTD-declared or SYSTEM entities, and the
//!   reader surfaces the unresolved reference as a clean error rather than
//!   materializing an exponential string or fetching a file.
//! * A deeply nested sibling that the reader *skips* (`skip_subtree`) is walked
//!   iteratively (an integer depth counter, not the call stack), so a
//!   pathological nesting depth in a skipped subtree cannot overflow the stack.
//!   (The record-extraction path's deep-nesting guarantee is pinned by
//!   `streaming_doc_index_xml::deeply_nested_document_streams_without_stack_overflow`.)
//!
//! Companion to the JSON suite in `reader_dos_json.rs`.

use std::io::Cursor;

use clinker_format::FormatReader;
use clinker_format::error::FormatError;
use clinker_format::xml::reader::{XmlReader, XmlReaderConfig};
use clinker_record::Value;

fn reader(xml: String, record_path: &str) -> XmlReader {
    XmlReader::from_reader(
        Cursor::new(xml.into_bytes()),
        XmlReaderConfig {
            record_path: Some(record_path.into()),
            ..Default::default()
        },
    )
    .expect("XML buffer read")
}

/// Drive `schema()` then drain every record, returning the first error the
/// reader surfaces (or `Ok(count)` when the whole document reads cleanly).
/// Returning at all proves the reader did not overflow the stack.
fn drain(r: &mut XmlReader) -> Result<usize, FormatError> {
    r.schema()?;
    let mut n = 0;
    while r.next_record()?.is_some() {
        n += 1;
    }
    Ok(n)
}

#[test]
fn billion_laughs_entities_do_not_expand() {
    // Classic entity-expansion bomb. The DOCTYPE's <!ENTITY> declarations are
    // inert: quick-xml never expands `&lol4;`, so the reader meets an
    // unresolved general entity and errors instead of inflating an exponential
    // string. Memory/time cannot blow up because no expansion ever happens.
    let xml = concat!(
        "<?xml version=\"1.0\"?>\n",
        "<!DOCTYPE root [\n",
        "  <!ENTITY lol \"lol\">\n",
        "  <!ENTITY lol1 \"&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;\">\n",
        "  <!ENTITY lol2 \"&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;\">\n",
        "  <!ENTITY lol3 \"&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;\">\n",
        "  <!ENTITY lol4 \"&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;\">\n",
        "]>\n",
        "<root><record><v>&lol4;</v></record></root>",
    )
    .to_string();
    let mut r = reader(xml, "root/record");
    let err = drain(&mut r).expect_err("billion-laughs entity must error, never expand");
    match err {
        // The error names the unresolved entity — proving the reader met the
        // `&lol4;` reference and rejected it, rather than expanding it into a
        // string of `lol`s.
        FormatError::Xml(msg) => assert!(
            msg.contains("lol4"),
            "error must name the unresolved entity: {msg}"
        ),
        other => panic!("expected FormatError::Xml, got {other:?}"),
    }
}

#[test]
fn external_system_entity_is_not_resolved() {
    // An external (SYSTEM) entity must never be fetched. quick-xml does not
    // resolve it, so the reader meets an unresolved entity and errors — it
    // never touches the filesystem or network.
    let xml = concat!(
        "<?xml version=\"1.0\"?>\n",
        "<!DOCTYPE root [ <!ENTITY xxe SYSTEM \"file:///etc/passwd\"> ]>\n",
        "<root><record><v>&xxe;</v></record></root>",
    )
    .to_string();
    let mut r = reader(xml, "root/record");
    let err = drain(&mut r).expect_err("external entity must not resolve");
    match err {
        // The error names the unresolved entity — the SYSTEM entity was never
        // fetched; the reader simply met an entity it does not resolve.
        FormatError::Xml(msg) => assert!(
            msg.contains("xxe"),
            "error must name the unresolved external entity: {msg}"
        ),
        other => panic!("expected FormatError::Xml, got {other:?}"),
    }
}

#[test]
fn deeply_nested_skipped_sibling_does_not_overflow_the_stack() {
    // A deeply nested sibling BEFORE the record element is skipped iteratively
    // (`skip_subtree` tracks depth with a counter, never a recursive call), so
    // it cannot overflow the stack. The record that follows the skipped subtree
    // still reads correctly.
    let depth = 5_000;
    let mut xml = String::with_capacity(depth * 7 + 48);
    xml.push_str("<root><skip>");
    for _ in 0..depth {
        xml.push_str("<n>");
    }
    for _ in 0..depth {
        xml.push_str("</n>");
    }
    xml.push_str("</skip><rec><x>1</x></rec></root>");

    let mut r = reader(xml, "root/rec");
    let rec = r
        .next_record()
        .expect("the record after a deep skipped sibling is reached, not crashed")
        .expect("one record");
    assert_eq!(rec.get("x"), Some(&Value::Integer(1)));
    assert!(r.next_record().unwrap().is_none());
}

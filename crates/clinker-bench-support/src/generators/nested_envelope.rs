//! Streamed nested-envelope document generators for the bounded-memory
//! `$doc` proof.
//!
//! Shape (JSON / XML twins):
//! - a small **head** section at a declared pointer (`"Head"` / `<Head>`),
//! - a huge **undeclared body** array (`"records"` / `<records>`),
//! - a small **trailing** section after the body (`"Summary"` / `<Summary>`).
//!
//! The trailing section after a multi-GB body forces the envelope pre-scan
//! to stream *past* the whole body to reach it — the exact path the proof
//! guards: a reader that buffered the body to find the trailer would blow
//! the memory budget the proof asserts.
//!
//! **Streaming guarantee.** Each generator takes `&mut impl Write` and emits
//! the head, then loops emitting one body record at a time into a small
//! reused scratch `Vec` that is flushed to the writer and cleared per record,
//! then emits the trailer. No accumulator ever grows with `body_records`, so
//! generating a 1 GiB document costs `O(one record)` of memory, not `O(1
//! GiB)`. Determinism comes from `fastrand::Rng::with_seed`, so a fixed
//! `(body_records, layout, string_len, blob_len, seed)` reproduces
//! byte-for-byte.

use std::io::{self, Write};

use crate::FieldKind;

/// Stream a nested-envelope JSON document to `w`.
///
/// Emits `{ "Head": {...}, "records": [ ...body_records... ], "Summary":
/// {...} }`. The `Head` section carries a deterministic `batch_id`; the
/// `Summary` trailer carries the body record count. Body records are
/// `{ "id": <n>, "fN": <value>, ..., "blob": "<blob_len chars>" }` per
/// `layout`.
///
/// `blob_len` is an undeclared per-record text field that scales the body's
/// *byte* size independently of `body_records`. It lets a memory proof
/// inflate the body to GB scale with only a few thousand records, so the
/// body byte-size — not the record count — is the variable under test.
///
/// # Errors
///
/// Propagates any [`io::Error`] from `w` (a full disk, a closed pipe).
pub fn write_nested_envelope_json(
    w: &mut impl Write,
    body_records: usize,
    layout: &[FieldKind],
    string_len: usize,
    blob_len: usize,
    seed: u64,
) -> io::Result<()> {
    let mut rng = fastrand::Rng::with_seed(seed);
    // One reused scratch buffer for the whole body — never grows with
    // `body_records`. Sized for a single record's worst case up front so the
    // per-record `clear()`/refill never reallocates.
    let mut scratch: Vec<u8> = Vec::with_capacity(64 + layout.len() * (string_len + 16) + blob_len);

    // Head section at the declared `/Head` pointer.
    write!(
        w,
        "{{\"Head\":{{\"batch_id\":\"RUN-{seed:08}\"}},\"records\":["
    )?;

    for i in 0..body_records {
        scratch.clear();
        if i > 0 {
            scratch.push(b',');
        }
        scratch.extend_from_slice(format!("{{\"id\":{i}").as_bytes());
        for (f, &kind) in layout.iter().enumerate() {
            scratch.extend_from_slice(format!(",\"f{f}\":").as_bytes());
            write_json_field(&mut scratch, kind, &mut rng, string_len);
        }
        if blob_len > 0 {
            scratch.extend_from_slice(b",\"blob\":\"");
            push_blob(&mut scratch, &mut rng, blob_len);
            scratch.push(b'"');
        }
        scratch.push(b'}');
        w.write_all(&scratch)?;
    }

    // Trailing section after the body — reached only by streaming past it.
    write!(w, "],\"Summary\":{{\"record_count\":{body_records}}}}}")?;
    Ok(())
}

/// Stream a nested-envelope XML document to `w`.
///
/// Emits `<doc><Head>...</Head><records>...</records><Summary>...</Summary></doc>`
/// with a prolog. The `Head` element carries a deterministic `batch_id`; the
/// `Summary` trailer carries the body record count. Body records are
/// `<record><id>n</id><fN>value</fN>...<blob>...</blob></record>` per
/// `layout`.
///
/// `blob_len` is an undeclared per-record text element that scales the
/// body's *byte* size independently of `body_records` — see
/// [`write_nested_envelope_json`].
///
/// # Errors
///
/// Propagates any [`io::Error`] from `w`.
pub fn write_nested_envelope_xml(
    w: &mut impl Write,
    body_records: usize,
    layout: &[FieldKind],
    string_len: usize,
    blob_len: usize,
    seed: u64,
) -> io::Result<()> {
    let mut rng = fastrand::Rng::with_seed(seed);
    let mut scratch: Vec<u8> = Vec::with_capacity(64 + layout.len() * (string_len + 24) + blob_len);

    w.write_all(b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<doc>")?;
    write!(
        w,
        "<Head><batch_id>RUN-{seed:08}</batch_id></Head><records>"
    )?;

    for i in 0..body_records {
        scratch.clear();
        scratch.extend_from_slice(format!("<record><id>{i}</id>").as_bytes());
        for (f, &kind) in layout.iter().enumerate() {
            scratch.extend_from_slice(format!("<f{f}>").as_bytes());
            // Body values are alphanumeric/date/numeric — none contain XML
            // metacharacters, so no escaping is needed for the proof corpus.
            crate::write_field_value(&mut scratch, kind, &mut rng, string_len);
            scratch.extend_from_slice(format!("</f{f}>").as_bytes());
        }
        if blob_len > 0 {
            scratch.extend_from_slice(b"<blob>");
            // The blob is lowercase ASCII — no XML metacharacters.
            push_blob(&mut scratch, &mut rng, blob_len);
            scratch.extend_from_slice(b"</blob>");
        }
        scratch.extend_from_slice(b"</record>");
        w.write_all(&scratch)?;
    }

    write!(
        w,
        "</records><Summary><record_count>{body_records}</record_count></Summary></doc>"
    )?;
    Ok(())
}

/// Append `blob_len` deterministic lowercase-ASCII bytes to `buf`. Carries
/// no JSON/XML metacharacters, so it needs no escaping in either format.
fn push_blob(buf: &mut Vec<u8>, rng: &mut fastrand::Rng, blob_len: usize) {
    for _ in 0..blob_len {
        buf.push(rng.u8(b'a'..=b'z'));
    }
}

/// Write one JSON field value, quoting string-like kinds. Mirrors the
/// per-field quoting in [`crate::generators::json`].
fn write_json_field(
    buf: &mut Vec<u8>,
    kind: FieldKind,
    rng: &mut fastrand::Rng,
    string_len: usize,
) {
    match kind {
        FieldKind::Int | FieldKind::Float | FieldKind::Bool => {
            crate::write_field_value(buf, kind, rng, string_len);
        }
        FieldKind::String | FieldKind::Date => {
            buf.push(b'"');
            crate::write_field_value(buf, kind, rng, string_len);
            buf.push(b'"');
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A small JSON instance parses as valid JSON, exposes the declared
    /// head/trailer sections, carries exactly `body_records` body rows, and
    /// includes the undeclared per-record `blob` when `blob_len > 0`.
    #[test]
    fn json_generator_produces_valid_document() {
        let layout = FieldKind::default_layout(3);
        let mut out = Vec::new();
        write_nested_envelope_json(&mut out, 10, &layout, 8, 32, 42).unwrap();

        let doc: serde_json::Value = serde_json::from_slice(&out).expect("valid JSON document");
        assert_eq!(doc["Head"]["batch_id"], serde_json::json!("RUN-00000042"));
        assert_eq!(doc["Summary"]["record_count"], serde_json::json!(10));
        let records = doc["records"].as_array().expect("records is an array");
        assert_eq!(records.len(), 10, "every body record is present");
        // Body ids stream in order 0..10, each carrying the sized blob.
        for (i, rec) in records.iter().enumerate() {
            assert_eq!(rec["id"], serde_json::json!(i));
            assert_eq!(
                rec["blob"].as_str().expect("blob is a string").len(),
                32,
                "per-record blob is blob_len chars"
            );
        }
    }

    /// `blob_len = 0` omits the blob field entirely.
    #[test]
    fn json_generator_omits_blob_when_zero() {
        let layout = FieldKind::default_layout(2);
        let mut out = Vec::new();
        write_nested_envelope_json(&mut out, 3, &layout, 6, 0, 1).unwrap();
        let doc: serde_json::Value = serde_json::from_slice(&out).expect("valid JSON");
        assert!(
            doc["records"][0].get("blob").is_none(),
            "no blob field when blob_len is 0"
        );
    }

    /// Same parameters always produce identical bytes — the seed drives a
    /// reproducible body, blob included.
    #[test]
    fn json_generator_deterministic() {
        let layout = FieldKind::default_layout(4);
        let mut a = Vec::new();
        let mut b = Vec::new();
        write_nested_envelope_json(&mut a, 50, &layout, 6, 16, 7).unwrap();
        write_nested_envelope_json(&mut b, 50, &layout, 6, 16, 7).unwrap();
        assert_eq!(a, b);
    }

    /// A small XML instance is well-formed, and the `record` count matches.
    #[test]
    fn xml_generator_produces_valid_document() {
        use quick_xml::Reader;
        use quick_xml::events::Event;

        let layout = FieldKind::default_layout(3);
        let mut out = Vec::new();
        write_nested_envelope_xml(&mut out, 10, &layout, 8, 32, 42).unwrap();

        let mut reader = Reader::from_reader(out.as_slice());
        let mut record_count = 0u64;
        let mut saw_head = false;
        let mut saw_summary = false;
        let mut saw_blob = false;
        let mut buf = Vec::new();
        loop {
            match reader.read_event_into(&mut buf) {
                Ok(Event::Start(e)) => match e.name().as_ref() {
                    b"record" => record_count += 1,
                    b"Head" => saw_head = true,
                    b"Summary" => saw_summary = true,
                    b"blob" => saw_blob = true,
                    _ => {}
                },
                Ok(Event::Eof) => break,
                Err(e) => panic!("XML parse error: {e}"),
                _ => {}
            }
            buf.clear();
        }
        assert_eq!(record_count, 10, "every body record is present");
        assert!(saw_head, "declared head section emitted");
        assert!(saw_summary, "trailing summary section emitted");
        assert!(
            saw_blob,
            "per-record blob element emitted when blob_len > 0"
        );
    }

    /// Same parameters always produce identical XML bytes.
    #[test]
    fn xml_generator_deterministic() {
        let layout = FieldKind::default_layout(4);
        let mut a = Vec::new();
        let mut b = Vec::new();
        write_nested_envelope_xml(&mut a, 50, &layout, 6, 16, 7).unwrap();
        write_nested_envelope_xml(&mut b, 50, &layout, 6, 16, 7).unwrap();
        assert_eq!(a, b);
    }
}

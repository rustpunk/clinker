//! Deterministic XML payload generator for benchmarks.

use crate::FieldKind;

/// Generate deterministic XML bytes with prolog and `<records>` root element.
///
/// Streams records into a pre-allocated `Vec<u8>`. Uses hand-rolled byte
/// writing for performance (D-6).
pub fn generate_xml(
    record_count: usize,
    field_types: &[FieldKind],
    string_len: usize,
    seed: u64,
) -> Vec<u8> {
    let field_count = field_types.len();
    let mut buf = Vec::with_capacity(38 + record_count * field_count * (string_len + 20) + 20);
    let mut rng = fastrand::Rng::with_seed(seed);

    // Prolog (D-5)
    buf.extend_from_slice(b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<records>\n");

    for _ in 0..record_count {
        buf.extend_from_slice(b"<record>");
        for (i, &kind) in field_types.iter().enumerate() {
            let tag = format!("f{i}");
            buf.extend_from_slice(b"<");
            buf.extend_from_slice(tag.as_bytes());
            buf.extend_from_slice(b">");

            crate::write_field_value(&mut buf, kind, &mut rng, string_len);

            buf.extend_from_slice(b"</");
            buf.extend_from_slice(tag.as_bytes());
            buf.extend_from_slice(b">");
        }
        buf.extend_from_slice(b"</record>\n");
    }

    buf.extend_from_slice(b"</records>\n");
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify generated XML is well-formed and contains the expected record count.
    /// Uses quick-xml (dev-dep) to parse — catches structural errors the generator
    /// could introduce (unclosed tags, bad escaping).
    #[test]
    fn test_xml_generator_produces_valid_xml() {
        let xml = generate_xml(100, &FieldKind::default_layout(5), 8, 42);

        use quick_xml::Reader;
        use quick_xml::events::Event;
        let mut reader = Reader::from_reader(xml.as_slice());
        let mut record_count = 0u64;
        let mut buf = Vec::new();
        loop {
            match reader.read_event_into(&mut buf) {
                Ok(Event::Start(e)) if e.name().as_ref() == b"record" => record_count += 1,
                Ok(Event::Eof) => break,
                Err(e) => panic!("XML parse error: {e}"),
                _ => {}
            }
            buf.clear();
        }

        assert_eq!(record_count, 100);
    }

    /// Verify deterministic output — same parameters always produce identical bytes.
    #[test]
    fn test_xml_generator_deterministic() {
        let ft = FieldKind::default_layout(4);
        let a = generate_xml(50, &ft, 6, 42);
        let b = generate_xml(50, &ft, 6, 42);
        assert_eq!(a, b);
    }

    /// V-5-1: Verify XLARGE generation completes without OOM.
    /// Run with `cargo test -- --ignored` (slow — generates ~600MB).
    #[test]
    #[ignore]
    fn test_xml_generator_xlarge_no_oom() {
        let xml = generate_xml(1_000_000, &FieldKind::default_layout(20), 10, 42);
        assert!(xml.len() > 100_000_000);
    }
}

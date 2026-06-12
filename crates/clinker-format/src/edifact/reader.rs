//! UN/EDIFACT interchange reader.
//!
//! Streaming, row-at-a-time: each non-service segment of an interchange
//! becomes one [`Record`] with a static positional schema
//! `[seg_id, msg_ref, msg_type, e01..e<max>]`. Service segments
//! (`UNB`/`UNZ`/`UNH`/`UNT`) are consumed by the reader to drive
//! envelope state and inline structural validation; they are never
//! emitted as body records.
//!
//! Memory model: only the interchange header (`UNA` + `UNB`) is
//! pre-scanned and retained, so `prepare_document` exposes the `UNB`
//! control fields as a `$doc` section with O(UNB) held memory. The body
//! streams one segment at a time — the whole interchange is never
//! buffered. Trailer control counts (`UNT`/`UNZ`) are validated as they
//! arrive, not surfaced as envelope sections, because they follow the
//! body and the document context is sealed before body streaming.

use std::io::{BufReader, Read};
use std::sync::Arc;

use clinker_record::{Record, Schema, Value};
use indexmap::IndexMap;

use crate::edifact::tokenizer::{ParsedSegment, SegmentTokenizer, split_segment};
use crate::edifact::{RAW_ELEMENTS_KEY, unz_echo_matches_unb};
use crate::envelope::{EnvelopeConfig, EnvelopeExtract, coerce_section_fields};
use crate::error::FormatError;
use crate::traits::FormatReader;

/// Default ceiling on the number of positional element columns the
/// record schema exposes. A segment carrying more data elements than
/// this errors with guidance rather than silently truncating.
const DEFAULT_MAX_ELEMENTS: usize = 32;

/// Configuration for the EDIFACT reader.
pub struct EdifactReaderConfig {
    /// Number of positional `eNN` element columns on the record schema.
    /// A body segment with more data elements than this is rejected.
    pub max_elements: usize,
}

impl Default for EdifactReaderConfig {
    fn default() -> Self {
        Self {
            max_elements: DEFAULT_MAX_ELEMENTS,
        }
    }
}

/// Streaming EDIFACT interchange reader.
///
/// Holds the tokenizer, the static positional schema, and the inline
/// envelope-validation state (open-message segment count, control-ref
/// echoes, interchange message count). The `UNB` elements are stashed
/// at initialization so [`FormatReader::prepare_document`] can serve a
/// `UNB` envelope section without re-reading the source.
pub struct EdifactReader<R: Read> {
    tokenizer: SegmentTokenizer<BufReader<R>>,
    schema: Arc<Schema>,
    max_elements: usize,
    initialized: bool,
    /// Raw `UNB` data elements, stashed at init for envelope serving and
    /// for structurally validating the `UNZ` control-reference echo once
    /// the trailer arrives.
    unb_elements: Vec<String>,
    /// State of the message currently being streamed, if any.
    open_message: Option<OpenMessage>,
    /// Count of `UNH` messages seen, checked against `UNZ`.
    message_count: u64,
    /// `true` once `UNZ` has been consumed; any further segment is an
    /// after-trailer-content error.
    interchange_closed: bool,
    done: bool,
}

/// Per-message validation state for the message currently streaming.
struct OpenMessage {
    /// `UNH` message reference (UNH element 1), echoed by `UNT`.
    reference: String,
    /// Count of segments in this message including `UNH` (and, at close,
    /// `UNT`), checked against the `UNT` segment count.
    segment_count: u64,
}

impl<R: Read> EdifactReader<R> {
    /// Build a reader over any `Read` source with the given element-width
    /// configuration. Delimiter discovery and `UNB` consumption are
    /// deferred to the first read.
    pub fn new(reader: R, config: EdifactReaderConfig) -> Self {
        let schema = build_schema(config.max_elements);
        Self {
            tokenizer: SegmentTokenizer::new(BufReader::new(reader)),
            schema,
            max_elements: config.max_elements,
            initialized: false,
            unb_elements: Vec::new(),
            open_message: None,
            message_count: 0,
            interchange_closed: false,
            done: false,
        }
    }

    /// Consume the optional `UNA` and the mandatory `UNB`, stashing the
    /// `UNB` data elements for envelope serving and for the deferred
    /// control-reference validation against the `UNZ` trailer. Idempotent.
    fn ensure_initialized(&mut self) -> Result<(), FormatError> {
        if self.initialized {
            return Ok(());
        }
        self.initialized = true;

        let first = self.tokenizer.next_segment()?.ok_or_else(|| {
            FormatError::Edifact("empty interchange: no UNB segment found".into())
        })?;
        let delims = self.tokenizer.delimiters();
        let parsed = split_segment(&first, &delims);
        if parsed.tag != "UNB" {
            return Err(FormatError::Edifact(format!(
                "interchange must begin with a UNB segment, found {:?}",
                parsed.tag
            )));
        }
        // Stash the full element list; the control reference is resolved
        // structurally against the UNZ echo at interchange close, not from
        // the header alone — a doubly-degenerate UNB (empty date/time slot
        // plus a date-only date/time) leaves two plausible control-reference
        // positions that only the trailer can disambiguate.
        self.unb_elements = parsed.elements;
        Ok(())
    }

    /// Pull the next service-or-body segment and advance envelope state.
    /// Returns the body record to emit, or `None` at clean interchange
    /// end. Service segments are consumed transparently (the loop
    /// continues) so the caller only ever sees body records.
    fn pull_next(&mut self) -> Result<Option<Record>, FormatError> {
        loop {
            let raw = match self.tokenizer.next_segment()? {
                Some(s) => s,
                None => {
                    if !self.interchange_closed {
                        return Err(FormatError::Edifact(
                            "interchange truncated: reached end of input with no UNZ trailer"
                                .into(),
                        ));
                    }
                    self.done = true;
                    return Ok(None);
                }
            };
            let delims = self.tokenizer.delimiters();
            let segment = split_segment(&raw, &delims);

            if self.interchange_closed {
                return Err(FormatError::Edifact(format!(
                    "segment {:?} found after the UNZ interchange trailer; \
                     content past UNZ is not permitted",
                    segment.tag
                )));
            }

            match segment.tag.as_str() {
                "UNB" => {
                    return Err(FormatError::Edifact(
                        "a second UNB segment appeared; nested or repeated interchanges \
                         in one stream are not supported"
                            .into(),
                    ));
                }
                "UNG" | "UNE" => {
                    return Err(FormatError::Edifact(format!(
                        "functional group segment {:?} is not supported; this reader \
                         handles a single UNB..UNZ interchange without UNG/UNE groups",
                        segment.tag
                    )));
                }
                "UNH" => {
                    if self.open_message.is_some() {
                        return Err(FormatError::Edifact(
                            "UNH opened a new message before the previous message's UNT \
                             trailer; messages must be UNH..UNT balanced"
                                .into(),
                        ));
                    }
                    let reference = segment.elements.first().cloned().unwrap_or_default();
                    self.open_message = Some(OpenMessage {
                        reference: reference.clone(),
                        segment_count: 1,
                    });
                    self.message_count += 1;
                    let record = self.body_record(&segment)?;
                    return Ok(Some(record));
                }
                "UNT" => {
                    self.close_message(&segment)?;
                    // UNT is a service segment — consume and continue.
                }
                "UNZ" => {
                    self.close_interchange(&segment)?;
                    // UNZ is a service segment — consume and continue to
                    // the EOF check on the next loop turn.
                }
                _ => {
                    let msg = self.open_message.as_mut().ok_or_else(|| {
                        FormatError::Edifact(format!(
                            "body segment {:?} appeared outside any UNH..UNT message",
                            segment.tag
                        ))
                    })?;
                    msg.segment_count += 1;
                    let record = self.body_record(&segment)?;
                    return Ok(Some(record));
                }
            }
        }
    }

    /// Validate and close the open message against its `UNT` trailer.
    /// `UNT` carries the message segment count (element 1, including
    /// `UNH` and `UNT`) and the message reference echo (element 2).
    fn close_message(&mut self, unt: &ParsedSegment) -> Result<(), FormatError> {
        let msg = self
            .open_message
            .take()
            .ok_or_else(|| FormatError::Edifact("UNT trailer with no open UNH message".into()))?;
        let claimed: u64 = unt
            .elements
            .first()
            .ok_or_else(|| FormatError::Edifact("UNT missing its segment-count element".into()))?
            .trim()
            .parse()
            .map_err(|_| {
                FormatError::Edifact(format!(
                    "UNT segment count {:?} is not a number",
                    unt.elements.first()
                ))
            })?;
        // The open message's running count tallied UNH + body segments;
        // the UNT segment itself completes the count.
        let actual = msg.segment_count + 1;
        if claimed != actual {
            return Err(FormatError::Edifact(format!(
                "UNT segment count mismatch for message {:?}: trailer claims {claimed}, \
                 interchange contains {actual} (UNH..UNT inclusive)",
                msg.reference
            )));
        }
        let echoed_ref = unt.elements.get(1).map(|s| s.as_str()).unwrap_or("");
        if echoed_ref != msg.reference {
            return Err(FormatError::Edifact(format!(
                "UNT message reference {echoed_ref:?} does not echo the UNH reference \
                 {:?}",
                msg.reference
            )));
        }
        Ok(())
    }

    /// Validate and close the interchange against its `UNZ` trailer.
    /// `UNZ` carries the interchange message count (element 1) and the
    /// interchange control-reference echo (element 2).
    fn close_interchange(&mut self, unz: &ParsedSegment) -> Result<(), FormatError> {
        if self.open_message.is_some() {
            return Err(FormatError::Edifact(
                "UNZ interchange trailer arrived before the last message's UNT".into(),
            ));
        }
        let claimed: u64 = unz
            .elements
            .first()
            .ok_or_else(|| FormatError::Edifact("UNZ missing its message-count element".into()))?
            .trim()
            .parse()
            .map_err(|_| {
                FormatError::Edifact(format!(
                    "UNZ message count {:?} is not a number",
                    unz.elements.first()
                ))
            })?;
        if claimed != self.message_count {
            return Err(FormatError::Edifact(format!(
                "UNZ message count mismatch: trailer claims {claimed}, interchange \
                 contains {} messages",
                self.message_count
            )));
        }
        // Validate the control-reference echo against the UNB structure
        // rather than a single pre-chosen index: the echo confirms which
        // of the plausible control-reference slots holds 0020, so a
        // doubly-degenerate header (empty date/time slot plus a date-only
        // date/time, which shifts the reference one slot) is accepted when
        // the trailer echoes the true reference, while a genuinely
        // out-of-sync trailer still fails.
        let echoed = unz.elements.get(1).map(|s| s.as_str()).unwrap_or("");
        if !unz_echo_matches_unb(&self.unb_elements, echoed) {
            return Err(FormatError::Edifact(format!(
                "UNZ control reference {echoed:?} does not echo the UNB interchange \
                 control reference"
            )));
        }
        self.interchange_closed = true;
        Ok(())
    }

    /// Map a parsed segment to a positional [`Record`] under the static
    /// schema. `seg_id`, `msg_ref`, and `msg_type` are stamped from the
    /// open message; the data elements fill `e01..`. Absent trailing
    /// elements stay `Value::Null`; an element count past `max_elements`
    /// errors with guidance.
    fn body_record(&self, segment: &ParsedSegment) -> Result<Record, FormatError> {
        if segment.elements.len() > self.max_elements {
            return Err(FormatError::Edifact(format!(
                "segment {:?} carries {} data elements, exceeding the configured \
                 max_elements of {}; raise the source's `max_elements` option",
                segment.tag,
                segment.elements.len(),
                self.max_elements
            )));
        }
        let (msg_ref, msg_type) = match (segment.tag.as_str(), &self.open_message) {
            // The UNH record's own elements carry the ref and type; the
            // open message was just created from them.
            ("UNH", _) => (
                segment.elements.first().cloned().unwrap_or_default(),
                segment.elements.get(1).cloned().unwrap_or_default(),
            ),
            (_, Some(msg)) => (msg.reference.clone(), String::new()),
            (_, None) => (String::new(), String::new()),
        };

        let mut values: Vec<Value> = Vec::with_capacity(3 + self.max_elements);
        values.push(Value::String(segment.tag.as_str().into()));
        values.push(string_or_null(&msg_ref));
        values.push(string_or_null(&msg_type));
        for i in 0..self.max_elements {
            match segment.elements.get(i) {
                Some(e) => values.push(string_or_null(e)),
                None => values.push(Value::Null),
            }
        }
        Ok(Record::new(Arc::clone(&self.schema), values))
    }
}

impl<R: Read + Send> FormatReader for EdifactReader<R> {
    fn schema(&mut self) -> Result<Arc<Schema>, FormatError> {
        Ok(Arc::clone(&self.schema))
    }

    fn next_record(&mut self) -> Result<Option<Record>, FormatError> {
        if self.done {
            return Ok(None);
        }
        self.ensure_initialized()?;
        self.pull_next()
    }

    fn prepare_document(
        &mut self,
        config: &EnvelopeConfig,
    ) -> Result<IndexMap<Box<str>, Value>, FormatError> {
        if config.is_empty() {
            return Ok(IndexMap::new());
        }
        self.ensure_initialized()?;

        let mut out: IndexMap<Box<str>, Value> = IndexMap::with_capacity(config.sections.len());
        for (name, section) in &config.sections {
            let segment_tag = match &section.extract {
                EnvelopeExtract::Segment(tag) => tag.as_str(),
                EnvelopeExtract::XmlPath(_) | EnvelopeExtract::JsonPointer(_) => {
                    return Err(FormatError::Edifact(format!(
                        "envelope section {name:?}: declared a non-`segment` extract \
                         against an EDIFACT source. Use `segment` (e.g. \
                         `extract: {{ segment: \"UNB\" }}`) for EDIFACT envelope sections."
                    )));
                }
            };
            if segment_tag != "UNB" {
                return Err(FormatError::Edifact(format!(
                    "envelope section {name:?}: segment {segment_tag:?} is not \
                     extractable. Only the interchange header `UNB` is available as \
                     an envelope section; trailer segments (UNT/UNZ) arrive after the \
                     body and are validated by the reader, not exposed as $doc fields."
                )));
            }
            let raw: Vec<(String, String)> = self
                .unb_elements
                .iter()
                .enumerate()
                .map(|(i, e)| (positional_key(i), e.clone()))
                .collect();
            let mut typed =
                coerce_section_fields(raw, &section.fields).map_err(FormatError::Edifact)?;
            // Stash the complete, ordered UNB element list (empty middle
            // elements included) under an engine-internal key so a writer
            // can reconstruct the header losslessly via
            // `interchange_from_doc`. The declared `fields` drive typed
            // CXL `$doc.<section>.<field>` access and intentionally drop
            // empty/undeclared elements; that filtered view cannot
            // round-trip a UNB whose middle elements are empty, so the
            // raw list is carried alongside it for the writer's use.
            let raw_elements: Vec<Value> = self
                .unb_elements
                .iter()
                .map(|e| Value::String(e.as_str().into()))
                .collect();
            typed.insert(Box::from(RAW_ELEMENTS_KEY), Value::Array(raw_elements));
            out.insert(Box::from(name.as_str()), Value::Map(Box::new(typed)));
        }
        Ok(out)
    }
}

/// Build the static positional schema `[seg_id, msg_ref, msg_type,
/// e01..e<max>]`. All columns are string-typed; element text is stored
/// verbatim so the round-trip is lossless.
fn build_schema(max_elements: usize) -> Arc<Schema> {
    let mut columns: Vec<Box<str>> = Vec::with_capacity(3 + max_elements);
    columns.push(Box::from("seg_id"));
    columns.push(Box::from("msg_ref"));
    columns.push(Box::from("msg_type"));
    for i in 0..max_elements {
        columns.push(positional_key(i).into_boxed_str());
    }
    Arc::new(Schema::new(columns))
}

/// Positional element column name for element index `i`: `e01`, `e02`, …
fn positional_key(i: usize) -> String {
    format!("e{:02}", i + 1)
}

/// Map an element string to a `Value`: empty text becomes `Null` so an
/// absent or blank element is uniformly null at the schema slot.
fn string_or_null(s: &str) -> Value {
    if s.is_empty() {
        Value::Null
    } else {
        Value::String(s.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::{EnvelopeFieldType, EnvelopeSection};
    use std::io::Cursor;

    fn reader(data: &str) -> EdifactReader<Cursor<Vec<u8>>> {
        EdifactReader::new(
            Cursor::new(data.as_bytes().to_vec()),
            EdifactReaderConfig::default(),
        )
    }

    /// A minimal valid single-message ORDERS interchange (no UNA;
    /// Level-A defaults). UNH..UNT covers 4 segments (UNH, BGM, NAD,
    /// UNT); UNZ claims 1 message and echoes the UNB ref `REF1`.
    const ORDERS: &str = "UNB+UNOA:1+SENDER+RECEIVER+240101:1200+REF1'\
        UNH+M1+ORDERS:D:96A:UN'\
        BGM+220+12345'\
        NAD+BY+ACME'\
        UNT+4+M1'\
        UNZ+1+REF1'";

    fn collect(data: &str) -> Vec<Record> {
        let mut r = reader(data);
        let mut out = Vec::new();
        while let Some(rec) = r.next_record().unwrap() {
            out.push(rec);
        }
        out
    }

    #[test]
    fn one_record_per_body_segment_with_positional_columns() {
        let recs = collect(ORDERS);
        // UNH, BGM, NAD are body records; UNB/UNZ/UNT are not emitted.
        assert_eq!(recs.len(), 3);
        assert_eq!(recs[0].get("seg_id"), Some(&Value::String("UNH".into())));
        assert_eq!(recs[1].get("seg_id"), Some(&Value::String("BGM".into())));
        assert_eq!(recs[1].get("e01"), Some(&Value::String("220".into())));
        assert_eq!(recs[1].get("e02"), Some(&Value::String("12345".into())));
        assert_eq!(recs[2].get("seg_id"), Some(&Value::String("NAD".into())));
    }

    #[test]
    fn unh_ref_and_type_stamped_on_every_message_record() {
        let recs = collect(ORDERS);
        for rec in &recs {
            assert_eq!(rec.get("msg_ref"), Some(&Value::String("M1".into())));
        }
        // The UNH record carries the composite message type verbatim.
        assert_eq!(
            recs[0].get("msg_type"),
            Some(&Value::String("ORDERS:D:96A:UN".into()))
        );
    }

    #[test]
    fn service_segments_never_emitted_as_body() {
        let recs = collect(ORDERS);
        for rec in &recs {
            let seg = rec.get("seg_id").unwrap();
            assert!(!matches!(seg, Value::String(s) if matches!(&**s, "UNB" | "UNZ" | "UNT")));
        }
    }

    #[test]
    fn absent_trailing_elements_are_null() {
        let recs = collect(ORDERS);
        // BGM has 2 elements; e03.. are null.
        assert_eq!(recs[1].get("e03"), Some(&Value::Null));
    }

    #[test]
    fn release_sequences_decode_into_clean_values() {
        // The NAD element carries a release-escaped apostrophe on the
        // wire (O?'BRIEN). The reader must decode it so downstream sees
        // the clean data "O'BRIEN", not the wire escape.
        let data = "UNB+UNOA:1+S+R+240101:1200+REF1'\
            UNH+M1+ORDERS:D:96A:UN'NAD+O?'BRIEN'UNT+3+M1'UNZ+1+REF1'";
        let recs = collect(data);
        let nad = &recs[1];
        assert_eq!(
            nad.get("e01"),
            Some(&Value::String("O'BRIEN".into())),
            "release sequence leaked into the parsed value"
        );
    }

    #[test]
    fn escaped_element_separator_decodes_and_does_not_split() {
        // BGM+A?+B' — the ?+ is one element "A+B", not two elements.
        let data = "UNB+UNOA:1+S+R+240101:1200+REF1'\
            UNH+M1+ORDERS:D:96A:UN'BGM+A?+B'UNT+3+M1'UNZ+1+REF1'";
        let recs = collect(data);
        let bgm = &recs[1];
        assert_eq!(bgm.get("e01"), Some(&Value::String("A+B".into())));
        assert_eq!(bgm.get("e02"), Some(&Value::Null));
    }

    #[test]
    fn unt_segment_count_mismatch_errors() {
        let data = "UNB+UNOA:1+S+R+240101:1200+REF1'\
            UNH+M1+ORDERS:D:96A:UN'BGM+220'UNT+9+M1'UNZ+1+REF1'";
        let mut r = reader(data);
        let err = loop {
            match r.next_record() {
                Ok(Some(_)) => continue,
                Ok(None) => panic!("expected count mismatch error"),
                Err(e) => break e,
            }
        };
        assert!(matches!(err, FormatError::Edifact(m) if m.contains("UNT segment count mismatch")));
    }

    #[test]
    fn unt_ref_unh_ref_mismatch_errors() {
        let data = "UNB+UNOA:1+S+R+240101:1200+REF1'\
            UNH+M1+ORDERS:D:96A:UN'BGM+220'UNT+3+WRONG'UNZ+1+REF1'";
        let mut r = reader(data);
        let err = loop {
            match r.next_record() {
                Ok(Some(_)) => continue,
                Ok(None) => panic!("expected ref mismatch error"),
                Err(e) => break e,
            }
        };
        assert!(matches!(err, FormatError::Edifact(m) if m.contains("does not echo the UNH")));
    }

    #[test]
    fn unz_message_count_mismatch_errors() {
        let data = "UNB+UNOA:1+S+R+240101:1200+REF1'\
            UNH+M1+ORDERS:D:96A:UN'BGM+220'UNT+3+M1'UNZ+5+REF1'";
        let mut r = reader(data);
        let err = loop {
            match r.next_record() {
                Ok(Some(_)) => continue,
                Ok(None) => panic!("expected message count mismatch"),
                Err(e) => break e,
            }
        };
        assert!(matches!(err, FormatError::Edifact(m) if m.contains("UNZ message count mismatch")));
    }

    #[test]
    fn unz_ref_unb_ref_mismatch_errors() {
        let data = "UNB+UNOA:1+S+R+240101:1200+REF1'\
            UNH+M1+ORDERS:D:96A:UN'BGM+220'UNT+3+M1'UNZ+1+WRONGREF'";
        let mut r = reader(data);
        let err = loop {
            match r.next_record() {
                Ok(Some(_)) => continue,
                Ok(None) => panic!("expected control ref mismatch"),
                Err(e) => break e,
            }
        };
        assert!(matches!(err, FormatError::Edifact(m) if m.contains("does not echo the UNB")));
    }

    #[test]
    fn unb_control_ref_located_past_empty_padding_element_validates() {
        // The UNB carries an empty optional element (index 4) ahead of
        // the control reference, shifting "REF1" to index 5. A fixed
        // index-4 lookup reads the empty padding and spuriously rejects
        // this internally consistent interchange (UNZ correctly echoes
        // "REF1"); the structural locator finds the real reference.
        let data = "UNB+UNOA:1+S+R+240101:1200++REF1'\
            UNH+M1+ORDERS:D:96A:UN'BGM+220'UNT+3+M1'UNZ+1+REF1'";
        let recs = collect(data);
        // One body BGM record streams; no spurious control-ref rejection.
        assert_eq!(recs.len(), 2); // UNH, BGM
        assert_eq!(recs[1].get("seg_id"), Some(&Value::String("BGM".into())));
    }

    #[test]
    fn shifted_control_ref_still_rejects_genuine_unz_mismatch() {
        // With the control reference shifted to index 5 by empty padding,
        // a UNZ that does *not* echo it must still be rejected — the
        // structural locator must not weaken the echo check.
        let data = "UNB+UNOA:1+S+R+240101:1200++REF1'\
            UNH+M1+ORDERS:D:96A:UN'BGM+220'UNT+3+M1'UNZ+1+WRONGREF'";
        let mut r = reader(data);
        let err = loop {
            match r.next_record() {
                Ok(Some(_)) => continue,
                Ok(None) => panic!("expected control ref mismatch for shifted reference"),
                Err(e) => break e,
            }
        };
        assert!(matches!(err, FormatError::Edifact(m) if m.contains("does not echo the UNB")));
    }

    #[test]
    fn doubly_degenerate_unb_resolves_control_ref_to_intended_element() {
        // A header degenerate in two ways at once: the S004 date/time slot
        // (index 3) is an empty pad AND a date-only S004 "240101" sits at
        // the canonical control-reference slot (index 4), shifting the true
        // reference "REF9" to index 5. Locating the reference by the first
        // non-empty element after the leading composites would pick the
        // date "240101"; the structural check accepts the UNZ echo of the
        // real reference at the shifted slot, so this internally consistent
        // interchange validates instead of being spuriously rejected.
        let data = "UNB+UNOA:1+SENDER+RECEIVER++240101+REF9'\
            UNH+M1+ORDERS:D:96A:UN'BGM+220'UNT+3+M1'UNZ+1+REF9'";
        let recs = collect(data);
        assert_eq!(recs.len(), 2); // UNH, BGM — no spurious rejection
        assert_eq!(recs[1].get("seg_id"), Some(&Value::String("BGM".into())));
    }

    #[test]
    fn split_date_time_unb_locates_control_ref_past_the_time_part() {
        // The UNB transmits S004 date/time as two separate elements
        // (date "240101" at index 3, time "1200" at index 4) instead of
        // the conformant single composite "240101:1200". That split pushes
        // the true control reference "REF1" to index 5. Without
        // recombining the date/time the locator would take the time "1200"
        // as the reference and spuriously reject this internally consistent
        // interchange (UNZ correctly echoes "REF1").
        let data = "UNB+UNOA:1+SENDER+RECEIVER+240101+1200+REF1'\
            UNH+M1+ORDERS:D:96A:UN'BGM+220'UNT+3+M1'UNZ+1+REF1'";
        let recs = collect(data);
        assert_eq!(recs.len(), 2); // UNH, BGM — no spurious rejection
        assert_eq!(recs[1].get("seg_id"), Some(&Value::String("BGM".into())));
    }

    #[test]
    fn split_date_time_unb_still_rejects_genuine_unz_mismatch() {
        // The split-date/time recombination must not become a wildcard: a
        // UNZ that echoes the time part "1200" instead of the true
        // reference "REF1" is still a genuine mismatch.
        let data = "UNB+UNOA:1+SENDER+RECEIVER+240101+1200+REF1'\
            UNH+M1+ORDERS:D:96A:UN'BGM+220'UNT+3+M1'UNZ+1+1200'";
        let mut r = reader(data);
        let err = loop {
            match r.next_record() {
                Ok(Some(_)) => continue,
                Ok(None) => panic!("expected control ref mismatch for split date/time header"),
                Err(e) => break e,
            }
        };
        assert!(matches!(err, FormatError::Edifact(m) if m.contains("does not echo the UNB")));
    }

    #[test]
    fn doubly_degenerate_unb_still_rejects_genuine_unz_mismatch() {
        // The doubly-degenerate shape must not become a wildcard: a UNZ
        // that echoes neither the canonical-slot date "240101" nor the
        // shifted true reference "REF9" is still a genuine mismatch.
        let data = "UNB+UNOA:1+SENDER+RECEIVER++240101+REF9'\
            UNH+M1+ORDERS:D:96A:UN'BGM+220'UNT+3+M1'UNZ+1+WRONGREF'";
        let mut r = reader(data);
        let err = loop {
            match r.next_record() {
                Ok(Some(_)) => continue,
                Ok(None) => panic!("expected control ref mismatch for doubly-degenerate header"),
                Err(e) => break e,
            }
        };
        assert!(matches!(err, FormatError::Edifact(m) if m.contains("does not echo the UNB")));
    }

    #[test]
    fn missing_unz_at_eof_errors() {
        let data = "UNB+UNOA:1+S+R+240101:1200+REF1'\
            UNH+M1+ORDERS:D:96A:UN'BGM+220'UNT+3+M1'";
        let mut r = reader(data);
        let err = loop {
            match r.next_record() {
                Ok(Some(_)) => continue,
                Ok(None) => panic!("expected truncation error"),
                Err(e) => break e,
            }
        };
        assert!(matches!(err, FormatError::Edifact(m) if m.contains("no UNZ")));
    }

    #[test]
    fn content_after_unz_errors() {
        let data = "UNB+UNOA:1+S+R+240101:1200+REF1'\
            UNH+M1+ORDERS:D:96A:UN'BGM+220'UNT+3+M1'UNZ+1+REF1'BGM+999'";
        let mut r = reader(data);
        let err = loop {
            match r.next_record() {
                Ok(Some(_)) => continue,
                Ok(None) => panic!("expected after-trailer error"),
                Err(e) => break e,
            }
        };
        assert!(matches!(err, FormatError::Edifact(m) if m.contains("after the UNZ")));
    }

    #[test]
    fn ung_une_unsupported_error() {
        let data = "UNB+UNOA:1+S+R+240101:1200+REF1'UNG+ORDERS+S+R'UNZ+0+REF1'";
        let mut r = reader(data);
        let err = loop {
            match r.next_record() {
                Ok(Some(_)) => continue,
                Ok(None) => panic!("expected UNG unsupported error"),
                Err(e) => break e,
            }
        };
        assert!(matches!(err, FormatError::Edifact(m) if m.contains("functional group")));
    }

    #[test]
    fn max_elements_overflow_errors_with_guidance() {
        let mut r = EdifactReader::new(
            Cursor::new(
                "UNB+UNOA:1+S+R+240101:1200+REF1'\
                 UNH+M1+ORDERS:D:96A:UN'BGM+a+b+c+d'UNT+3+M1'UNZ+1+REF1'"
                    .as_bytes()
                    .to_vec(),
            ),
            EdifactReaderConfig { max_elements: 2 },
        );
        let err = loop {
            match r.next_record() {
                Ok(Some(_)) => continue,
                Ok(None) => panic!("expected element overflow"),
                Err(e) => break e,
            }
        };
        assert!(matches!(err, FormatError::Edifact(m) if m.contains("max_elements")));
    }

    fn unb_section(fields: &[(&str, EnvelopeFieldType)]) -> EnvelopeConfig {
        let mut cfg = EnvelopeConfig::default();
        let mut field_map = IndexMap::new();
        for (k, ty) in fields {
            field_map.insert((*k).to_string(), *ty);
        }
        cfg.sections.insert(
            "interchange".to_string(),
            EnvelopeSection {
                extract: EnvelopeExtract::Segment("UNB".to_string()),
                fields: field_map,
            },
        );
        cfg
    }

    #[test]
    fn prepare_document_extracts_unb_positional_fields_typed() {
        let cfg = unb_section(&[
            ("e01", EnvelopeFieldType::String),
            ("e05", EnvelopeFieldType::String),
        ]);
        let mut r = reader(ORDERS);
        let sections = r.prepare_document(&cfg).unwrap();
        let interchange = match sections.get("interchange").unwrap() {
            Value::Map(m) => m,
            other => panic!("expected map, got {other:?}"),
        };
        // UNB element 1 is the syntax identifier composite "UNOA:1".
        assert_eq!(
            interchange.get("e01"),
            Some(&Value::String("UNOA:1".into()))
        );
        // UNB element 5 is the interchange control reference.
        assert_eq!(interchange.get("e05"), Some(&Value::String("REF1".into())));
        // Body still streams after the header pre-scan.
        let recs: Vec<_> = std::iter::from_fn(|| r.next_record().unwrap()).collect();
        assert_eq!(recs.len(), 3);
    }

    #[test]
    fn prepare_document_rejects_non_unb_segment() {
        let mut cfg = EnvelopeConfig::default();
        cfg.sections.insert(
            "trailer".to_string(),
            EnvelopeSection {
                extract: EnvelopeExtract::Segment("UNZ".to_string()),
                fields: IndexMap::new(),
            },
        );
        let mut r = reader(ORDERS);
        let err = r.prepare_document(&cfg).unwrap_err();
        assert!(matches!(err, FormatError::Edifact(m) if m.contains("not extractable")));
    }

    #[test]
    fn prepare_document_rejects_xml_path_and_json_pointer_extracts() {
        let mut cfg = EnvelopeConfig::default();
        cfg.sections.insert(
            "bad".to_string(),
            EnvelopeSection {
                extract: EnvelopeExtract::XmlPath("/doc".to_string()),
                fields: IndexMap::new(),
            },
        );
        let mut r = reader(ORDERS);
        let err = r.prepare_document(&cfg).unwrap_err();
        assert!(matches!(err, FormatError::Edifact(m) if m.contains("non-`segment`")));
    }

    #[test]
    fn multi_message_interchange_streams_all_bodies() {
        let data = "UNB+UNOA:1+S+R+240101:1200+REF1'\
            UNH+M1+ORDERS:D:96A:UN'BGM+220+A'UNT+3+M1'\
            UNH+M2+ORDERS:D:96A:UN'BGM+220+B'UNT+3+M2'\
            UNZ+2+REF1'";
        let recs = collect(data);
        // Two messages, each UNH + BGM = 2 body records.
        assert_eq!(recs.len(), 4);
        assert_eq!(recs[0].get("msg_ref"), Some(&Value::String("M1".into())));
        assert_eq!(recs[2].get("msg_ref"), Some(&Value::String("M2".into())));
    }

    #[test]
    fn una_present_interchange_parses() {
        let data = "UNA:+.? 'UNB+UNOA:1+S+R+240101:1200+REF1'\
            UNH+M1+ORDERS:D:96A:UN'BGM+220'UNT+3+M1'UNZ+1+REF1'";
        let recs = collect(data);
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0].get("seg_id"), Some(&Value::String("UNH".into())));
    }

    /// Full reader → `$doc` → writer → reader round-trip exercising a UNB
    /// header with an empty middle element and a release-escaped data
    /// byte — the path that the writer's `interchange_from_doc`
    /// reconstruction and release-escaping must keep faithful end to end.
    #[test]
    fn reader_doc_writer_round_trip_preserves_unb_and_escapes() {
        use crate::edifact::writer::{EdifactWriter, EdifactWriterConfig};
        use crate::envelope::EnvelopeFieldType;
        use crate::traits::FormatWriter;
        use clinker_record::{DocumentContext, DocumentId};

        // UNB element 4 (index 3, the prep date/time) is empty, so the
        // control reference "REF9" stays at the standard 5th-element
        // position (index 4); the NAD body element carries a literal
        // apostrophe that arrives release-escaped on the wire.
        let input = "UNB+UNOA:1+SENDER+RECEIVER++REF9'\
            UNH+M1+ORDERS:D:96A:UN'NAD+O?'BRIEN'UNT+3+M1'UNZ+1+REF9'";

        // 1. Read the envelope section and body records.
        let cfg = unb_section(&[("e05", EnvelopeFieldType::String)]);
        let mut r = reader(input);
        let sections = r.prepare_document(&cfg).unwrap();
        let body_recs: Vec<Record> = std::iter::from_fn(|| r.next_record().unwrap()).collect();
        assert_eq!(body_recs.len(), 2); // UNH, NAD

        // The decoded NAD value is clean (no wire escape).
        assert_eq!(
            body_recs[1].get("e01"),
            Some(&Value::String("O'BRIEN".into()))
        );

        // 2. Attach the document context and re-emit through the writer.
        let ctx = Arc::new(DocumentContext::new(
            DocumentId::next(),
            Arc::from("orders.edi"),
            sections,
        ));
        let schema = r.schema().unwrap();
        let out = {
            let mut buf = Vec::new();
            let mut w = EdifactWriter::new(
                std::io::Cursor::new(&mut buf),
                Arc::clone(&schema),
                EdifactWriterConfig {
                    interchange_from_doc: Some("interchange".into()),
                    segment_newline: false,
                    ..Default::default()
                },
            );
            for rec in &body_recs {
                let mut rec = rec.clone();
                rec.set_doc_ctx(Arc::clone(&ctx));
                w.write_record(&rec).unwrap();
            }
            w.flush().unwrap();
            String::from_utf8(buf).unwrap()
        };

        // The empty middle element and the release-escaped apostrophe both
        // survive the reconstruction.
        assert!(
            out.starts_with("UNB+UNOA:1+SENDER+RECEIVER++REF9'"),
            "{out}"
        );
        assert!(out.contains("NAD+O?'BRIEN'"), "{out}");
        assert!(out.contains("UNZ+1+REF9'"), "{out}");

        // 3. Re-read the emitted interchange: the decoded value matches.
        let recs2 = collect(&out);
        assert_eq!(recs2[1].get("e01"), Some(&Value::String("O'BRIEN".into())));
    }
}

//! SWIFT MT message reader.
//!
//! Streaming, row-at-a-time over the message-text block: each `:tag:value`
//! line of block 4 becomes one [`Record`] with a static positional schema
//! `[block, tag, value]`. The service blocks (`1` basic header, `2`
//! application header, `3` user header, `5` trailer) are consumed by the
//! reader to drive the message-level document context and serve file-level
//! `$doc` sections; they are never emitted as body records. This mirrors the
//! one-segment-one-record shape of the X12 and HL7 readers, so memory scales
//! O(1) with message size — only the block currently being framed is held.
//!
//! A SWIFT MT message is a single envelope: the service blocks are extracted
//! in a one-time pre-scan and surface as file-level `$doc` sections via
//! [`FormatReader::prepare_document`], and one message-level
//! `OpenLevel`/`CloseLevel` pair brackets the block-4 body records so the
//! driver's document-context stack stays balanced. A header-only message
//! (no block 4, or an empty block 4) still produces the balanced
//! open/close pair with no records between.
//!
//! Structural separators (braces, the leading `:`, the `-}` trailer, the
//! line breaks) are kept out of the stored field values; the writer
//! re-frames them, so the reader → writer → reader round-trip is
//! byte-faithful.

use std::io::{BufReader, Read};
use std::sync::Arc;

use clinker_record::{Record, Schema, Value};
use indexmap::IndexMap;

use crate::envelope::{EnvelopeConfig, EnvelopeEvent, EnvelopeExtract, FrameRole};
use crate::error::FormatError;
use crate::swift::tokenizer::{
    BlockTokenizer, ParsedBlock, ParsedBlock4Line, TEXT_BLOCK_ID, split_block4,
};
use crate::swift::{
    BODY_FIELD, DEFAULT_APP_HEADER_SECTION, DEFAULT_BASIC_HEADER_SECTION, DEFAULT_TRAILER_SECTION,
    DEFAULT_USER_HEADER_SECTION,
};
use crate::traits::FormatReader;

/// Default ceiling on the number of block-4 field lines a single message may
/// carry. A message with more `:tag:value` lines than this is rejected with
/// guidance rather than streaming an unbounded body. A real MT message is
/// well under this; the cap exists only as a corruption guard.
const DEFAULT_MAX_FIELDS: usize = 10_000;

/// Configuration for the SWIFT reader.
pub struct SwiftReaderConfig {
    /// Maximum number of block-4 field lines a single message may carry. A
    /// message exceeding this is rejected rather than streamed unbounded.
    pub max_fields: usize,
}

impl Default for SwiftReaderConfig {
    fn default() -> Self {
        Self {
            max_fields: DEFAULT_MAX_FIELDS,
        }
    }
}

/// Streaming SWIFT MT message reader.
///
/// Holds the block framer, the static `[block, tag, value]` schema, the
/// service blocks captured during the one-time pre-scan, and the queue of
/// envelope events the source ingest driver drains after each `next_record`.
/// The block-4 field lines are framed once at the first body pull and
/// streamed one record at a time.
pub struct SwiftReader<R: Read> {
    tokenizer: BlockTokenizer<BufReader<R>>,
    schema: Arc<Schema>,
    max_fields: usize,
    initialized: bool,
    /// Block 1 (basic header) body, captured in the pre-scan.
    basic_header: Option<String>,
    /// Block 2 (application header) body.
    app_header: Option<String>,
    /// Block 3 (user header) body, which may carry nested sub-blocks.
    user_header: Option<String>,
    /// Block 5 (trailer) body, which may carry nested sub-blocks.
    trailer: Option<String>,
    /// The block-4 field lines, framed at first body pull and drained one
    /// record per `next_record`.
    body_lines: std::collections::VecDeque<ParsedBlock4Line>,
    /// `true` once the message-level `OpenLevel` has been queued, so the
    /// matching `CloseLevel` is queued exactly once at end-of-message.
    level_open: bool,
    /// Nested-envelope events queued during the most recent `next_record`,
    /// drained by the driver via [`FormatReader::take_envelope_events`].
    pending_events: Vec<EnvelopeEvent>,
    done: bool,
}

impl<R: Read> SwiftReader<R> {
    /// Build a reader over any `Read` source. Block framing and the
    /// service-block pre-scan are deferred to the first read.
    pub fn new(reader: R, config: SwiftReaderConfig) -> Self {
        Self {
            tokenizer: BlockTokenizer::new(BufReader::new(reader)),
            schema: build_schema(),
            max_fields: config.max_fields,
            initialized: false,
            basic_header: None,
            app_header: None,
            user_header: None,
            trailer: None,
            body_lines: std::collections::VecDeque::new(),
            level_open: false,
            pending_events: Vec::new(),
            done: false,
        }
    }

    /// Frame every block of the message once: stash the service blocks
    /// (1/2/3/5) for envelope serving and split block 4 into its field lines
    /// for streaming. A SWIFT message is bounded and small, so framing all
    /// blocks up front holds only the (small) service-block bodies plus the
    /// block-4 line list — not the whole input stream. Idempotent.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::Swift`] on a malformed block (unbalanced brace,
    /// bad id, missing `-}` trailer, malformed `:tag:value` line), a repeated
    /// block id, or a block-4 line count past `max_fields`.
    fn ensure_initialized(&mut self) -> Result<(), FormatError> {
        if self.initialized {
            return Ok(());
        }
        self.initialized = true;

        while let Some(block) = self.tokenizer.read_block()? {
            self.absorb_block(block)?;
        }
        Ok(())
    }

    /// Route one framed block to its service-block slot or split it into body
    /// field lines. Rejects a repeated block id and a block-4 line count past
    /// the configured ceiling.
    fn absorb_block(&mut self, block: ParsedBlock) -> Result<(), FormatError> {
        let ParsedBlock { id, body } = block;
        match id {
            1 => set_once(&mut self.basic_header, body, 1)?,
            2 => set_once(&mut self.app_header, body, 2)?,
            3 => set_once(&mut self.user_header, body, 3)?,
            TEXT_BLOCK_ID => {
                if !self.body_lines.is_empty() {
                    return Err(FormatError::Swift(
                        "a second SWIFT block 4 appeared; a message carries at most one \
                         message-text block"
                            .into(),
                    ));
                }
                let lines = split_block4(&body)?;
                if lines.len() > self.max_fields {
                    return Err(FormatError::Swift(format!(
                        "SWIFT message text carries {} fields, exceeding the configured \
                         max_fields of {}; raise the source's `max_fields` option",
                        lines.len(),
                        self.max_fields
                    )));
                }
                self.body_lines = lines.into();
            }
            5 => set_once(&mut self.trailer, body, 5)?,
            other => {
                return Err(FormatError::Swift(format!(
                    "unsupported SWIFT block id {other}; expected one of 1, 2, 3, 4, 5"
                )));
            }
        }
        Ok(())
    }

    /// Pull the next body record, queuing the message-level `OpenLevel`
    /// before the first record and the matching `CloseLevel` once the body is
    /// drained. Returns `None` at clean end of message.
    ///
    /// The `CloseLevel` rides the terminal `Ok(None)` pull, not the final
    /// record-bearing one: the source ingest driver drains
    /// [`FormatReader::take_envelope_events`] and applies each boundary
    /// *before* it pushes the record that pull returned, so a close queued
    /// alongside the last record would be applied ahead of that record and
    /// strand it outside its own document level. Queuing on the `None` pull
    /// keeps every body record inside the message level and the close after
    /// the message's last record.
    fn pull_next(&mut self) -> Result<Option<Record>, FormatError> {
        if !self.level_open {
            // One message = one nested document level wrapping the block-4
            // records. Open it once, before the first record (or, for a
            // header-only message, immediately before the close) so the
            // driver's document stack always balances.
            // A SWIFT MT message is itself the logical document, so its
            // message level opens a fresh frame. One message per file today,
            // so this matches the file grain; marking it `NewFrame` keeps the
            // grain correct if a multi-message container is ever supported.
            self.pending_events.push(EnvelopeEvent::OpenLevel {
                sections: IndexMap::new(),
                frame: FrameRole::NewFrame,
            });
            self.level_open = true;
        }
        match self.body_lines.pop_front() {
            Some(line) => Ok(Some(self.body_record(&line))),
            None => {
                self.pending_events.push(EnvelopeEvent::CloseLevel);
                self.done = true;
                Ok(None)
            }
        }
    }

    /// Map one block-4 field line to a `[block, tag, value]` record. `block`
    /// is the constant `4` (the message-text block id), so downstream CXL can
    /// distinguish body lines from any future block-keyed record shape.
    fn body_record(&self, line: &ParsedBlock4Line) -> Record {
        let values = vec![
            Value::String(TEXT_BLOCK_ID.to_string().as_str().into()),
            Value::String(line.tag.as_str().into()),
            string_or_null(&line.value),
        ];
        Record::new(Arc::clone(&self.schema), values)
    }

    /// Build the file-level `$doc` sections from the captured service blocks,
    /// honoring the source's declared section names where given and falling
    /// back to the stable default labels otherwise.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::Swift`] when a declared section's extract rule
    /// is not a `segment` rule (an `xml_path`/`json_pointer` against a SWIFT
    /// source) or names a block the message does not carry.
    fn serve_sections(
        &self,
        config: &EnvelopeConfig,
    ) -> Result<IndexMap<Box<str>, Value>, FormatError> {
        let mut out: IndexMap<Box<str>, Value> = IndexMap::with_capacity(config.sections.len());
        for (name, section) in &config.sections {
            let block_id = match &section.extract {
                EnvelopeExtract::Segment(id) => id.as_str(),
                EnvelopeExtract::XmlPath(_)
                | EnvelopeExtract::JsonPointer(_)
                | EnvelopeExtract::RecordType(_) => {
                    return Err(FormatError::Swift(format!(
                        "envelope section {name:?}: declared a non-`segment` extract against a \
                         SWIFT source. Use `segment` with the block id (e.g. \
                         `extract: {{ segment: \"1\" }}`) for SWIFT envelope sections."
                    )));
                }
            };
            let body = self.block_body_for(block_id).ok_or_else(|| {
                FormatError::Swift(format!(
                    "envelope section {name:?}: block {block_id:?} is not a service block this \
                     message carries. Declare a section over block 1, 2, 3, or 5; block 4 is the \
                     message-text body streamed as records."
                ))
            })?;
            out.insert(Box::from(name.as_str()), block_section_value(body));
        }
        Ok(out)
    }

    /// Look up a service block's captured body by the user-facing id string,
    /// accepting either the numeric id (`"1"`) or the default section label
    /// (`"basic_header"`) so a `segment` extract can name a block either way.
    fn block_body_for(&self, id: &str) -> Option<&str> {
        match id {
            "1" | DEFAULT_BASIC_HEADER_SECTION => self.basic_header.as_deref(),
            "2" | DEFAULT_APP_HEADER_SECTION => self.app_header.as_deref(),
            "3" | DEFAULT_USER_HEADER_SECTION => self.user_header.as_deref(),
            "5" | DEFAULT_TRAILER_SECTION => self.trailer.as_deref(),
            _ => None,
        }
    }
}

impl<R: Read + Send> FormatReader for SwiftReader<R> {
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

    fn take_envelope_events(&mut self) -> Vec<EnvelopeEvent> {
        std::mem::take(&mut self.pending_events)
    }

    fn prepare_document(
        &mut self,
        config: &EnvelopeConfig,
    ) -> Result<IndexMap<Box<str>, Value>, FormatError> {
        if config.is_empty() {
            return Ok(IndexMap::new());
        }
        self.ensure_initialized()?;
        self.serve_sections(config)
    }
}

/// Set a service-block slot exactly once, rejecting a repeated block id (two
/// `{1:...}` headers in one message, for instance).
fn set_once(slot: &mut Option<String>, body: String, id: u8) -> Result<(), FormatError> {
    if slot.is_some() {
        return Err(FormatError::Swift(format!(
            "a second SWIFT block {id} appeared; each service block occurs at most once per \
             message"
        )));
    }
    *slot = Some(body);
    Ok(())
}

/// Wrap a service block's verbatim body in a single-field `$doc` section map
/// under the [`BODY_FIELD`] key. SWIFT service blocks carry free-form text
/// (a header string, nested `{sub:tag}` blocks), so the whole body is one
/// addressable field rather than positional elements.
fn block_section_value(body: &str) -> Value {
    let mut fields: IndexMap<Box<str>, Value> = IndexMap::with_capacity(1);
    fields.insert(Box::from(BODY_FIELD), string_or_null(body));
    Value::Map(Box::new(fields))
}

/// Build the static `[block, tag, value]` schema. All columns are
/// string-typed; tag and value text is stored verbatim so the round-trip is
/// lossless.
fn build_schema() -> Arc<Schema> {
    let columns: Vec<Box<str>> = vec![Box::from("block"), Box::from("tag"), Box::from("value")];
    Arc::new(Schema::new(columns))
}

/// Map a string to a `Value`: empty text becomes `Null` so an absent or blank
/// value is uniformly null at the schema slot.
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
    use crate::envelope::EnvelopeSection;
    use std::io::Cursor;

    /// A minimal single-customer-credit-transfer MT103 with all five blocks.
    const MT103: &str = "{1:F01BANKBEBBAXXX0000000000}{2:I103BANKDEFFXXXXN}\
        {3:{108:MSGREF12345}}\
        {4:\r\n:20:REFERENCE12345\r\n:23B:CRED\r\n:32A:240101USD1000,00\r\n\
        :59:/98765432\r\nJANE SMITH\r\n-}\
        {5:{CHK:1234567890AB}}";

    fn reader(data: &str) -> SwiftReader<Cursor<Vec<u8>>> {
        SwiftReader::new(
            Cursor::new(data.as_bytes().to_vec()),
            SwiftReaderConfig::default(),
        )
    }

    fn collect(data: &str) -> Vec<Record> {
        let mut r = reader(data);
        let mut out = Vec::new();
        while let Some(rec) = r.next_record().unwrap() {
            out.push(rec);
        }
        out
    }

    #[test]
    fn one_record_per_block4_tag_line() {
        let recs = collect(MT103);
        // :20:, :23B:, :32A:, :59: are the four block-4 fields.
        assert_eq!(recs.len(), 4);
        assert_eq!(recs[0].get("tag"), Some(&Value::String("20".into())));
        assert_eq!(
            recs[0].get("value"),
            Some(&Value::String("REFERENCE12345".into()))
        );
        assert_eq!(recs[2].get("tag"), Some(&Value::String("32A".into())));
        assert_eq!(
            recs[2].get("value"),
            Some(&Value::String("240101USD1000,00".into()))
        );
    }

    #[test]
    fn block_column_stamped_with_text_block_id() {
        let recs = collect(MT103);
        for rec in &recs {
            assert_eq!(rec.get("block"), Some(&Value::String("4".into())));
        }
    }

    #[test]
    fn service_blocks_never_emitted_as_records() {
        let recs = collect(MT103);
        // Only block-4 tags appear; no header/trailer text.
        for rec in &recs {
            let tag = rec.get("tag").unwrap();
            assert!(matches!(tag, Value::String(s) if !s.contains("F01")));
        }
    }

    #[test]
    fn multiline_continuation_value_folds() {
        let recs = collect(MT103);
        let f59 = recs
            .iter()
            .find(|r| matches!(r.get("tag"), Some(Value::String(s)) if &**s == "59"));
        let f59 = f59.unwrap();
        assert_eq!(
            f59.get("value"),
            Some(&Value::String("/98765432\nJANE SMITH".into()))
        );
    }

    #[test]
    fn balanced_open_close_for_message() {
        let mut r = reader(MT103);
        let mut opens = 0;
        let mut closes = 0;
        loop {
            let more = r.next_record().unwrap().is_some();
            for ev in r.take_envelope_events() {
                match ev {
                    EnvelopeEvent::OpenLevel { .. } => opens += 1,
                    EnvelopeEvent::CloseLevel => closes += 1,
                }
            }
            if !more {
                break;
            }
        }
        assert_eq!(opens, 1, "one message-level OpenLevel");
        assert_eq!(closes, 1, "one matching CloseLevel");
    }

    #[test]
    fn header_only_message_still_balances() {
        // No block 4 at all — a header/trailer-only message must still emit a
        // balanced OpenLevel/CloseLevel pair with no records between.
        let data = "{1:F01BANKBEBBAXXX0000000000}{2:I103BANKDEFFXXXXN}{5:{CHK:ABC}}";
        let mut r = reader(data);
        assert!(r.next_record().unwrap().is_none(), "no body records");
        let events = r.take_envelope_events();
        let opens = events
            .iter()
            .filter(|e| matches!(e, EnvelopeEvent::OpenLevel { .. }))
            .count();
        let closes = events
            .iter()
            .filter(|e| matches!(e, EnvelopeEvent::CloseLevel))
            .count();
        assert_eq!(opens, 1);
        assert_eq!(closes, 1);
    }

    #[test]
    fn prepare_document_serves_service_blocks_by_block_id() {
        let mut cfg = EnvelopeConfig::default();
        cfg.sections.insert(
            "basic".to_string(),
            EnvelopeSection {
                extract: EnvelopeExtract::Segment("1".to_string()),
                fields: IndexMap::new(),
            },
        );
        cfg.sections.insert(
            "user".to_string(),
            EnvelopeSection {
                extract: EnvelopeExtract::Segment("3".to_string()),
                fields: IndexMap::new(),
            },
        );
        let mut r = reader(MT103);
        let sections = r.prepare_document(&cfg).unwrap();
        let basic = match sections.get("basic").unwrap() {
            Value::Map(m) => m,
            other => panic!("expected map, got {other:?}"),
        };
        assert_eq!(
            basic.get("body"),
            Some(&Value::String("F01BANKBEBBAXXX0000000000".into()))
        );
        let user = match sections.get("user").unwrap() {
            Value::Map(m) => m,
            other => panic!("expected map, got {other:?}"),
        };
        // Block 3 keeps its nested sub-block verbatim in the body field.
        assert_eq!(
            user.get("body"),
            Some(&Value::String("{108:MSGREF12345}".into()))
        );
        // The body still streams after the pre-scan.
        let recs: Vec<_> = std::iter::from_fn(|| r.next_record().unwrap()).collect();
        assert_eq!(recs.len(), 4);
    }

    #[test]
    fn prepare_document_accepts_default_section_label_id() {
        // A `segment: "trailer"` extract resolves block 5 by its default
        // label, not just its numeric id.
        let mut cfg = EnvelopeConfig::default();
        cfg.sections.insert(
            "tlr".to_string(),
            EnvelopeSection {
                extract: EnvelopeExtract::Segment("trailer".to_string()),
                fields: IndexMap::new(),
            },
        );
        let mut r = reader(MT103);
        let sections = r.prepare_document(&cfg).unwrap();
        let tlr = match sections.get("tlr").unwrap() {
            Value::Map(m) => m,
            other => panic!("expected map, got {other:?}"),
        };
        assert_eq!(
            tlr.get("body"),
            Some(&Value::String("{CHK:1234567890AB}".into()))
        );
    }

    #[test]
    fn prepare_document_rejects_block4_section() {
        let mut cfg = EnvelopeConfig::default();
        cfg.sections.insert(
            "bad".to_string(),
            EnvelopeSection {
                extract: EnvelopeExtract::Segment("4".to_string()),
                fields: IndexMap::new(),
            },
        );
        let mut r = reader(MT103);
        let err = r.prepare_document(&cfg).unwrap_err();
        assert!(matches!(err, FormatError::Swift(m) if m.contains("not a service block")));
    }

    #[test]
    fn prepare_document_rejects_xml_path_extract() {
        let mut cfg = EnvelopeConfig::default();
        cfg.sections.insert(
            "bad".to_string(),
            EnvelopeSection {
                extract: EnvelopeExtract::XmlPath("/doc".to_string()),
                fields: IndexMap::new(),
            },
        );
        let mut r = reader(MT103);
        let err = r.prepare_document(&cfg).unwrap_err();
        assert!(matches!(err, FormatError::Swift(m) if m.contains("non-`segment`")));
    }

    #[test]
    fn unbalanced_brace_errors_cleanly() {
        // Block 2 never closes; the framer surfaces a truncation error rather
        // than panicking.
        let data = "{1:F01X}{2:I103BANKDEFFXXXXN";
        let mut r = reader(data);
        let err = loop {
            match r.next_record() {
                Ok(Some(_)) => continue,
                Ok(None) => panic!("expected a truncation error"),
                Err(e) => break e,
            }
        };
        assert!(matches!(err, FormatError::Swift(m) if m.contains("truncated")));
    }

    #[test]
    fn missing_block4_trailer_errors_cleanly() {
        let data = "{1:F01X}{4:\r\n:20:REF\r\n";
        let mut r = reader(data);
        let err = loop {
            match r.next_record() {
                Ok(Some(_)) => continue,
                Ok(None) => panic!("expected a truncation error"),
                Err(e) => break e,
            }
        };
        assert!(matches!(err, FormatError::Swift(m) if m.contains("truncated")));
    }

    #[test]
    fn repeated_block_id_errors() {
        let data = "{1:F01X}{1:F01Y}{4:\r\n:20:REF\r\n-}";
        let mut r = reader(data);
        let err = loop {
            match r.next_record() {
                Ok(Some(_)) => continue,
                Ok(None) => panic!("expected a repeated-block error"),
                Err(e) => break e,
            }
        };
        assert!(matches!(err, FormatError::Swift(m) if m.contains("second SWIFT block 1")));
    }

    /// An MT940 statement carries repeated `:61:` / `:86:` block-4 tags — one
    /// record per statement line. The reader streams every repeat in order.
    #[test]
    fn mt940_repeated_tags_stream_as_records() {
        let mt940 = "{1:F01BANKBEBBAXXX0000000000}{2:I940BANKDEFFXXXXN}\
            {4:\r\n:20:STMT001\r\n:25:12345678\r\n:28C:1/1\r\n\
            :60F:C240101USD5000,00\r\n\
            :61:2401010101D100,00NTRFREF1//BANK1\r\n:86:PAYMENT ONE\r\n\
            :61:2401020102C200,00NTRFREF2//BANK2\r\n:86:PAYMENT TWO\r\n\
            :62F:C240102USD5100,00\r\n-}";
        let recs = collect(mt940);
        let tags: Vec<String> = recs
            .iter()
            .filter_map(|r| match r.get("tag") {
                Some(Value::String(s)) => Some(s.to_string()),
                _ => None,
            })
            .collect();
        assert_eq!(
            tags,
            vec!["20", "25", "28C", "60F", "61", "86", "61", "86", "62F"]
        );
        // Both :61: lines stream with their distinct values.
        let sixty_ones: Vec<&Record> = recs
            .iter()
            .filter(|r| matches!(r.get("tag"), Some(Value::String(s)) if &**s == "61"))
            .collect();
        assert_eq!(sixty_ones.len(), 2);
        assert_eq!(
            sixty_ones[0].get("value"),
            Some(&Value::String("2401010101D100,00NTRFREF1//BANK1".into()))
        );
        assert_eq!(
            sixty_ones[1].get("value"),
            Some(&Value::String("2401020102C200,00NTRFREF2//BANK2".into()))
        );
    }
}

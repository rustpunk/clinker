//! SWIFT MT message writer.
//!
//! Reconstructs a single SWIFT MT envelope around emitted block-4 records:
//! the service blocks 1/2/3 (echoed from `$doc` sections or written from
//! literal config), then block 4 (`{4:` … `-}`) framing the `:tag:value`
//! lines re-emitted from the record stream, then the optional block 5
//! trailer. The writer inverts the reader exactly — the reader strips the
//! structural separators (braces, the leading `:`, the `-}` trailer, the
//! line breaks) and keeps every other byte; the writer re-adds exactly those
//! separators and nothing else. Block-4 free text is opaque and has no escape
//! mechanism, so values are written verbatim, making the
//! reader → writer → reader round-trip byte-faithful even for values that
//! carry `{`, `}`, or a mid-line `-}` as literal data.
//!
//! Verbatim emission is faithful for any value the reader can produce. The
//! few shapes that unescaped block-4 text cannot represent — a folded
//! continuation line beginning with the line-anchored `-}` terminator or with
//! a `:` tag marker — only arise when a value is built from arbitrary records
//! (CSV/JSON → Transform → SWIFT), never from the reader. Rather than emit
//! silently-corrupt output, the writer rejects such a value with a clear
//! error (see `reject_unrepresentable_value`).
//!
//! Record columns map by name: `block`, `tag`, `value`. Only `tag`/`value`
//! become block-4 lines; `block` is the constant `4` discriminator (a record
//! whose `block` is not `4` is rejected, since service blocks never arrive as
//! records — they ride the document context).
//!
//! Memory model: streaming and O(1) in held state — only the open/finalized
//! flags are retained, never a buffered message. A SWIFT message is a single
//! indivisible envelope, so `flush` is the sole end-of-stream finalizer: it
//! closes block 4 and writes the optional trailer exactly once. Byte-limit
//! file splitting (which would flush — and thus finalize — mid-stream) is
//! rejected for SWIFT outputs at config-validation time, so this writer is
//! only ever flushed at true end of stream.

use std::io::Write;
use std::sync::Arc;

use clinker_record::{Record, Schema, Value};

use crate::error::FormatError;
use crate::swift::BODY_FIELD;
use crate::swift::tokenizer::TEXT_BLOCK_ID;
use crate::traits::FormatWriter;

/// One service block's source: a literal config body wins; otherwise the
/// body is echoed from the named `$doc` section; otherwise the block is
/// omitted.
struct ServiceBlock {
    /// The numeric block id (`1`, `2`, `3`, `5`).
    id: u8,
    /// Literal block body written verbatim. Takes precedence over `from_doc`.
    literal: Option<String>,
    /// Name of a `$doc` section to echo the block body from (read under the
    /// shared `body` field). Used only when `literal` is unset.
    from_doc: Option<String>,
}

/// Configuration for the SWIFT writer.
///
/// Each service block (1/2/3/5) is either written from a literal body, echoed
/// from a user-declared `$doc` section, or omitted. The `*_from_doc` options
/// name the section the user declared on the source — never an engine
/// built-in; the engine reserves no section name.
#[derive(Clone, Default)]
pub struct SwiftWriterConfig {
    /// Literal block-1 (basic header) body. Takes precedence over
    /// `basic_header_from_doc`.
    pub basic_header: Option<String>,
    /// Name of a `$doc` section to echo the block-1 body from.
    pub basic_header_from_doc: Option<String>,
    /// Literal block-2 (application header) body. Takes precedence over
    /// `app_header_from_doc`.
    pub app_header: Option<String>,
    /// Name of a `$doc` section to echo the block-2 body from.
    pub app_header_from_doc: Option<String>,
    /// Literal block-3 (user header) body. Takes precedence over
    /// `user_header_from_doc`.
    pub user_header: Option<String>,
    /// Name of a `$doc` section to echo the block-3 body from.
    pub user_header_from_doc: Option<String>,
    /// Literal block-5 (trailer) body. Takes precedence over
    /// `trailer_from_doc`.
    pub trailer: Option<String>,
    /// Name of a `$doc` section to echo the block-5 body from.
    pub trailer_from_doc: Option<String>,
}

/// Streaming SWIFT MT message writer.
///
/// Holds the resolved `block`/`tag`/`value` column indices, the service-block
/// configuration, and the open/finalized flags. Block-4 lines stream one
/// record at a time; the envelope is opened on the first record and closed
/// by `flush`.
pub struct SwiftWriter<W: Write> {
    writer: W,
    config: SwiftWriterConfig,
    block_idx: Option<usize>,
    tag_idx: Option<usize>,
    value_idx: Option<usize>,
    /// `true` once the headers and the `{4:` opener have been written, so
    /// later records skip straight to their `:tag:value` line.
    header_written: bool,
    /// The resolved block-5 trailer body, captured during the header phase
    /// (when the first record's document context is available) and written by
    /// `flush` after block 4 closes. `None` when no trailer is configured. A
    /// `trailer_from_doc` echo must read the document context, which only a
    /// record carries — so the trailer is resolved up front, not at flush.
    trailer_body: Option<String>,
    /// `true` once `flush` has closed block 4 and written the trailer, so a
    /// repeat `flush` is a no-op.
    finalized: bool,
}

impl<W: Write> SwiftWriter<W> {
    /// Build a writer over a sink with the given schema and config. The
    /// `block`, `tag`, and `value` columns are resolved to positional indices
    /// once; the body lines stream one record at a time thereafter.
    pub fn new(writer: W, schema: Arc<Schema>, config: SwiftWriterConfig) -> Self {
        let mut block_idx = None;
        let mut tag_idx = None;
        let mut value_idx = None;
        for (i, col) in schema.columns().iter().enumerate() {
            match &**col {
                "block" => block_idx = Some(i),
                "tag" => tag_idx = Some(i),
                "value" => value_idx = Some(i),
                _ => {}
            }
        }
        Self {
            writer,
            config,
            block_idx,
            tag_idx,
            value_idx,
            header_written: false,
            trailer_body: None,
            finalized: false,
        }
    }

    /// Re-emit the service blocks 1/2/3 in order and open block 4 on the first
    /// record. Each service block is `{<id>:<body>}` with its body verbatim
    /// (block 3's nested `{108:...}` re-emitted byte-for-byte, no escaping);
    /// an omitted block writes nothing. Block 4 is opened with `{4:` and the
    /// `\r\n` that frames its first line, mirroring the reader's framing.
    fn write_header(&mut self, record: &Record) -> Result<(), FormatError> {
        if self.header_written {
            return Ok(());
        }
        self.header_written = true;

        for block in self.service_blocks_1_2_3() {
            if let Some(body) = self.resolve_block_body(&block, record)? {
                self.write_service_block(block.id, &body)?;
            }
        }
        // Resolve the trailer now, while the record's document context is in
        // hand; `flush` writes the stashed body after block 4 closes (it has
        // no record to echo a `trailer_from_doc` section from).
        self.trailer_body = self.resolve_block_body(&self.trailer_block(), record)?;
        // Open the message-text block. `{4:` then the framing `\r\n`, so the
        // first `:tag:value` line begins its own line and the closing `-}`
        // trailer the writer appends at flush is line-anchored.
        self.writer.write_all(b"{4:\r\n").map_err(FormatError::Io)?;
        Ok(())
    }

    /// The service blocks the header phase emits, in wire order (1, 2, 3).
    /// Block 5 (the trailer) is written by `flush`, after block 4 closes.
    fn service_blocks_1_2_3(&self) -> [ServiceBlock; 3] {
        [
            ServiceBlock {
                id: 1,
                literal: self.config.basic_header.clone(),
                from_doc: self.config.basic_header_from_doc.clone(),
            },
            ServiceBlock {
                id: 2,
                literal: self.config.app_header.clone(),
                from_doc: self.config.app_header_from_doc.clone(),
            },
            ServiceBlock {
                id: 3,
                literal: self.config.user_header.clone(),
                from_doc: self.config.user_header_from_doc.clone(),
            },
        ]
    }

    /// The block-5 trailer source (literal or echoed from `$doc`).
    fn trailer_block(&self) -> ServiceBlock {
        ServiceBlock {
            id: 5,
            literal: self.config.trailer.clone(),
            from_doc: self.config.trailer_from_doc.clone(),
        }
    }

    /// Resolve a service block's body: the literal config wins; otherwise echo
    /// the body verbatim from the named `$doc` section under the shared `body`
    /// field; otherwise `None` (the block is omitted).
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::Swift`] when `from_doc` names a section the
    /// record's document context does not carry — a writer pointed at a
    /// section the source never declared.
    fn resolve_block_body(
        &self,
        block: &ServiceBlock,
        record: &Record,
    ) -> Result<Option<String>, FormatError> {
        if let Some(literal) = &block.literal {
            return Ok(Some(literal.clone()));
        }
        let Some(section) = &block.from_doc else {
            return Ok(None);
        };
        let ctx = record.doc_ctx();
        let value = ctx.get_section_field(section, BODY_FIELD).ok_or_else(|| {
            FormatError::Swift(format!(
                "block {id} echo names `$doc` section {section:?}, but the record's document \
                 context carries no `{BODY_FIELD}` field for it. Ensure the source declares an \
                 envelope section of the same name over block {id} (e.g. \
                 `extract: {{ segment: \"{id}\" }}`).",
                id = block.id,
            ))
        })?;
        Ok(Some(value_to_field(
            &value,
            &format!("{section}.{BODY_FIELD}"),
        )?))
    }

    /// Write one service block as `{<id>:<body>}` with the body verbatim.
    /// SWIFT block bodies (including block 3/5 nested `{sub:tag}` sub-blocks)
    /// are opaque, so no byte is escaped — the reader kept them verbatim and
    /// the writer re-frames them verbatim.
    fn write_service_block(&mut self, id: u8, body: &str) -> Result<(), FormatError> {
        self.writer.write_all(b"{").map_err(FormatError::Io)?;
        self.writer
            .write_all(id.to_string().as_bytes())
            .map_err(FormatError::Io)?;
        self.writer.write_all(b":").map_err(FormatError::Io)?;
        self.writer
            .write_all(body.as_bytes())
            .map_err(FormatError::Io)?;
        self.writer.write_all(b"}").map_err(FormatError::Io)?;
        Ok(())
    }

    /// Re-emit one block-4 record as a `:tag:value\r\n` line. A record whose
    /// `block` column is not the constant `4` is rejected — service blocks
    /// ride the document context, never the record stream. The value is
    /// written verbatim — interior braces and a mid-line `-}` reproduce as
    /// data — *unless* the shape is unrepresentable in unescaped block-4 free
    /// text (see [`reject_unrepresentable_value`]), in which case the record
    /// is rejected rather than emitted as silently-corrupt output.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::Swift`] when the record carries no `tag`, a
    /// `block` value other than `4`, a `value` whose continuation line begins
    /// with the block terminator (`-}`) or a tag marker (`:`), or
    /// [`FormatError::UnserializableMapValue`] when `tag`/`value` hold a
    /// `Value::Map`/`Value::Array`.
    fn write_body_record(&mut self, record: &Record) -> Result<(), FormatError> {
        let values = record.values();

        if let Some(idx) = self.block_idx
            && let Some(v) = values.get(idx)
        {
            let block = value_to_field(v, "block")?;
            // An empty/null `block` is treated as the message-text block —
            // a Transform that projects only `tag`/`value` need not restamp
            // the discriminator the reader set. A non-empty `block` must parse
            // to the text-block id; comparing the parsed number ties the check
            // to TEXT_BLOCK_ID without allocating its string form per record.
            if !block.is_empty() && block.parse::<u8>() != Ok(TEXT_BLOCK_ID) {
                return Err(FormatError::Swift(format!(
                    "record carries block {block:?}, but the SWIFT writer emits only block-4 \
                     message-text lines as records (block \"4\"). Service blocks (1/2/3/5) are \
                     written from the writer's header/trailer options or echoed from `$doc`, not \
                     from the record stream."
                )));
            }
        }

        let tag = match self.tag_idx.and_then(|i| values.get(i)) {
            Some(v) => value_to_field(v, "tag")?,
            None => String::new(),
        };
        if tag.is_empty() {
            return Err(FormatError::Swift(
                "record carries no `tag`; every SWIFT block-4 line needs a field tag to write \
                 (`:tag:value`)"
                    .into(),
            ));
        }
        let value = match self.value_idx.and_then(|i| values.get(i)) {
            Some(v) => value_to_field(v, "value")?,
            None => String::new(),
        };
        reject_unrepresentable_value(&tag, &value)?;

        self.writer.write_all(b":").map_err(FormatError::Io)?;
        self.writer
            .write_all(tag.as_bytes())
            .map_err(FormatError::Io)?;
        self.writer.write_all(b":").map_err(FormatError::Io)?;
        self.writer
            .write_all(value.as_bytes())
            .map_err(FormatError::Io)?;
        self.writer.write_all(b"\r\n").map_err(FormatError::Io)?;
        Ok(())
    }
}

impl<W: Write + Send> FormatWriter for SwiftWriter<W> {
    fn write_record(&mut self, record: &Record) -> Result<(), FormatError> {
        self.write_header(record)?;
        self.write_body_record(record)
    }

    fn flush(&mut self) -> Result<(), FormatError> {
        if self.finalized {
            return Ok(());
        }
        self.finalized = true;
        // A header-only output (no records) still frames a valid empty
        // message: open block 4 so the `-}` trailer below closes a real
        // block. The first-record header phase is skipped when no record
        // arrived, so open it here from no record context — only the
        // literal-config service blocks can appear in that case.
        if !self.header_written {
            self.header_written = true;
            for block in self.service_blocks_1_2_3() {
                if let Some(body) = block.literal {
                    self.write_service_block(block.id, &body)?;
                }
            }
            // No record means no document context, so only a literal trailer
            // can frame block 5 in a zero-record output.
            self.trailer_body = self.config.trailer.clone();
            self.writer.write_all(b"{4:\r\n").map_err(FormatError::Io)?;
        }
        // Close block 4 with the line-anchored `-}` trailer: the last body
        // line ended with `\r\n`, so `-}` begins its own line and the reader
        // recognizes it as the trailer rather than data.
        self.writer.write_all(b"-}").map_err(FormatError::Io)?;
        // Re-emit the trailer (block 5) after block 4 closes from the body
        // resolved during the header phase (literal config or a
        // `trailer_from_doc` echo of the first record's document context).
        if let Some(body) = self.trailer_body.take() {
            self.write_service_block(5, &body)?;
        }
        self.writer.flush().map_err(FormatError::Io)?;
        Ok(())
    }

    /// Drain the underlying sink without closing block 4 or emitting the block-5
    /// trailer, so byte-limit split accounting stays non-finalizing. Plan
    /// validation rejects byte splitting for this single-message format; this
    /// keeps the trait contract honest regardless, with the trailers written
    /// only by [`Self::flush`].
    fn flush_bytes(&mut self) -> Result<(), FormatError> {
        self.writer.flush().map_err(FormatError::Io)
    }
}

/// Reject a block-4 value whose folded continuation lines cannot be framed
/// faithfully in unescaped block-4 free text.
///
/// Block 4 has no escape mechanism, so the writer emits values verbatim. A
/// value written as `:tag:value\r\n` splits back on its interior `\n` line
/// breaks on re-read: each continuation line (a line after the first, which
/// rides directly after `:tag:`) becomes its own physical block-4 line. A
/// continuation line that begins with `-}` re-reads as the line-anchored
/// block-4 terminator (closing the block early — truncation), and one that
/// begins with `:` re-reads as a spurious new `:tag:` field. The reader can
/// never *produce* such a value, so a reader → writer round-trip is safe; but
/// a value built from arbitrary records (CSV/JSON → Transform → SWIFT) can,
/// and emitting it verbatim would write silently-corrupt output. Fail loud
/// instead.
///
/// The first line of the value is exempt: it is preceded by the `:tag:`
/// marker on the wire, so it can never be mistaken for a fresh tag or the
/// terminator.
///
/// # Errors
///
/// Returns [`FormatError::Swift`] naming the offending continuation line.
fn reject_unrepresentable_value(tag: &str, value: &str) -> Result<(), FormatError> {
    for line in value.split('\n').skip(1) {
        let starts_with_trailer = line.starts_with("-}");
        if starts_with_trailer || line.starts_with(':') {
            let marker = if starts_with_trailer {
                "the block terminator `-}`"
            } else {
                "a tag marker `:`"
            };
            return Err(FormatError::Swift(format!(
                "SWIFT block-4 field `:{tag}:` has a continuation line {line:?} beginning with \
                 {marker}. Block-4 free text has no escape mechanism, so a value line that starts \
                 with `-}}` (the block terminator) or `:` (a field-tag marker) cannot be written \
                 back faithfully — it would re-read as an early block close or a spurious field. \
                 Rewrite the value so no continuation line begins with `-}}` or `:`."
            )));
        }
    }
    Ok(())
}

/// Render a `Value` to SWIFT field text. `Null` renders as the empty string;
/// scalars render via their natural string form. A `Value::Map` or
/// `Value::Array` has no scalar SWIFT representation, so it raises rather than
/// silently emitting an empty field — mirroring the other formats' explicit
/// posture for non-scalar payloads.
fn value_to_field(value: &Value, column: &str) -> Result<String, FormatError> {
    let text = match value {
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Integer(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Decimal(d) => d.to_string(),
        Value::String(s) => s.to_string(),
        Value::Date(d) => d.to_string(),
        Value::DateTime(dt) => dt.to_string(),
        Value::Map(_) | Value::Array(_) => {
            return Err(FormatError::UnserializableMapValue {
                format: "swift",
                column: column.to_string(),
            });
        }
    };
    Ok(text)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swift::{
        DEFAULT_APP_HEADER_SECTION, DEFAULT_BASIC_HEADER_SECTION, DEFAULT_TRAILER_SECTION,
        DEFAULT_USER_HEADER_SECTION,
    };
    use clinker_record::{DocumentContext, DocumentId, EnvelopeRecord};
    use indexmap::IndexMap;
    use std::io::Cursor;

    /// A `[block, tag, value]` schema, the one the reader emits.
    fn schema() -> Arc<Schema> {
        let cols: Vec<Box<str>> = vec!["block".into(), "tag".into(), "value".into()];
        Arc::new(Schema::new(cols))
    }

    fn record(schema: &Arc<Schema>, block: &str, tag: &str, value: &str) -> Record {
        let values = vec![
            Value::String(block.into()),
            Value::String(tag.into()),
            Value::String(value.into()),
        ];
        Record::new(Arc::clone(schema), values)
    }

    fn write_all(config: SwiftWriterConfig, records: &[Record], schema: &Arc<Schema>) -> String {
        let mut buf = Vec::new();
        let mut w = SwiftWriter::new(Cursor::new(&mut buf), Arc::clone(schema), config);
        for r in records {
            w.write_record(r).unwrap();
        }
        w.flush().unwrap();
        String::from_utf8(buf).unwrap()
    }

    /// Build a document context exposing each named service-block section
    /// under the shared `body` field, the shape the reader produces.
    fn doc_ctx(sections: &[(&str, &str)]) -> Arc<DocumentContext> {
        let mut out: IndexMap<Box<str>, Value> = IndexMap::new();
        for (name, body) in sections {
            let mut fields: IndexMap<Box<str>, Value> = IndexMap::new();
            fields.insert(BODY_FIELD.into(), Value::String((*body).into()));
            out.insert(Box::from(*name), Value::Map(Box::new(fields)));
        }
        Arc::new(DocumentContext::new(
            DocumentId::next(),
            Arc::from("a.swift"),
            EnvelopeRecord::from_sections(out),
        ))
    }

    #[test]
    fn block4_tag_line_reconstructed() {
        let s = schema();
        let rec = record(&s, "4", "20", "REFERENCE12345");
        let out = write_all(SwiftWriterConfig::default(), &[rec], &s);
        assert!(out.contains(":20:REFERENCE12345\r\n"), "{out}");
        // The message-text block opens and closes around the line.
        assert!(out.starts_with("{4:\r\n"), "{out}");
        assert!(out.ends_with("-}"), "{out}");
    }

    #[test]
    fn multiline_value_reproduces_continuation() {
        // A folded continuation value (`/12345678\nJOHN DOE`) re-emits with the
        // interior line break intact so a re-read folds it back identically.
        let s = schema();
        let rec = record(&s, "4", "50K", "/12345678\nJOHN DOE");
        let out = write_all(SwiftWriterConfig::default(), &[rec], &s);
        assert!(out.contains(":50K:/12345678\nJOHN DOE\r\n"), "{out}");
    }

    #[test]
    fn interior_blank_line_in_value_survives() {
        let s = schema();
        let rec = record(&s, "4", "77E", "LINE1\n\nLINE3");
        let out = write_all(SwiftWriterConfig::default(), &[rec], &s);
        assert!(out.contains(":77E:LINE1\n\nLINE3\r\n"), "{out}");
    }

    #[test]
    fn interior_brace_in_value_written_verbatim() {
        // Block-4 free text is opaque — a `}` or `{` in a value is data,
        // written with zero escaping.
        let s = schema();
        let recs = [
            record(&s, "4", "79", "REF}WITHBRACE"),
            record(&s, "4", "86", "A{B"),
        ];
        let out = write_all(SwiftWriterConfig::default(), &recs, &s);
        assert!(out.contains(":79:REF}WITHBRACE\r\n"), "{out}");
        assert!(out.contains(":86:A{B\r\n"), "{out}");
    }

    #[test]
    fn block4_trailer_line_anchored() {
        // The `-}` trailer begins its own line: the last body line ends with
        // `\r\n`, so `-}` is preceded by a line break, not mid-line.
        let s = schema();
        let rec = record(&s, "4", "20", "REF");
        let out = write_all(SwiftWriterConfig::default(), &[rec], &s);
        assert!(out.ends_with(":20:REF\r\n-}"), "{out}");
    }

    #[test]
    fn service_blocks_echoed_from_doc() {
        // A record carrying a document context with the four service-block
        // sections frames `{1:}{2:}{3:}` + block 4 + `{5:}` in order; block 3's
        // nested sub-block re-emits verbatim.
        let s = schema();
        let mut rec = record(&s, "4", "20", "REF");
        rec.set_doc_ctx(doc_ctx(&[
            (DEFAULT_BASIC_HEADER_SECTION, "F01BANKBEBBAXXX0000000000"),
            (DEFAULT_APP_HEADER_SECTION, "I103BANKDEFFXXXXN"),
            (DEFAULT_USER_HEADER_SECTION, "{108:MSGREF12345}"),
            (DEFAULT_TRAILER_SECTION, "{CHK:1234567890AB}"),
        ]));
        let config = SwiftWriterConfig {
            basic_header_from_doc: Some(DEFAULT_BASIC_HEADER_SECTION.into()),
            app_header_from_doc: Some(DEFAULT_APP_HEADER_SECTION.into()),
            user_header_from_doc: Some(DEFAULT_USER_HEADER_SECTION.into()),
            trailer_from_doc: Some(DEFAULT_TRAILER_SECTION.into()),
            ..Default::default()
        };
        let out = write_all(config, &[rec], &s);
        let expected = "{1:F01BANKBEBBAXXX0000000000}{2:I103BANKDEFFXXXXN}\
            {3:{108:MSGREF12345}}{4:\r\n:20:REF\r\n-}{5:{CHK:1234567890AB}}";
        assert_eq!(out, expected, "framing wrong");
    }

    #[test]
    fn literal_header_wins_over_doc() {
        let s = schema();
        let mut rec = record(&s, "4", "20", "REF");
        rec.set_doc_ctx(doc_ctx(&[(DEFAULT_BASIC_HEADER_SECTION, "FROMDOC")]));
        let config = SwiftWriterConfig {
            basic_header: Some("LITERAL1".into()),
            basic_header_from_doc: Some(DEFAULT_BASIC_HEADER_SECTION.into()),
            ..Default::default()
        };
        let out = write_all(config, &[rec], &s);
        assert!(out.starts_with("{1:LITERAL1}{4:"), "{out}");
        assert!(!out.contains("FROMDOC"), "{out}");
    }

    #[test]
    fn flush_idempotent() {
        let s = schema();
        let rec = record(&s, "4", "20", "REF");
        let mut buf = Vec::new();
        let mut w = SwiftWriter::new(
            Cursor::new(&mut buf),
            Arc::clone(&s),
            SwiftWriterConfig {
                trailer: Some("{CHK:AB}".into()),
                ..Default::default()
            },
        );
        w.write_record(&rec).unwrap();
        w.flush().unwrap();
        w.flush().unwrap();
        let out = String::from_utf8(buf).unwrap();
        assert_eq!(out.matches("-}").count(), 1, "trailer written twice: {out}");
        assert_eq!(
            out.matches("{5:").count(),
            1,
            "block 5 written twice: {out}"
        );
    }

    #[test]
    fn non_block4_record_errors() {
        let s = schema();
        let rec = record(&s, "1", "20", "REF");
        let mut buf = Vec::new();
        let mut w = SwiftWriter::new(
            Cursor::new(&mut buf),
            Arc::clone(&s),
            SwiftWriterConfig::default(),
        );
        let err = w.write_record(&rec).unwrap_err();
        assert!(
            matches!(&err, FormatError::Swift(m) if m.contains("block-4")),
            "expected a block-4 rejection, got: {err:?}"
        );
    }

    #[test]
    fn empty_block_column_defaults_to_block4() {
        // A Transform that projects only tag/value (no block restamp) writes
        // its lines as block-4 fields rather than being rejected.
        let s = schema();
        let rec = record(&s, "", "20", "REF");
        let out = write_all(SwiftWriterConfig::default(), &[rec], &s);
        assert!(out.contains(":20:REF\r\n"), "{out}");
    }

    #[test]
    fn continuation_line_starting_with_trailer_errors() {
        // A folded continuation line beginning with the line-anchored `-}`
        // would re-read as an early block close — unrepresentable in
        // unescaped block-4 text, so the writer fails loud rather than
        // emitting truncated output. Such a value can only come from an
        // arbitrary-record source, never the reader.
        let s = schema();
        let rec = record(&s, "4", "77E", "LINE1\n-}EVIL");
        let mut buf = Vec::new();
        let mut w = SwiftWriter::new(
            Cursor::new(&mut buf),
            Arc::clone(&s),
            SwiftWriterConfig::default(),
        );
        let err = w.write_record(&rec).unwrap_err();
        assert!(
            matches!(&err, FormatError::Swift(m) if m.contains("block terminator")),
            "expected a block-terminator rejection, got: {err:?}"
        );
    }

    #[test]
    fn continuation_line_starting_with_tag_marker_errors() {
        // A folded continuation line beginning with `:` would re-read as a
        // spurious new `:tag:` field — unrepresentable, so reject it.
        let s = schema();
        let rec = record(&s, "4", "86", "NARRATIVE\n:99:NOTAFIELD");
        let mut buf = Vec::new();
        let mut w = SwiftWriter::new(
            Cursor::new(&mut buf),
            Arc::clone(&s),
            SwiftWriterConfig::default(),
        );
        let err = w.write_record(&rec).unwrap_err();
        assert!(
            matches!(&err, FormatError::Swift(m) if m.contains("tag marker")),
            "expected a tag-marker rejection, got: {err:?}"
        );
    }

    #[test]
    fn first_value_line_may_start_with_marker() {
        // The first line of the value rides directly after the `:tag:` marker
        // on the wire, so it can begin with `:` or `-}` without ambiguity —
        // only *continuation* lines (after a fold) are constrained.
        let s = schema();
        let rec = record(&s, "4", "20", ":STARTSWITHCOLON");
        let out = write_all(SwiftWriterConfig::default(), &[rec], &s);
        assert!(out.contains(":20::STARTSWITHCOLON\r\n"), "{out}");
    }

    #[test]
    fn missing_tag_errors() {
        let s = schema();
        let rec = Record::new(
            Arc::clone(&s),
            vec![
                Value::String("4".into()),
                Value::Null,
                Value::String("v".into()),
            ],
        );
        let mut buf = Vec::new();
        let mut w = SwiftWriter::new(
            Cursor::new(&mut buf),
            Arc::clone(&s),
            SwiftWriterConfig::default(),
        );
        let err = w.write_record(&rec).unwrap_err();
        assert!(
            matches!(&err, FormatError::Swift(m) if m.contains("no `tag`")),
            "expected a missing-tag error, got: {err:?}"
        );
    }

    #[test]
    fn map_value_in_value_column_errors() {
        let s = schema();
        let mut nested: IndexMap<Box<str>, Value> = IndexMap::new();
        nested.insert("k".into(), Value::String("v".into()));
        let rec = Record::new(
            Arc::clone(&s),
            vec![
                Value::String("4".into()),
                Value::String("20".into()),
                Value::Map(Box::new(nested)),
            ],
        );
        let mut buf = Vec::new();
        let mut w = SwiftWriter::new(
            Cursor::new(&mut buf),
            Arc::clone(&s),
            SwiftWriterConfig::default(),
        );
        let err = w.write_record(&rec).unwrap_err();
        assert!(
            matches!(&err, FormatError::UnserializableMapValue { format: "swift", column } if column == "value"),
            "expected UnserializableMapValue for value, got: {err:?}"
        );
    }

    #[test]
    fn from_doc_naming_absent_section_errors() {
        let s = schema();
        let rec = record(&s, "4", "20", "REF");
        let config = SwiftWriterConfig {
            basic_header_from_doc: Some("nope".into()),
            ..Default::default()
        };
        let mut buf = Vec::new();
        let mut w = SwiftWriter::new(Cursor::new(&mut buf), Arc::clone(&s), config);
        let err = w.write_record(&rec).unwrap_err();
        assert!(
            matches!(&err, FormatError::Swift(m) if m.contains("no `body` field")),
            "expected an absent-section error, got: {err:?}"
        );
    }

    #[test]
    fn zero_records_frames_empty_message() {
        // A header-only output still frames a valid empty message: literal
        // service blocks plus an empty block 4 closed by `-}`.
        let s = schema();
        let config = SwiftWriterConfig {
            basic_header: Some("F01X".into()),
            trailer: Some("{CHK:AB}".into()),
            ..Default::default()
        };
        let out = write_all(config, &[], &s);
        assert_eq!(out, "{1:F01X}{4:\r\n-}{5:{CHK:AB}}", "{out}");
    }
}

//! Multi-record flat-file reader (CSV / fixed-width).
//!
//! A multi-record flat file interleaves heterogeneous record types in one
//! file: a header row, many body rows, and a trailer row, each distinguished
//! by a discriminator (a fixed byte range for fixed-width, a named column for
//! CSV). This reader streams **one [`Record`] per physical line** on a single
//! static superset schema whose lead column is the matched record type's id
//! and whose remaining columns are the union of every type's declared fields.
//! A downstream `Route` discriminates on the `record_type` column; the reader
//! never binds a different physical schema per type and never buffers the
//! file.
//!
//! Memory model: strictly record-at-a-time. The only retained state beyond
//! one line is the optional header pre-scan ([`FormatReader::prepare_document`]
//! captures the first header-tag row, O(one row per header section)), the
//! running body count, and the most recent trailer row's fields used to
//! validate a trailer's declared count at document close. Trailer rows arrive
//! after the body they close, so a trailer record type is validated as it
//! streams rather than surfaced as a `$doc` pre-scan section.
//!
//! Field parsing is delegated to [`crate::fixed_width::field`] so a declared
//! `type:` parses byte-for-byte identically to the single-record fixed-width
//! reader; CSV fields share the same scalar coercion.

use std::collections::HashMap;
use std::io::{BufReader, Read};
use std::sync::Arc;

use indexmap::IndexMap;

use clinker_record::schema_def::{Justify, LineSeparator};
use clinker_record::{Record, Schema, SchemaBuilder, Value};
use cxl::typecheck::Type;

use crate::bom::SkipBom;
use crate::envelope::{EnvelopeConfig, EnvelopeExtract, coerce_section_fields};
use crate::error::FormatError;
use crate::fixed_width::field::{self, ResolvedField};
use crate::schema::{Column, Discriminator, RecordType, StructureConstraint};
use crate::traits::FormatReader;

/// The lead column of the superset schema, carrying the matched record type's
/// declared `id` so a downstream `Route` discriminates on a stable name
/// independent of the discriminator's physical location.
///
/// Reserved: a record type may not declare a field of this name, or its raw
/// value would overwrite the stamped type id and silently misroute every row.
/// Aliased to the shared [`crate::schema::RECORD_TYPE_COLUMN`] so the reader's
/// lead column and the planner's multi-record superset cannot drift apart.
pub const RECORD_TYPE_COLUMN: &str = crate::schema::RECORD_TYPE_COLUMN;

/// CSV dialect for the multi-record reader.
pub struct CsvDialect {
    pub delimiter: u8,
    pub quote_char: u8,
    /// When `true`, the first physical row is a textual column header and is
    /// skipped rather than classified as a record type.
    pub has_header: bool,
}

/// How the discriminator value is located in a physical line.
enum Discrimination {
    /// Fixed-width: a byte half-open range `[start, start + width)`.
    ByteRange { start: usize, width: usize },
    /// CSV: a zero-based column index, resolved from the discriminator field
    /// name's position in the record-type field lists.
    Column(usize),
}

/// One declared record type, resolved for streaming: its id and the per-field
/// extraction plan against a physical line.
struct ResolvedType {
    /// Value stamped into the `record_type` lead column for matched rows.
    id: Arc<str>,
    /// The fields this type contributes, in declaration order. Each maps to a
    /// superset-schema column index plus its extraction plan.
    fields: Vec<TypeField>,
}

/// One declared field of a record type, mapping to a superset column slot.
///
/// A fixed-width field carries a byte-positioned [`ResolvedField`]; a CSV field
/// carries a column index plus the same field-level parse policy (type, format,
/// trim, justify, pad) so a user's `trim`/`pad`/`type` on a CSV field is
/// honored, not silently dropped.
struct TypeField {
    /// Index into the superset schema's column vector.
    column: usize,
    /// The field's name (keys header pre-scan and trailer pairs).
    name: String,
    ty: Type,
    format: Option<String>,
    /// Decimal scale (fractional digits) for a `decimal` column; `None`
    /// otherwise. Threaded into `coerce_scalar` so a decimal value is rounded
    /// to the column scale at parse.
    scale: Option<u8>,
    /// Fixed-width byte-positioned field, or `None` for a CSV field.
    fixed: Option<ResolvedField>,
    /// CSV column index, or `None` for a fixed-width field.
    csv_column: Option<usize>,
    /// CSV-only parse policy (fixed-width carries these on `fixed`).
    trim: bool,
    justify: Option<Justify>,
    pad: String,
}

/// The line scanner backing the reader — fixed-width physical reads or a
/// `csv::Reader` configured `flexible(true)` for ragged per-type rows.
enum LineScanner<R: Read> {
    FixedWidth {
        reader: BufReader<SkipBom<R>>,
        separator: LineSeparator,
        record_length: usize,
        line_buf: Vec<u8>,
    },
    Csv {
        reader: csv::Reader<SkipBom<R>>,
        record_buf: csv::StringRecord,
        /// `true` until the column-header row has been consumed (when the
        /// dialect declares one).
        pending_header: bool,
    },
}

/// One physical line, decoded into the shape the discriminator and fields
/// extract from: raw bytes for fixed-width, parsed columns for CSV.
enum ScannedLine {
    FixedWidth(Vec<u8>),
    Csv(csv::StringRecord),
}

/// The resolved building blocks both backends produce from a `records:` list:
/// the static superset schema and the tag → record-type map.
type ResolvedParts = (Arc<Schema>, HashMap<String, ResolvedType>);

/// A backend resolver's output: the [`ResolvedParts`] plus a trailing usize —
/// the fixed-width record length, or the CSV discriminator column index.
type ResolvedBackend = (Arc<Schema>, HashMap<String, ResolvedType>, usize);

/// Streaming multi-record flat-file reader.
///
/// Holds the line scanner, the static superset schema, a `tag → ResolvedType`
/// map, the running counts, and the trailer structural constraints. Stamps
/// each matched line as one record on the superset schema.
pub struct MultiRecordReader<R: Read> {
    scanner: LineScanner<R>,
    schema: Arc<Schema>,
    discrimination: Discrimination,
    /// Tag (trimmed) → resolved record type. A row whose tag is absent here is
    /// an unknown discriminator value.
    by_tag: HashMap<String, ResolvedType>,
    /// Trailer structural constraints: a record-type id whose declared count
    /// column must equal the actual body count at document close.
    structure: Vec<StructureConstraint>,
    /// Count of physical lines read (every line, including header/trailer and
    /// skipped blanks), so a diagnostic names the offending physical line.
    physical_row: u64,
    /// Count of body rows (every matched row that is neither a header nor a
    /// trailer type), checked against a trailer's declared count.
    body_count: u64,
    /// Header-type tags captured by the bounded pre-scan and excluded from the
    /// body count. Known at construction from the source's `envelope:` config.
    header_tags: Vec<String>,
    /// Header-type ids matched to `header_tags`, for the body-skip pointer check.
    header_ids: Vec<Arc<str>>,
    /// Trailer-type ids (the `record` side of a `StructureConstraint`).
    trailer_ids: Vec<Arc<str>>,
    /// Captured header rows, keyed by record-type tag: the first row of each
    /// header type, as declared field-name → raw text pairs. The envelope
    /// section schema coerces them when `prepare_document` runs.
    captured_headers: HashMap<String, Vec<(String, String)>>,
    /// Whether the one-time header pre-scan has run.
    prescanned: bool,
    /// The non-header row that terminated the pre-scan, held so the body stream
    /// consumes it rather than dropping it.
    pending_line: Option<ScannedLine>,
    /// The most recent trailer row: its type id and field-name → declared count
    /// text, resolved at end-of-input structural validation.
    last_trailer: Option<(Arc<str>, HashMap<String, String>)>,
    /// `true` once the trailer of a constrained type has streamed; a body row
    /// after it is a structural error (content past the document's trailer).
    trailer_seen: bool,
    done: bool,
}

/// The format-agnostic construction inputs for a [`MultiRecordReader`], shared
/// by the CSV and fixed-width entry points. Carries the already-resolved
/// record types (inherits merged, constraints validated).
pub struct MultiRecordSpec {
    /// How each line's record type is identified.
    pub discriminator: Discriminator,
    /// The resolved record types — each with its tag and per-field layout.
    pub record_types: Vec<RecordType>,
    /// Trailer structural-count constraints validated at document close.
    pub structure: Vec<StructureConstraint>,
    /// Header record-type tags captured by the pre-scan (the `record_type`
    /// envelope extracts) and excluded from the body stream.
    pub header_tags: Vec<String>,
}

impl<R: Read> MultiRecordReader<R> {
    /// Build a fixed-width multi-record reader.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::FixedWidth`] when the discriminator lacks a byte
    /// `start`; or a construction error when a record type declares no `tag`,
    /// duplicates a tag/id, declares a `record_type` field, a same-named field
    /// with an incompatible type across types, a structural constraint names an
    /// undeclared record type, or a field omits its `start`/`width`.
    pub fn new_fixed_width(
        reader: R,
        spec: MultiRecordSpec,
        separator: LineSeparator,
    ) -> Result<Self, FormatError> {
        let start = spec.discriminator.start.ok_or_else(|| {
            FormatError::FixedWidth(
                "multi-record fixed-width discriminator requires a byte `start`".into(),
            )
        })?;
        let width = spec.discriminator.width.unwrap_or(1);
        let discrimination = Discrimination::ByteRange { start, width };

        let (schema, by_tag, record_length) =
            resolve_fixed_width(&spec.record_types, start, width)?;
        let scanner = LineScanner::FixedWidth {
            reader: BufReader::new(SkipBom::new(reader)),
            separator,
            record_length,
            line_buf: Vec::with_capacity(256),
        };
        Self::assemble(scanner, schema, discrimination, by_tag, spec)
    }

    /// Build a CSV multi-record reader.
    ///
    /// The discriminator names a field; its column index is the position of
    /// that field in each record type's declared field list and must be
    /// consistent across every type (typically the first column).
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::Csv`] when the discriminator omits its `field`
    /// name, the field is absent from a type or sits at a different column
    /// across types; or a construction error for a duplicate tag/id, a
    /// `record_type` field, an incompatible same-named field, or a structural
    /// constraint naming an undeclared record type.
    pub fn new_csv(
        reader: R,
        spec: MultiRecordSpec,
        dialect: CsvDialect,
    ) -> Result<Self, FormatError> {
        let field = spec.discriminator.field.as_deref().ok_or_else(|| {
            FormatError::Csv(csv_error(
                "multi-record CSV discriminator requires a `field` name",
            ))
        })?;
        let (schema, by_tag, column) = resolve_csv(&spec.record_types, field)?;
        let discrimination = Discrimination::Column(column);
        let csv_reader = csv::ReaderBuilder::new()
            .delimiter(dialect.delimiter)
            .quote(dialect.quote_char)
            // Heterogeneous record types have different column counts; per-type
            // column-count validation happens at extraction, so the underlying
            // parser must accept ragged rows rather than erroring file-wide.
            .flexible(true)
            // The reader handles the column-header row itself (skipping it as a
            // pending header), so the csv crate never treats a row as headers.
            .has_headers(false)
            .from_reader(SkipBom::new(reader));
        let scanner = LineScanner::Csv {
            reader: csv_reader,
            record_buf: csv::StringRecord::new(),
            pending_header: dialect.has_header,
        };
        Self::assemble(scanner, schema, discrimination, by_tag, spec)
    }

    /// Assemble the reader from its resolved parts, deriving the header/trailer
    /// id sets used for body-count bookkeeping and pre-scan classification.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::SchemaInference`] when a `header_tags` entry or a
    /// structural constraint names a record type that does not exist.
    fn assemble(
        scanner: LineScanner<R>,
        schema: Arc<Schema>,
        discrimination: Discrimination,
        by_tag: HashMap<String, ResolvedType>,
        spec: MultiRecordSpec,
    ) -> Result<Self, FormatError> {
        let MultiRecordSpec {
            structure,
            header_tags,
            ..
        } = spec;
        // Every structural constraint must name a declared record type; an id
        // that matches none is a config error, not a silently-skipped check.
        for c in &structure {
            if !by_tag.values().any(|rt| rt.id.as_ref() == c.record) {
                return Err(FormatError::SchemaInference(format!(
                    "structure constraint names record type '{}', which no `records:` entry \
                     declares (id mismatch)",
                    c.record
                )));
            }
        }
        let trailer_ids: Vec<Arc<str>> = by_tag
            .values()
            .filter(|rt| {
                structure
                    .iter()
                    .any(|c| c.record.as_str() == rt.id.as_ref())
            })
            .map(|rt| Arc::clone(&rt.id))
            .collect();
        let mut header_ids = Vec::with_capacity(header_tags.len());
        for tag in &header_tags {
            let id = by_tag
                .get(tag)
                .map(|rt| Arc::clone(&rt.id))
                .ok_or_else(|| {
                    FormatError::SchemaInference(format!(
                        "envelope `record_type: {tag}` matches no declared `records:` entry"
                    ))
                })?;
            header_ids.push(id);
        }
        Ok(Self {
            scanner,
            schema,
            discrimination,
            by_tag,
            structure,
            physical_row: 0,
            body_count: 0,
            header_tags,
            header_ids,
            trailer_ids,
            captured_headers: HashMap::new(),
            prescanned: false,
            pending_line: None,
            last_trailer: None,
            trailer_seen: false,
            done: false,
        })
    }

    /// Read one non-blank physical line from the scanner, or `None` at end of
    /// input. Blank lines (empty, or whitespace-only — common after file
    /// concatenation) are skipped rather than rejected.
    fn scan_line(&mut self) -> Result<Option<ScannedLine>, FormatError> {
        loop {
            match &mut self.scanner {
                LineScanner::FixedWidth {
                    reader,
                    separator,
                    record_length,
                    line_buf,
                } => {
                    if !field::read_physical_line(
                        reader,
                        separator,
                        *record_length,
                        self.physical_row,
                        line_buf,
                    )? {
                        return Ok(None);
                    }
                    self.physical_row += 1;
                    if field::is_blank_line(line_buf) {
                        continue;
                    }
                    return Ok(Some(ScannedLine::FixedWidth(line_buf.clone())));
                }
                LineScanner::Csv {
                    reader,
                    record_buf,
                    pending_header,
                } => {
                    if !reader.read_record(record_buf)? {
                        return Ok(None);
                    }
                    self.physical_row += 1;
                    // The first physical row is the textual column header when
                    // the dialect declares one — skip it once.
                    if *pending_header {
                        *pending_header = false;
                        continue;
                    }
                    if csv_row_is_blank(record_buf) {
                        continue;
                    }
                    return Ok(Some(ScannedLine::Csv(record_buf.clone())));
                }
            }
        }
    }

    /// Extract the trimmed discriminator tag from a scanned line.
    fn extract_tag(&self, line: &ScannedLine) -> Result<String, FormatError> {
        match (&self.discrimination, line) {
            (Discrimination::ByteRange { start, width }, ScannedLine::FixedWidth(bytes)) => {
                let end = start + width;
                let slice = bytes.get(*start..end).ok_or_else(|| {
                    FormatError::FixedWidth(format!(
                        "line {} too short for discriminator: need {end} bytes, got {}",
                        self.physical_row,
                        bytes.len()
                    ))
                })?;
                let tag = std::str::from_utf8(slice).map_err(|_| {
                    FormatError::FixedWidth(format!(
                        "line {}: invalid UTF-8 in discriminator byte range",
                        self.physical_row
                    ))
                })?;
                Ok(tag.trim().to_string())
            }
            (Discrimination::Column(idx), ScannedLine::Csv(rec)) => {
                Ok(rec.get(*idx).unwrap_or("").trim().to_string())
            }
            // The discrimination kind is bound to the scanner kind at
            // construction, so a cross pairing cannot occur.
            _ => Err(FormatError::SchemaInference(
                "discriminator/line-format mismatch".into(),
            )),
        }
    }

    /// Build the body [`Record`] for a matched line: the `record_type` lead
    /// column plus every declared field coerced into its superset slot;
    /// unfilled slots stay `Value::Null`.
    fn build_record(&self, rt: &ResolvedType, line: &ScannedLine) -> Result<Record, FormatError> {
        let mut values: Vec<Value> = vec![Value::Null; self.schema.column_count()];
        values[0] = Value::String(rt.id.as_ref().into());
        for tf in &rt.fields {
            values[tf.column] = extract_field_value(tf, line, self.physical_row)?;
        }
        Ok(Record::new(Arc::clone(&self.schema), values))
    }

    /// Run the one-time header pre-scan: forward-scan the contiguous header
    /// region at the file head, capturing the first row of each declared header
    /// tag, and stop at the first non-header row. Idempotent.
    fn ensure_prescanned(&mut self) -> Result<(), FormatError> {
        if self.prescanned {
            return Ok(());
        }
        self.prescanned = true;
        if self.header_tags.is_empty() {
            return Ok(());
        }
        loop {
            if self.captured_headers.len() == self.header_tags.len() {
                return Ok(());
            }
            let Some(line) = self.scan_line()? else {
                return Ok(());
            };
            let tag = self.extract_tag(&line)?;
            if !self.header_tags.iter().any(|t| t == &tag) {
                // The header region ended. Hold this row for the body stream.
                self.pending_line = Some(line);
                return Ok(());
            }
            if self.captured_headers.contains_key(&tag) {
                continue; // first occurrence wins
            }
            let rt = self
                .by_tag
                .get(&tag)
                .expect("header tag resolved to a record type at construction");
            let pairs = collect_named_pairs(rt, &line, self.physical_row)?;
            self.captured_headers.insert(tag, pairs);
        }
    }

    /// Pull the next matched body record, consuming header rows (captured by
    /// the pre-scan) and trailer rows (they feed structural validation), or
    /// `None` at clean end of input.
    fn pull_next(&mut self) -> Result<Option<Record>, FormatError> {
        self.ensure_prescanned()?;
        loop {
            let line = match self.pending_line.take() {
                Some(line) => line,
                None => match self.scan_line()? {
                    Some(line) => line,
                    None => {
                        self.validate_structure()?;
                        self.done = true;
                        return Ok(None);
                    }
                },
            };
            let tag = self.extract_tag(&line)?;
            let Some(rt) = self.by_tag.get(&tag) else {
                return Err(self.unknown_tag_error(&tag));
            };
            let id = Arc::clone(&rt.id);
            if self.header_ids.iter().any(|h| Arc::ptr_eq(h, &id)) {
                // A header type's row outside the contiguous head region: the
                // pre-scan already captured the first occurrence.
                continue;
            }
            if self.trailer_ids.iter().any(|t| Arc::ptr_eq(t, &id)) {
                let raw = collect_named_pairs(rt, &line, self.physical_row)?;
                self.last_trailer = Some((id, raw.into_iter().collect()));
                self.trailer_seen = true;
                continue;
            }
            // A body row after a constrained trailer is content past the
            // document's close — a structural error, not a silently-counted row.
            if self.trailer_seen {
                return Err(FormatError::multi_record_structural_validation(format!(
                    "line {}: body record of type '{}' appears after the trailer that closes \
                     the document",
                    self.physical_row, id
                )));
            }
            self.body_count += 1;
            let rt = &self.by_tag[&tag];
            return Ok(Some(self.build_record(rt, &line)?));
        }
    }

    /// Build the E345 error for an unknown discriminator value.
    ///
    /// Classed as a structural-validation error — distinguishable from a
    /// trailer count mismatch — so the source ingest driver routes it through
    /// the document-level dead-letter seam under `dlq_granularity: document`
    /// (condemning the file) instead of always aborting the run. Per-record
    /// dead-lettering of a single unknown-tag row is tracked separately.
    fn unknown_tag_error(&self, tag: &str) -> FormatError {
        FormatError::multi_record_structural_validation(format!(
            "E345 line {}: unknown record-type discriminator {tag:?} — no `records:` entry \
             declares this tag. Declare a record type with `tag: {tag}`, or set \
             `dlq_granularity: document` to dead-letter the file. \
             See: clinker explain --code E345",
            self.physical_row
        ))
    }

    /// Validate every trailer structural constraint against the streamed body
    /// count at end of input.
    ///
    /// A declared constraint whose trailer never appeared is an incomplete
    /// document, not a silently-skipped check.
    ///
    /// # Errors
    ///
    /// Returns a [`FormatError::StructuralCount`] when a trailer's declared
    /// count disagrees with the body count, or its trailer is absent.
    fn validate_structure(&self) -> Result<(), FormatError> {
        for constraint in &self.structure {
            let declared = self.last_trailer_count(&constraint.record, &constraint.count)?;
            let Some(declared) = declared else {
                return Err(FormatError::multi_record_structural_count(format!(
                    "trailer record '{}' is declared in `structure` but no such row appeared; \
                     the document is incomplete (expected a trailer carrying field '{}')",
                    constraint.record, constraint.count
                )));
            };
            if declared != self.body_count {
                return Err(FormatError::multi_record_structural_count(format!(
                    "trailer record '{}' declares count {} (field '{}'), but the file streamed \
                     {} body records",
                    constraint.record, declared, constraint.count, self.body_count
                )));
            }
        }
        Ok(())
    }

    /// The declared count from the most recent trailer row of the named type,
    /// parsed from its `count` field. `None` when no trailer was seen.
    ///
    /// # Errors
    ///
    /// Returns a [`FormatError::StructuralCount`] when the trailer declares no
    /// such field (a truncated trailer line whose count column was cut off, or
    /// a misconfigured field name) or its value is not a non-negative integer.
    fn last_trailer_count(
        &self,
        record_id: &str,
        count_field: &str,
    ) -> Result<Option<u64>, FormatError> {
        match self.last_trailer.as_ref() {
            Some((id, fields)) if id.as_ref() == record_id => {
                let raw = fields
                    .get(count_field)
                    .filter(|v| !v.is_empty())
                    .ok_or_else(|| {
                        FormatError::multi_record_structural_count(format!(
                            "trailer record '{record_id}' carries no value for count field \
                         '{count_field}' — the trailer line is truncated or misconfigured"
                        ))
                    })?;
                let parsed: u64 = raw.trim().parse().map_err(|_| {
                    FormatError::multi_record_structural_count(format!(
                        "trailer record '{record_id}' field '{count_field}' value {raw:?} is not \
                         a non-negative integer count"
                    ))
                })?;
                Ok(Some(parsed))
            }
            _ => Ok(None),
        }
    }
}

impl<R: Read + Send> FormatReader for MultiRecordReader<R> {
    fn schema(&mut self) -> Result<Arc<Schema>, FormatError> {
        Ok(Arc::clone(&self.schema))
    }

    fn next_record(&mut self) -> Result<Option<Record>, FormatError> {
        if self.done {
            return Ok(None);
        }
        self.pull_next()
    }

    fn prepare_document(
        &mut self,
        config: &EnvelopeConfig,
    ) -> Result<IndexMap<Box<str>, Value>, FormatError> {
        if config.is_empty() {
            return Ok(IndexMap::new());
        }
        self.ensure_prescanned()?;
        let mut out: IndexMap<Box<str>, Value> = IndexMap::with_capacity(config.sections.len());
        for (name, section) in &config.sections {
            let tag = match &section.extract {
                EnvelopeExtract::RecordType(tag) => tag.as_str(),
                EnvelopeExtract::XmlPath(_)
                | EnvelopeExtract::JsonPointer(_)
                | EnvelopeExtract::Segment(_) => {
                    return Err(FormatError::SchemaInference(format!(
                        "envelope section {name:?}: declared a non-`record_type` extract against \
                         a multi-record flat-file source. Use `record_type` (e.g. \
                         `extract: {{ record_type: H }}`) for multi-record CSV / fixed-width."
                    )));
                }
            };
            // The captured row's fields are keyed by the record type's declared
            // field names; the section schema selects and coerces the ones it
            // declares to their `$doc` types.
            let Some(captured) = self.captured_headers.get(tag) else {
                // No row of that header type appeared; its `$doc` fields resolve
                // to Null via the absent-section convention.
                continue;
            };
            let typed = coerce_section_fields(captured.clone(), &section.fields)
                .map_err(FormatError::SchemaInference)?;
            out.insert(name.as_str().into(), Value::Map(Box::new(typed)));
        }
        Ok(out)
    }
}

/// Resolve the fixed-width superset schema, tag map, and record length.
fn resolve_fixed_width(
    record_types: &[RecordType],
    disc_start: usize,
    disc_width: usize,
) -> Result<ResolvedBackend, FormatError> {
    let mut builder = SupersetBuilder::new();
    let mut record_length = disc_start + disc_width;
    for rt in record_types {
        let tag = require_tag(&rt.tag, &rt.id).map_err(FormatError::FixedWidth)?;
        let id: Arc<str> = Arc::from(rt.id.as_str());
        let mut fields = Vec::with_capacity(rt.columns.len());
        for f in &rt.columns {
            reject_record_type_field(&f.name).map_err(FormatError::FixedWidth)?;
            let resolved = ResolvedField::from_column(f)?;
            record_length = record_length.max(resolved.end());
            let column = builder.intern(f).map_err(FormatError::FixedWidth)?;
            fields.push(TypeField {
                column,
                name: f.name.clone(),
                ty: f.ty.clone(),
                format: f.format.clone(),
                scale: f.scale,
                fixed: Some(resolved),
                csv_column: None,
                trim: f.trim.unwrap_or(true),
                justify: f.justify.clone(),
                pad: f.pad.clone().unwrap_or_else(|| " ".into()),
            });
        }
        insert_type(
            &mut builder.by_tag,
            tag,
            &rt.id,
            ResolvedType { id, fields },
        )
        .map_err(FormatError::FixedWidth)?;
    }
    let (schema, by_tag) = builder.finish();
    Ok((schema, by_tag, record_length))
}

/// Resolve the CSV superset schema, tag map, and the discriminator column index.
fn resolve_csv(
    record_types: &[RecordType],
    disc_field: &str,
) -> Result<ResolvedBackend, FormatError> {
    let mut builder = SupersetBuilder::new();
    let mut disc_column: Option<usize> = None;
    for rt in record_types {
        let tag = require_tag(&rt.tag, &rt.id).map_err(|m| FormatError::Csv(csv_error(&m)))?;
        let id: Arc<str> = Arc::from(rt.id.as_str());
        let local = rt
            .columns
            .iter()
            .position(|f| f.name == disc_field)
            .ok_or_else(|| {
                FormatError::Csv(csv_error(&format!(
                    "multi-record CSV discriminator field '{disc_field}' is absent from record \
                     type '{}'; every record type must declare it at the same column",
                    rt.id
                )))
            })?;
        match disc_column {
            None => disc_column = Some(local),
            Some(c) if c != local => {
                return Err(FormatError::Csv(csv_error(&format!(
                    "multi-record CSV discriminator field '{disc_field}' is at column {local} in \
                     record type '{}' but column {c} elsewhere; it must be at a consistent \
                     column across every type",
                    rt.id
                ))));
            }
            Some(_) => {}
        }
        let mut fields = Vec::with_capacity(rt.columns.len());
        for (csv_idx, f) in rt.columns.iter().enumerate() {
            reject_record_type_field(&f.name).map_err(|m| FormatError::Csv(csv_error(&m)))?;
            let column = builder
                .intern(f)
                .map_err(|m| FormatError::Csv(csv_error(&m)))?;
            fields.push(TypeField {
                column,
                name: f.name.clone(),
                ty: f.ty.clone(),
                format: f.format.clone(),
                scale: f.scale,
                fixed: None,
                csv_column: Some(csv_idx),
                trim: f.trim.unwrap_or(true),
                justify: f.justify.clone(),
                pad: f.pad.clone().unwrap_or_else(|| " ".into()),
            });
        }
        insert_type(
            &mut builder.by_tag,
            tag,
            &rt.id,
            ResolvedType { id, fields },
        )
        .map_err(|m| FormatError::Csv(csv_error(&m)))?;
    }
    let column = disc_column.ok_or_else(|| {
        FormatError::Csv(csv_error(
            "multi-record CSV schema declares no record types",
        ))
    })?;
    let (schema, by_tag) = builder.finish();
    Ok((schema, by_tag, column))
}

/// Accumulates the superset column list and the tag map while resolving record
/// types, enforcing the cross-type field-name/type compatibility invariant.
struct SupersetBuilder {
    columns: Vec<String>,
    column_index: HashMap<String, usize>,
    /// Declared type of each named column, to reject an incompatible re-use.
    column_type: HashMap<String, Type>,
    by_tag: HashMap<String, ResolvedType>,
}

impl SupersetBuilder {
    fn new() -> Self {
        let mut column_index = HashMap::new();
        column_index.insert(RECORD_TYPE_COLUMN.to_string(), 0);
        Self {
            columns: vec![RECORD_TYPE_COLUMN.to_string()],
            column_index,
            column_type: HashMap::new(),
            by_tag: HashMap::new(),
        }
    }

    /// Intern a field name into the shared superset column list, returning its
    /// index. A name shared across record types maps to one column — but only
    /// when the two declared types unify; two types declaring the same field
    /// name with types that cannot unify (e.g. `string` vs `int`) would corrupt
    /// one shared slot, so that is an error. Unification (not exact equality) is
    /// the compatibility rule, so a numeric widening (`int` vs `float`) is
    /// accepted.
    ///
    /// # Errors
    ///
    /// Returns a message when a same-named field is re-declared with a type
    /// that does not unify with its prior declaration.
    fn intern(&mut self, f: &Column) -> Result<usize, String> {
        if let Some(&idx) = self.column_index.get(&f.name) {
            if let Some(prior) = self.column_type.get(&f.name)
                && prior.unify(&f.ty).is_none()
            {
                return Err(format!(
                    "field '{}' is declared by more than one record type with incompatible \
                     types ({prior} vs {}); a shared superset column must have unifiable types",
                    f.name, f.ty
                ));
            }
            return Ok(idx);
        }
        let idx = self.columns.len();
        self.columns.push(f.name.clone());
        self.column_index.insert(f.name.clone(), idx);
        self.column_type.insert(f.name.clone(), f.ty.clone());
        Ok(idx)
    }

    fn finish(self) -> ResolvedParts {
        let schema = self
            .columns
            .into_iter()
            .map(Box::<str>::from)
            .collect::<SchemaBuilder>()
            .build();
        (schema, self.by_tag)
    }
}

/// Insert a resolved type under its tag, rejecting a duplicate tag or id.
fn insert_type(
    by_tag: &mut HashMap<String, ResolvedType>,
    tag: String,
    id: &str,
    rt: ResolvedType,
) -> Result<(), String> {
    if by_tag.contains_key(&tag) {
        return Err(format!(
            "discriminator tag '{tag}' is declared by more than one record type; tags must be \
             unique"
        ));
    }
    if by_tag.values().any(|t| t.id.as_ref() == id) {
        return Err(format!(
            "record type id '{id}' is declared more than once; ids must be unique"
        ));
    }
    by_tag.insert(tag, rt);
    Ok(())
}

/// Reject a field named `record_type`, which would collide with the reserved
/// lead column.
fn reject_record_type_field(name: &str) -> Result<(), String> {
    if name == RECORD_TYPE_COLUMN {
        return Err(format!(
            "a record type declares a field named '{RECORD_TYPE_COLUMN}', which is reserved for \
             the lead discriminator column; rename the field"
        ));
    }
    Ok(())
}

/// Require a non-empty record-type tag, returning the trimmed tag or a
/// format-neutral error message naming the offending type id.
fn require_tag(tag: &str, id: &str) -> Result<String, String> {
    if tag.is_empty() {
        return Err(format!(
            "record type '{id}' declares an empty `tag`; a discriminator tag is required"
        ));
    }
    Ok(tag.trim().to_string())
}

/// Extract one field's typed value from a scanned line: pull the raw text
/// (shared with pair collection), then coerce to the declared type.
fn extract_field_value(tf: &TypeField, line: &ScannedLine, row: u64) -> Result<Value, FormatError> {
    let raw = field_raw_text(tf, line, row)?;
    if raw.is_empty() {
        return Ok(Value::Null);
    }
    field::coerce_scalar(&tf.ty, tf.format.as_deref(), tf.scale, &raw).map_err(|m| {
        FormatError::InvalidRecord {
            row,
            message: format!("field '{}': {m}", tf.name),
        }
    })
}

/// A field's raw (un-coerced) text from a scanned line, stripped of padding
/// per its policy. Fixed-width delegates to the shared `field::field_text`
/// (which errors on a truncated value); a CSV field honors its own
/// `trim`/`justify`/`pad` so a user's `trim: false` or custom pad is respected.
///
/// # Errors
///
/// Returns [`FormatError::InvalidRecord`] when a fixed-width field is truncated
/// or not valid UTF-8.
fn field_raw_text(tf: &TypeField, line: &ScannedLine, row: u64) -> Result<String, FormatError> {
    match (line, tf.fixed.as_ref(), tf.csv_column) {
        (ScannedLine::FixedWidth(bytes), Some(resolved), _) => {
            field::field_text(resolved, bytes, row)
        }
        (ScannedLine::Csv(rec), _, Some(idx)) => Ok(csv_field_text(rec, idx, tf)),
        _ => Err(FormatError::SchemaInference(
            "field/line-format mismatch in multi-record extraction".into(),
        )),
    }
}

/// A CSV field's raw text, honoring the field's `trim` / `justify` / `pad`
/// policy (not a hardcoded trim), so a user's `trim: false` or custom pad on a
/// CSV field is respected.
fn csv_field_text(rec: &csv::StringRecord, idx: usize, tf: &TypeField) -> String {
    let raw = rec.get(idx).unwrap_or("");
    if !tf.trim {
        return raw.to_string();
    }
    if tf.pad.is_empty() {
        return raw.trim().to_string();
    }
    let stripped = match tf.justify {
        Some(Justify::Right) => raw.trim_start_matches(tf.pad.as_str()),
        Some(Justify::Left) | None => raw.trim_end_matches(tf.pad.as_str()),
    };
    stripped.trim().to_string()
}

/// Collect each declared field of a matched type with its raw string value,
/// keyed by the declared field name (drives trailer-field capture and the
/// header pre-scan).
fn collect_named_pairs(
    rt: &ResolvedType,
    line: &ScannedLine,
    row: u64,
) -> Result<Vec<(String, String)>, FormatError> {
    let mut pairs = Vec::with_capacity(rt.fields.len());
    for tf in &rt.fields {
        pairs.push((tf.name.clone(), field_raw_text(tf, line, row)?));
    }
    Ok(pairs)
}

/// `true` when a CSV row carries no field content (no fields, or every field
/// empty after trimming).
fn csv_row_is_blank(rec: &csv::StringRecord) -> bool {
    rec.iter().all(|f| f.trim().is_empty())
}

/// Build a `csv::Error` carrying a custom message.
fn csv_error(message: &str) -> csv::Error {
    csv::Error::from(std::io::Error::other(message.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::EnvelopeSection;
    use std::io::Cursor;

    fn fw_field(name: &str, start: usize, width: usize, ty: Type) -> Column {
        Column {
            start: Some(start),
            width: Some(width),
            ..Column::bare(name, ty)
        }
    }

    fn csv_field(name: &str, ty: Type) -> Column {
        Column::bare(name, ty)
    }

    fn rtype(id: &str, tag: &str, columns: Vec<Column>) -> RecordType {
        RecordType {
            id: id.into(),
            tag: tag.into(),
            description: None,
            parent: None,
            join_key: None,
            columns,
        }
    }

    fn fw_disc(start: usize, width: usize) -> Discriminator {
        Discriminator {
            start: Some(start),
            width: Some(width),
            field: None,
        }
    }

    fn csv_disc(field: &str) -> Discriminator {
        Discriminator {
            start: None,
            width: None,
            field: Some(field.into()),
        }
    }

    fn fw_types() -> Vec<RecordType> {
        vec![
            rtype("header", "H", vec![fw_field("batch", 1, 9, Type::String)]),
            rtype(
                "detail",
                "D",
                vec![
                    fw_field("id", 1, 5, Type::Int),
                    fw_field("amount", 6, 4, Type::Int),
                ],
            ),
            rtype("trailer", "T", vec![fw_field("count", 1, 5, Type::Int)]),
        ]
    }

    fn fw_reader<R: Read>(
        reader: R,
        types: Vec<RecordType>,
        structure: Vec<StructureConstraint>,
        header_tags: Vec<String>,
    ) -> Result<MultiRecordReader<R>, FormatError> {
        MultiRecordReader::new_fixed_width(
            reader,
            MultiRecordSpec {
                discriminator: fw_disc(0, 1),
                record_types: types,
                structure,
                header_tags,
            },
            LineSeparator::Lf,
        )
    }

    fn trailer_constraint() -> Vec<StructureConstraint> {
        vec![StructureConstraint {
            record: "trailer".into(),
            count: "count".into(),
        }]
    }

    fn collect<R: Read + Send>(mut reader: MultiRecordReader<R>) -> Vec<Record> {
        let mut out = Vec::new();
        while let Some(r) = reader.next_record().unwrap() {
            out.push(r);
        }
        out
    }

    fn drain_err<R: Read + Send>(mut reader: MultiRecordReader<R>) -> FormatError {
        loop {
            match reader.next_record() {
                Ok(Some(_)) => continue,
                Ok(None) => panic!("expected an error, got clean end"),
                Err(e) => return e,
            }
        }
    }

    #[test]
    fn fixed_width_discriminates_body_records_on_superset_schema() {
        let data = b"HREPORT   \nD00001 100\nD00002 200\nT00002    \n";
        let reader = fw_reader(&data[..], fw_types(), Vec::new(), Vec::new()).unwrap();
        let recs = collect(reader);
        // 1 header + 2 detail + 1 trailer = 4 records (no envelope, no
        // structural constraint, so every type is a body record).
        assert_eq!(recs.len(), 4);
        assert_eq!(
            recs[0].get(RECORD_TYPE_COLUMN),
            Some(&Value::String("header".into()))
        );
        assert_eq!(recs[1].get("id"), Some(&Value::Integer(1)));
        assert_eq!(recs[1].get("amount"), Some(&Value::Integer(100)));
        assert_eq!(recs[1].get("batch"), Some(&Value::Null));
        assert_eq!(recs[1].get("count"), Some(&Value::Null));
        assert_eq!(
            recs[3].get(RECORD_TYPE_COLUMN),
            Some(&Value::String("trailer".into()))
        );
    }

    #[test]
    fn unknown_tag_is_a_structural_validation_error_with_e345() {
        let data = b"D00001 100\nX99999 999\n";
        let mut reader = fw_reader(&data[..], fw_types(), Vec::new(), Vec::new()).unwrap();
        assert!(reader.next_record().unwrap().is_some());
        let err = reader.next_record().unwrap_err();
        assert!(
            matches!(
                err,
                FormatError::StructuralValidation {
                    format: "multi-record",
                    ..
                }
            ),
            "an unknown tag is a validation failure, not a count mismatch: {err:?}"
        );
        assert!(
            err.is_document_structural(),
            "must route through the document DLQ seam"
        );
        assert!(
            !err.is_structural_count(),
            "must stay distinguishable from a trailer count mismatch"
        );
        let msg = err.to_string();
        assert!(msg.contains("E345"), "expected E345 in: {msg}");
        assert!(msg.contains("\"X\""), "should name the unknown tag: {msg}");
        assert!(
            msg.contains("line 2"),
            "should name the physical line: {msg}"
        );
    }

    #[test]
    fn trailer_count_matches_body_count_with_header_doc() {
        let data = b"HREPORT   \nD00001 100\nD00002 200\nT00002    \n";
        let mut reader = fw_reader(
            &data[..],
            fw_types(),
            trailer_constraint(),
            vec!["H".to_string()],
        )
        .unwrap();
        let mut cfg = EnvelopeConfig::default();
        let mut fields = IndexMap::new();
        fields.insert(
            "batch".to_string(),
            crate::envelope::EnvelopeFieldType::String,
        );
        cfg.sections.insert(
            "head".to_string(),
            EnvelopeSection {
                extract: EnvelopeExtract::RecordType("H".into()),
                fields,
            },
        );
        let sections = reader.prepare_document(&cfg).unwrap();
        let head = match sections.get("head").unwrap() {
            Value::Map(m) => m,
            other => panic!("expected map: {other:?}"),
        };
        assert_eq!(head.get("batch"), Some(&Value::String("REPORT".into())));
        let recs = collect(reader);
        assert_eq!(recs.len(), 2);
        assert!(
            recs.iter()
                .all(|r| r.get(RECORD_TYPE_COLUMN) == Some(&Value::String("detail".into())))
        );
    }

    #[test]
    fn trailer_count_mismatch_errors() {
        let data = b"D00001 100\nD00002 200\nT00005    \n";
        let reader = fw_reader(&data[..], fw_types(), trailer_constraint(), Vec::new()).unwrap();
        let err = drain_err(reader);
        assert!(err.is_structural_count());
        assert!(
            err.is_document_structural(),
            "must route through the document DLQ seam"
        );
        let msg = err.to_string();
        assert!(msg.contains("declares count 5"), "msg: {msg}");
        assert!(msg.contains("2 body records"), "msg: {msg}");
    }

    #[test]
    fn missing_trailer_errors_as_incomplete_document() {
        // Two detail rows, no trailer at all, but a trailer is declared.
        let data = b"D00001 100\nD00002 200\n";
        let reader = fw_reader(&data[..], fw_types(), trailer_constraint(), Vec::new()).unwrap();
        let err = drain_err(reader);
        assert!(err.is_structural_count());
        assert!(err.to_string().contains("incomplete"), "{err}");
    }

    #[test]
    fn post_trailer_body_row_is_a_structural_validation_error() {
        // A detail row after the trailer that closes the document is content
        // past the document close, not a count claim.
        let data = b"D00001 100\nT00001    \nD00002 200\n";
        let reader = fw_reader(&data[..], fw_types(), trailer_constraint(), Vec::new()).unwrap();
        let err = drain_err(reader);
        assert!(
            matches!(err, FormatError::StructuralValidation { .. }),
            "content past the document close is a validation failure: {err:?}"
        );
        assert!(
            err.is_document_structural(),
            "must route through the document DLQ seam"
        );
        assert!(err.to_string().contains("after the trailer"), "{err}");
    }

    #[test]
    fn blank_lines_are_skipped_not_aborting() {
        // A blank line between body rows (common after concatenation) is
        // skipped, not treated as a too-short discriminator error.
        let data = b"D00001 100\n\n   \nD00002 200\n";
        let reader = fw_reader(&data[..], fw_types(), Vec::new(), Vec::new()).unwrap();
        let recs = collect(reader);
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[1].get("id"), Some(&Value::Integer(2)));
    }

    #[test]
    fn truncated_body_field_errors_not_silent_partial() {
        // The amount field (bytes 6..10) is cut off: "D00002 20" stops at 9
        // bytes, so amount would slice "20" — must error, not return 20.
        let data = b"D00002 20";
        let reader = fw_reader(&data[..], fw_types(), Vec::new(), Vec::new()).unwrap();
        let err = drain_err(reader);
        assert!(
            err.to_string().contains("truncated"),
            "truncated field must error: {err}"
        );
    }

    #[test]
    fn truncated_trailer_count_errors() {
        // The trailer line is cut before the count field's value: "T" with no
        // digits → the count column is absent/empty.
        let data = b"D00001 100\nT\n";
        let reader = fw_reader(&data[..], fw_types(), trailer_constraint(), Vec::new()).unwrap();
        let err = drain_err(reader);
        assert!(err.is_structural_count());
        assert!(
            err.to_string().contains("truncated") || err.to_string().contains("no value"),
            "{err}"
        );
    }

    #[test]
    fn record_type_field_name_is_rejected() {
        // A field literally named `record_type` would overwrite the stamped id.
        let types = vec![rtype(
            "detail",
            "D",
            vec![fw_field("record_type", 1, 1, Type::String)],
        )];
        let err = match fw_reader(&b""[..], types, Vec::new(), Vec::new()) {
            Ok(_) => panic!("expected a reserved-name error"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("reserved"), "{err}");
    }

    #[test]
    fn duplicate_tag_is_rejected() {
        let types = vec![
            rtype("a", "D", vec![fw_field("x", 1, 3, Type::String)]),
            rtype("b", "D", vec![fw_field("y", 1, 3, Type::String)]),
        ];
        let err = match fw_reader(&b""[..], types, Vec::new(), Vec::new()) {
            Ok(_) => panic!("expected a duplicate-tag error"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("unique"), "{err}");
    }

    #[test]
    fn incompatible_same_named_field_across_types_is_rejected() {
        // `ref` is string in one type, integer in another — one shared column
        // cannot carry both.
        let types = vec![
            rtype("a", "A", vec![fw_field("ref", 1, 4, Type::String)]),
            rtype("b", "B", vec![fw_field("ref", 1, 4, Type::Int)]),
        ];
        let err = match fw_reader(&b""[..], types, Vec::new(), Vec::new()) {
            Ok(_) => panic!("expected an incompatible-type error"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("incompatible"), "{err}");
    }

    #[test]
    fn structure_naming_unknown_record_is_rejected() {
        let bad = vec![StructureConstraint {
            record: "nonexistent".into(),
            count: "count".into(),
        }];
        let err = match fw_reader(&b""[..], fw_types(), bad, Vec::new()) {
            Ok(_) => panic!("expected an unknown-record error"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("no `records:`"), "{err}");
    }

    #[test]
    fn multi_char_pad_is_stripped_as_a_unit() {
        // A left-justified field padded with the multi-char string "ab":
        // "42abab" strips trailing "ab" runs as a unit → "42". The old reader
        // honored only the first pad char, so a multi-char pad leaked.
        let mut field = fw_field("v", 0, 6, Type::Int);
        field.justify = Some(Justify::Left);
        field.pad = Some("ab".into());
        let resolved = ResolvedField::from_column(&field).unwrap();
        let v = field::extract_value(&resolved, b"42abab", 1).unwrap();
        assert_eq!(v, Value::Integer(42));
    }

    #[test]
    fn csv_discriminates_and_honors_field_types() {
        let data = "H,2024-01-01\nD,1,100\nD,2,200\nT,2\n";
        let types = vec![
            rtype(
                "header",
                "H",
                vec![
                    csv_field("rec_type", Type::String),
                    csv_field("run_date", Type::String),
                ],
            ),
            rtype(
                "detail",
                "D",
                vec![
                    csv_field("rec_type", Type::String),
                    csv_field("id", Type::Int),
                    csv_field("amount", Type::Int),
                ],
            ),
            rtype(
                "trailer",
                "T",
                vec![
                    csv_field("rec_type", Type::String),
                    csv_field("count", Type::Int),
                ],
            ),
        ];
        let reader = MultiRecordReader::new_csv(
            Cursor::new(data.as_bytes().to_vec()),
            MultiRecordSpec {
                discriminator: csv_disc("rec_type"),
                record_types: types,
                structure: Vec::new(),
                header_tags: Vec::new(),
            },
            CsvDialect {
                delimiter: b',',
                quote_char: b'"',
                has_header: false,
            },
        )
        .unwrap();
        let recs = collect(reader);
        assert_eq!(recs.len(), 4);
        assert_eq!(recs[1].get("id"), Some(&Value::Integer(1)));
        assert_eq!(recs[1].get("amount"), Some(&Value::Integer(100)));
        assert_eq!(recs[1].get("run_date"), Some(&Value::Null));
    }

    #[test]
    fn csv_header_row_is_skipped() {
        // A textual column-header row precedes the data; has_header skips it.
        let data = "rec_type,id,amount\nD,1,100\nT,1\n";
        let types = vec![
            rtype(
                "detail",
                "D",
                vec![
                    csv_field("rec_type", Type::String),
                    csv_field("id", Type::Int),
                    csv_field("amount", Type::Int),
                ],
            ),
            rtype(
                "trailer",
                "T",
                vec![
                    csv_field("rec_type", Type::String),
                    csv_field("count", Type::Int),
                ],
            ),
        ];
        let reader = MultiRecordReader::new_csv(
            Cursor::new(data.as_bytes().to_vec()),
            MultiRecordSpec {
                discriminator: csv_disc("rec_type"),
                record_types: types,
                structure: vec![StructureConstraint {
                    record: "trailer".into(),
                    count: "count".into(),
                }],
                header_tags: Vec::new(),
            },
            CsvDialect {
                delimiter: b',',
                quote_char: b'"',
                has_header: true,
            },
        )
        .unwrap();
        let recs = collect(reader);
        // The "rec_type,id,amount" header row is skipped → only one D body row,
        // and the trailer count of 1 validates against it.
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].get("id"), Some(&Value::Integer(1)));
    }

    #[test]
    fn csv_discriminator_absent_from_a_type_errors() {
        let types = vec![
            rtype("header", "H", vec![csv_field("rec_type", Type::String)]),
            rtype("detail", "D", vec![csv_field("id", Type::String)]),
        ];
        let result = MultiRecordReader::new_csv(
            Cursor::new(Vec::new()),
            MultiRecordSpec {
                discriminator: csv_disc("rec_type"),
                record_types: types,
                structure: Vec::new(),
                header_tags: Vec::new(),
            },
            CsvDialect {
                delimiter: b',',
                quote_char: b'"',
                has_header: false,
            },
        );
        let err = match result {
            Ok(_) => panic!("expected discriminator-absent error"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("absent from record type"));
    }

    #[test]
    fn empty_file_yields_no_records() {
        let reader = fw_reader(&b""[..], fw_types(), Vec::new(), Vec::new()).unwrap();
        assert!(collect(reader).is_empty());
    }
}

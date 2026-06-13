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
//! stashes the first header-tag row, O(one row)), the running body count, and
//! the most recent trailer row's raw fields used to validate a trailer's
//! declared count at document close. Trailer rows arrive after the body they
//! close, so a trailer record type is validated as it streams rather than
//! surfaced as a `$doc` pre-scan section.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read};
use std::sync::Arc;

use chrono::NaiveDate;
use indexmap::IndexMap;

use clinker_record::schema_def::{
    Discriminator, FieldDef, FieldType, Justify, LineSeparator, RecordTypeDef, StructureConstraint,
};
use clinker_record::{Record, Schema, SchemaBuilder, Value};

use crate::bom::SkipBom;
use crate::envelope::{EnvelopeConfig, EnvelopeExtract, coerce_section_fields};
use crate::error::FormatError;
use crate::traits::FormatReader;

/// The lead column of the superset schema, carrying the matched record type's
/// declared `id` so a downstream `Route` discriminates on a stable name
/// independent of the discriminator's physical location.
pub const RECORD_TYPE_COLUMN: &str = "record_type";

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
    fields: Vec<ResolvedField>,
}

/// A field's extraction plan against a physical line, plus the superset
/// column slot it fills.
struct ResolvedField {
    /// The declared field name, used to key header pre-scan pairs against an
    /// envelope section's declared field schema.
    name: String,
    /// Index into the superset schema's column vector.
    column: usize,
    field_type: Option<FieldType>,
    format: Option<String>,
    /// Fixed-width byte range; `None` for CSV (positional column extraction).
    byte_range: Option<(usize, usize)>,
    /// CSV column index for this field; `None` for fixed-width.
    csv_column: Option<usize>,
    justify: Option<Justify>,
    pad: String,
    trim: bool,
}

/// The line scanner backing the reader — fixed-width fixed-length reads or a
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
    },
}

/// One physical line, decoded into the shape the discriminator and fields
/// extract from: raw bytes for fixed-width, parsed columns for CSV.
enum ScannedLine {
    FixedWidth(Vec<u8>),
    Csv(csv::StringRecord),
}

/// The resolved building blocks both backends produce from a `records:` list:
/// the static superset schema, the tag → record-type map, and a trailing
/// usize (the fixed-width record length, or the CSV discriminator column).
type ResolvedTypes = (Arc<Schema>, HashMap<String, ResolvedType>, usize);

/// Streaming multi-record flat-file reader.
///
/// Holds the line scanner, the static superset schema, a `tag → ResolvedType`
/// map, the running body count, and the optional trailer structural
/// constraint. Stamps each matched line as one record on the superset schema.
pub struct MultiRecordReader<R: Read> {
    scanner: LineScanner<R>,
    schema: Arc<Schema>,
    discrimination: Discrimination,
    /// Tag (trimmed) → resolved record type. A row whose tag is absent here is
    /// an unknown discriminator value.
    by_tag: HashMap<String, ResolvedType>,
    /// Whether an unknown discriminator value aborts the read (fail-fast) or
    /// dead-letters the row. Dead-lettering rides the document-DLQ seam: the
    /// driver's per-record error handling skips the row.
    fail_fast: bool,
    /// Trailer structural constraints: a record-type id whose declared count
    /// column must equal the actual body count at document close.
    structure: Vec<StructureConstraint>,
    /// Running count of body rows (every matched row that is neither a header
    /// nor a trailer type), checked against a trailer's declared count.
    body_count: u64,
    /// Header-type tags whose first row is captured by the bounded pre-scan and
    /// excluded from the body count. Known at construction from the source's
    /// `envelope:` config, so the pre-scan runs before any body record and a
    /// header row is never misclassified as a body record.
    header_tags: Vec<String>,
    /// Header-type ids matched to `header_tags`, kept for the body-skip
    /// pointer check.
    header_ids: Vec<Arc<str>>,
    /// Trailer-type ids (the `record` side of a `StructureConstraint`). A row
    /// of one of these types is the trailer and is excluded from the body
    /// count; its raw fields are stashed in `last_trailer`.
    trailer_ids: Vec<Arc<str>>,
    /// Captured header rows, keyed by record-type tag: the first row of each
    /// header type, as declared field-name → raw text pairs. Filled by the
    /// one-time pre-scan and read by `prepare_document` for section coercion.
    captured_headers: HashMap<String, Vec<(String, String)>>,
    /// Whether the one-time header pre-scan has run. It runs lazily on the
    /// first `next_record` or `prepare_document`, whichever the driver calls
    /// first, so the header region is consumed exactly once.
    prescanned: bool,
    /// The non-header row that terminated the pre-scan, held so the body stream
    /// consumes it as its first row rather than dropping it.
    pending_line: Option<ScannedLine>,
    /// The most recent trailer row: its type id and raw field-name → text map,
    /// resolved into a declared count at end-of-input structural validation.
    last_trailer: Option<(Arc<str>, HashMap<String, String>)>,
    done: bool,
}

/// The format-agnostic construction inputs for a [`MultiRecordReader`],
/// shared by the CSV and fixed-width entry points.
///
/// Bundled into one struct so the constructors stay below the argument-count
/// limit and so the executor factory builds the spec once and reuses it for
/// either backend.
pub struct MultiRecordSpec<'a> {
    /// How each line's record type is identified (a byte range for
    /// fixed-width, a named column for CSV).
    pub discriminator: &'a Discriminator,
    /// The declared record types — each with its tag and per-field layout.
    pub record_types: &'a [RecordTypeDef],
    /// Trailer structural-count constraints validated at document close.
    pub structure: Vec<StructureConstraint>,
    /// Header record-type tags captured by the bounded pre-scan and excluded
    /// from the body stream (the `record_type` envelope extracts).
    pub header_tags: Vec<String>,
    /// Whether an unknown discriminator value aborts the run (fail-fast) or
    /// dead-letters the row.
    pub fail_fast: bool,
}

impl<R: Read> MultiRecordReader<R> {
    /// Build a fixed-width multi-record reader from the discriminator, record
    /// types, and structural constraints of a `records:` schema.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::FixedWidth`] when the discriminator lacks a byte
    /// `start`, a record type declares no `tag`, or a field omits its
    /// `start`/`width`.
    pub fn new_fixed_width(
        reader: R,
        spec: MultiRecordSpec<'_>,
        separator: LineSeparator,
    ) -> Result<Self, FormatError> {
        let start = spec.discriminator.start.ok_or_else(|| {
            FormatError::FixedWidth(
                "multi-record fixed-width discriminator requires a byte `start`".into(),
            )
        })?;
        let width = spec.discriminator.width.unwrap_or(1);
        let discrimination = Discrimination::ByteRange { start, width };

        let (schema, by_tag, record_length) = resolve_fixed_width(spec.record_types, start, width)?;
        let scanner = LineScanner::FixedWidth {
            reader: BufReader::new(SkipBom::new(reader)),
            separator,
            record_length,
            line_buf: Vec::with_capacity(256),
        };
        Self::assemble(scanner, schema, discrimination, by_tag, spec)
    }

    /// Build a CSV multi-record reader from the discriminator, record types,
    /// and structural constraints of a `records:` schema.
    ///
    /// The discriminator names a field; its column index is the position of
    /// that field in each record type's declared field list and must be
    /// consistent across every type (typically the first column).
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::Csv`] when the discriminator omits its `field`
    /// name, a record type declares no `tag`, or the discriminator field is
    /// absent from a type's field list or sits at a different column index
    /// across types.
    pub fn new_csv(
        reader: R,
        spec: MultiRecordSpec<'_>,
        delimiter: u8,
        quote_char: u8,
    ) -> Result<Self, FormatError> {
        let field = spec.discriminator.field.as_deref().ok_or_else(|| {
            FormatError::Csv(csv_error(
                "multi-record CSV discriminator requires a `field` name",
            ))
        })?;
        let (schema, by_tag, column) = resolve_csv(spec.record_types, field)?;
        let discrimination = Discrimination::Column(column);
        let csv_reader = csv::ReaderBuilder::new()
            .delimiter(delimiter)
            .quote(quote_char)
            // Heterogeneous record types have different column counts; per-type
            // column-count validation happens at extraction, so the underlying
            // parser must accept ragged rows rather than erroring file-wide.
            .flexible(true)
            .has_headers(false)
            .from_reader(SkipBom::new(reader));
        let scanner = LineScanner::Csv {
            reader: csv_reader,
            record_buf: csv::StringRecord::new(),
        };
        Self::assemble(scanner, schema, discrimination, by_tag, spec)
    }

    /// Assemble the reader from its resolved parts, deriving the header/trailer
    /// id sets used for body-count bookkeeping and pre-scan classification.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::SchemaInference`] when a declared `header_tags`
    /// entry matches no record type.
    fn assemble(
        scanner: LineScanner<R>,
        schema: Arc<Schema>,
        discrimination: Discrimination,
        by_tag: HashMap<String, ResolvedType>,
        spec: MultiRecordSpec<'_>,
    ) -> Result<Self, FormatError> {
        let MultiRecordSpec {
            structure,
            header_tags,
            fail_fast,
            ..
        } = spec;
        // A trailer type is the `record` side of a structural constraint: its
        // row carries the declared count and must not be tallied as body.
        let trailer_ids: Vec<Arc<str>> = by_tag
            .values()
            .filter(|rt| {
                structure
                    .iter()
                    .any(|c| c.record.as_str() == rt.id.as_ref())
            })
            .map(|rt| Arc::clone(&rt.id))
            .collect();
        // Resolve each header tag to its record-type id up front so the body
        // stream can skip header rows by pointer check without re-reading the
        // envelope config.
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
            fail_fast,
            structure,
            body_count: 0,
            header_tags,
            header_ids,
            trailer_ids,
            captured_headers: HashMap::new(),
            prescanned: false,
            pending_line: None,
            last_trailer: None,
            done: false,
        })
    }

    /// Read one physical line from the scanner, or `None` at end of input.
    fn scan_line(&mut self) -> Result<Option<ScannedLine>, FormatError> {
        match &mut self.scanner {
            LineScanner::FixedWidth {
                reader,
                separator,
                record_length,
                line_buf,
            } => {
                if !read_fixed_line(reader, separator, *record_length, line_buf)? {
                    return Ok(None);
                }
                Ok(Some(ScannedLine::FixedWidth(line_buf.clone())))
            }
            LineScanner::Csv { reader, record_buf } => {
                if !reader.read_record(record_buf)? {
                    return Ok(None);
                }
                Ok(Some(ScannedLine::Csv(record_buf.clone())))
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
                        "line too short for discriminator: need {end} bytes, got {}",
                        bytes.len()
                    ))
                })?;
                let tag = std::str::from_utf8(slice).map_err(|_| {
                    FormatError::FixedWidth("invalid UTF-8 in discriminator byte range".into())
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
    /// column plus every declared field of the matched type coerced into its
    /// superset slot; unfilled slots stay `Value::Null`.
    fn build_record(&self, rt: &ResolvedType, line: &ScannedLine) -> Result<Record, FormatError> {
        let mut values: Vec<Value> = vec![Value::Null; self.schema.column_count()];
        values[0] = Value::String(rt.id.as_ref().into());
        for field in &rt.fields {
            values[field.column] = extract_field_value(field, line)?;
        }
        Ok(Record::new(Arc::clone(&self.schema), values))
    }

    /// Run the one-time header pre-scan: forward-scan the contiguous header
    /// region at the file head, capturing the first row of each declared header
    /// tag, and stop at the first non-header row. Idempotent — the first
    /// `next_record` or `prepare_document` triggers it; later calls are no-ops.
    ///
    /// The pending non-header row that terminates the scan is stashed in
    /// `pending_line` so `pull_next` consumes it as the first body row rather
    /// than dropping it. The scan retains O(one header row per section).
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
                // A repeated header of an already-captured tag: first wins.
                continue;
            }
            let rt = self
                .by_tag
                .get(&tag)
                .expect("header tag resolved to a record type at construction");
            let pairs = named_pairs(rt, &line)?;
            self.captured_headers.insert(tag, pairs);
        }
    }

    /// Pull the next matched body record, consuming header rows (captured by
    /// the pre-scan) and trailer rows (they feed structural validation)
    /// transparently, or `None` at clean end of input.
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
            if !self.by_tag.contains_key(&tag) {
                self.reject_unknown_tag(&tag)?;
            }
            let id = Arc::clone(&self.by_tag[&tag].id);
            if self.header_ids.iter().any(|h| Arc::ptr_eq(h, &id)) {
                // A header type's row outside the contiguous head region (the
                // pre-scan already captured the first occurrence): never a body
                // record.
                continue;
            }
            if self.trailer_ids.iter().any(|t| Arc::ptr_eq(t, &id)) {
                let raw = named_pairs(&self.by_tag[&tag], &line)?;
                self.last_trailer = Some((id, raw.into_iter().collect()));
                continue;
            }
            self.body_count += 1;
            let rt = &self.by_tag[&tag];
            return Ok(Some(self.build_record(rt, &line)?));
        }
    }

    /// Surface an unknown discriminator value: fail-fast aborts with the E345
    /// diagnostic; otherwise the row dead-letters through the document-DLQ
    /// seam (the driver skips a record-level error).
    fn reject_unknown_tag(&self, tag: &str) -> Result<(), FormatError> {
        if self.fail_fast {
            return Err(FormatError::FixedWidth(format!(
                "E345 unknown record-type discriminator {tag:?}: no `records:` entry declares \
                 this tag. Declare a record type with `tag: {tag}`, or set a non-fail-fast DLQ \
                 strategy to dead-letter unmatched rows. See: clinker explain --code E345"
            )));
        }
        Err(FormatError::InvalidRecord {
            row: self.body_count + 1,
            message: format!(
                "E345 unknown record-type discriminator {tag:?}: no `records:` entry declares \
                 this tag. See: clinker explain --code E345"
            ),
        })
    }

    /// Validate every trailer structural constraint against the streamed body
    /// count at end of input.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::InvalidRecord`] when a trailer's declared count
    /// disagrees with the actual body count it closes.
    fn validate_structure(&self) -> Result<(), FormatError> {
        for constraint in &self.structure {
            let Some(declared) = self.last_trailer_count(&constraint.record, &constraint.count)?
            else {
                continue;
            };
            if declared != self.body_count {
                // A trailer-count disagreement is an envelope structural-count
                // failure, the same class as X12 `SE` / EDIFACT `UNT` / HL7
                // `BTS`: under `dlq_granularity: document` it condemns the file
                // rather than aborting the run.
                return Err(FormatError::multi_record_structural_count(format!(
                    "trailer record '{}' declares count {} (field '{}'), but the file streamed \
                     {} body records",
                    constraint.record, declared, constraint.count, self.body_count
                )));
            }
        }
        Ok(())
    }

    /// The declared count carried by the most recent trailer row of the named
    /// record type, parsed from its `count` field. `None` when no trailer of
    /// that type was seen.
    fn last_trailer_count(
        &self,
        record_id: &str,
        count_field: &str,
    ) -> Result<Option<u64>, FormatError> {
        match self.last_trailer.as_ref() {
            Some((id, fields)) if id.as_ref() == record_id => {
                let raw = fields
                    .get(count_field)
                    .ok_or_else(|| FormatError::InvalidRecord {
                        row: self.body_count,
                        message: format!(
                            "trailer record '{record_id}' declares no field '{count_field}' to \
                         validate the body count against"
                        ),
                    })?;
                let parsed: u64 = raw.trim().parse().map_err(|_| FormatError::InvalidRecord {
                    row: self.body_count,
                    message: format!(
                        "trailer record '{record_id}' field '{count_field}' value {raw:?} is not \
                         a non-negative integer count"
                    ),
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
        // The header tags were resolved at construction (from the same
        // `envelope:` config), so the pre-scan has already captured each
        // declared header row by the time the driver calls this — whether it
        // ran here or on the first `next_record`.
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
            // A header tag with no captured row means the file carried no row
            // of that type; its `$doc` fields resolve to `Value::Null` per the
            // absent-section convention, so it is simply omitted here.
            let Some(pairs) = self.captured_headers.get(tag) else {
                continue;
            };
            let typed = coerce_section_fields(pairs.clone(), &section.fields)
                .map_err(FormatError::SchemaInference)?;
            out.insert(name.as_str().into(), Value::Map(Box::new(typed)));
        }
        Ok(out)
    }
}

/// Resolve the fixed-width superset schema, tag map, and record length.
///
/// The superset schema is `[record_type, <union of every type's field names in
/// declaration order>]`. Returns the tag → resolved type map and the maximum
/// line length any field reaches (drives the `LineSeparator::None` fixed read).
fn resolve_fixed_width(
    record_types: &[RecordTypeDef],
    disc_start: usize,
    disc_width: usize,
) -> Result<ResolvedTypes, FormatError> {
    let mut columns: Vec<String> = vec![RECORD_TYPE_COLUMN.to_string()];
    let mut column_index: HashMap<String, usize> = HashMap::new();
    column_index.insert(RECORD_TYPE_COLUMN.to_string(), 0);
    let mut by_tag: HashMap<String, ResolvedType> = HashMap::new();
    let mut record_length = disc_start + disc_width;

    for rt in record_types {
        let tag = require_tag(&rt.tag, &rt.id).map_err(FormatError::FixedWidth)?;
        let id: Arc<str> = Arc::from(rt.id.as_str());
        let mut fields = Vec::with_capacity(rt.fields.len());
        for f in &rt.fields {
            let col = intern_column(&mut columns, &mut column_index, &f.name);
            let (start, width) = fixed_width_range(f)?;
            record_length = record_length.max(start + width);
            fields.push(ResolvedField {
                name: f.name.clone(),
                column: col,
                field_type: f.field_type.clone(),
                format: f.format.clone(),
                byte_range: Some((start, width)),
                csv_column: None,
                justify: f.justify.clone(),
                pad: f.pad.clone().unwrap_or_else(|| " ".into()),
                trim: f.trim.unwrap_or(true),
            });
        }
        by_tag.insert(tag, ResolvedType { id, fields });
    }

    let schema = build_superset_schema(columns);
    Ok((schema, by_tag, record_length))
}

/// Resolve the CSV superset schema, tag map, and the discriminator column
/// index (consistent across every record type).
fn resolve_csv(
    record_types: &[RecordTypeDef],
    disc_field: &str,
) -> Result<ResolvedTypes, FormatError> {
    let mut columns: Vec<String> = vec![RECORD_TYPE_COLUMN.to_string()];
    let mut column_index: HashMap<String, usize> = HashMap::new();
    column_index.insert(RECORD_TYPE_COLUMN.to_string(), 0);
    let mut by_tag: HashMap<String, ResolvedType> = HashMap::new();
    let mut disc_column: Option<usize> = None;

    for rt in record_types {
        let tag = require_tag(&rt.tag, &rt.id).map_err(|m| FormatError::Csv(csv_error(&m)))?;
        let id: Arc<str> = Arc::from(rt.id.as_str());
        // The discriminator's column position is its index in this type's
        // field list; it must be present and consistent across every type so
        // the tag can be read before the type is known.
        let local = rt
            .fields
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
        let mut fields = Vec::with_capacity(rt.fields.len());
        for (csv_idx, f) in rt.fields.iter().enumerate() {
            let col = intern_column(&mut columns, &mut column_index, &f.name);
            fields.push(ResolvedField {
                name: f.name.clone(),
                column: col,
                field_type: f.field_type.clone(),
                format: f.format.clone(),
                byte_range: None,
                csv_column: Some(csv_idx),
                justify: None,
                pad: " ".into(),
                trim: true,
            });
        }
        by_tag.insert(tag, ResolvedType { id, fields });
    }

    let column = disc_column.ok_or_else(|| {
        FormatError::Csv(csv_error(
            "multi-record CSV schema declares no record types",
        ))
    })?;
    let schema = build_superset_schema(columns);
    Ok((schema, by_tag, column))
}

/// Build the superset [`Schema`] from the ordered column-name list.
fn build_superset_schema(columns: Vec<String>) -> Arc<Schema> {
    columns
        .into_iter()
        .map(Box::<str>::from)
        .collect::<SchemaBuilder>()
        .build()
}

/// Intern a field name into the shared superset column list, returning its
/// index. A name shared across record types maps to one column slot, so a
/// union field declared by two types is not duplicated.
fn intern_column(
    columns: &mut Vec<String>,
    column_index: &mut HashMap<String, usize>,
    name: &str,
) -> usize {
    if let Some(&idx) = column_index.get(name) {
        return idx;
    }
    let idx = columns.len();
    columns.push(name.to_string());
    column_index.insert(name.to_string(), idx);
    idx
}

/// Require a non-empty record-type tag, returning the trimmed tag or a
/// format-neutral error message naming the offending type id. Each caller
/// wraps the message in its own format-specific [`FormatError`] variant.
fn require_tag(tag: &str, id: &str) -> Result<String, String> {
    if tag.is_empty() {
        return Err(format!(
            "record type '{id}' declares an empty `tag`; a discriminator tag is required"
        ));
    }
    Ok(tag.trim().to_string())
}

/// Resolve a fixed-width field's `(start, width)` byte range from its
/// `start` + `width`/`end`.
fn fixed_width_range(f: &FieldDef) -> Result<(usize, usize), FormatError> {
    let start = f.start.ok_or_else(|| {
        FormatError::FixedWidth(format!(
            "field '{}': multi-record fixed-width field must declare `start`",
            f.name
        ))
    })?;
    let width = if let Some(w) = f.width {
        w
    } else if let Some(end) = f.end {
        end.checked_sub(start).ok_or_else(|| {
            FormatError::FixedWidth(format!(
                "field '{}': `end` ({end}) must be >= `start` ({start})",
                f.name
            ))
        })?
    } else {
        return Err(FormatError::FixedWidth(format!(
            "field '{}': multi-record fixed-width field must declare `width` or `end`",
            f.name
        )));
    };
    if width == 0 {
        return Err(FormatError::FixedWidth(format!(
            "field '{}': width must be > 0",
            f.name
        )));
    }
    Ok((start, width))
}

/// Extract one field's typed value from a scanned line.
fn extract_field_value(field: &ResolvedField, line: &ScannedLine) -> Result<Value, FormatError> {
    let raw = raw_field_text(field, line)?;
    if raw.is_empty() {
        return Ok(Value::Null);
    }
    parse_typed_value(&raw, field)
}

/// Extract a field's raw (un-typed) text from a scanned line, stripping
/// fixed-width padding / CSV whitespace.
fn raw_field_text(field: &ResolvedField, line: &ScannedLine) -> Result<String, FormatError> {
    match (line, field.byte_range, field.csv_column) {
        (ScannedLine::FixedWidth(bytes), Some((start, width)), _) => {
            let end = start + width;
            let slice = if end <= bytes.len() {
                &bytes[start..end]
            } else if start < bytes.len() {
                &bytes[start..]
            } else {
                b""
            };
            let s = std::str::from_utf8(slice).map_err(|e| {
                FormatError::FixedWidth(format!("field '{}': invalid UTF-8: {e}", field.name))
            })?;
            Ok(strip_fixed_padding(s, field).to_string())
        }
        (ScannedLine::Csv(rec), _, Some(idx)) => Ok(rec.get(idx).unwrap_or("").trim().to_string()),
        // The extraction plan is bound to the line format at construction.
        _ => Err(FormatError::SchemaInference(
            "field/line-format mismatch in multi-record extraction".into(),
        )),
    }
}

/// Strip fixed-width padding from a raw slice per the field's justification.
fn strip_fixed_padding<'a>(raw: &'a str, field: &ResolvedField) -> &'a str {
    if !field.trim {
        return raw;
    }
    let pad_char = field.pad.chars().next().unwrap_or(' ');
    let stripped = match field.justify {
        Some(Justify::Right) => raw.trim_start_matches(pad_char),
        Some(Justify::Left) | None => raw.trim_end_matches(pad_char),
    };
    stripped.trim()
}

/// Parse a non-empty raw field string into its declared type, defaulting to a
/// string when no type is declared.
fn parse_typed_value(raw: &str, field: &ResolvedField) -> Result<Value, FormatError> {
    match &field.field_type {
        Some(FieldType::Integer) => raw
            .parse::<i64>()
            .map(Value::Integer)
            .map_err(|e| typed_err(field, raw, "integer", &e.to_string())),
        Some(FieldType::Float | FieldType::Decimal) => raw
            .parse::<f64>()
            .map(Value::Float)
            .map_err(|e| typed_err(field, raw, "float", &e.to_string())),
        Some(FieldType::Boolean) => match raw.to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" | "y" => Ok(Value::Bool(true)),
            "false" | "0" | "no" | "n" => Ok(Value::Bool(false)),
            _ => Err(typed_err(
                field,
                raw,
                "boolean",
                "unrecognized boolean literal",
            )),
        },
        Some(FieldType::Date) => {
            let fmt = field.format.as_deref().unwrap_or("%Y-%m-%d");
            NaiveDate::parse_from_str(raw, fmt)
                .map(Value::Date)
                .map_err(|e| typed_err(field, raw, "date", &e.to_string()))
        }
        // String / DateTime / Object / Array carry the raw text; CXL coercion
        // and downstream schema binding refine them. This mirrors the
        // all-string posture of the CSV and EDI readers.
        _ => Ok(Value::String(raw.into())),
    }
}

/// Build a typed-parse [`FormatError`] naming the field, value, and declared
/// type.
fn typed_err(field: &ResolvedField, raw: &str, ty: &str, detail: &str) -> FormatError {
    FormatError::InvalidRecord {
        row: 0,
        message: format!(
            "multi-record field '{}': cannot parse {raw:?} as {ty}: {detail}",
            field.name
        ),
    }
}

/// Pair each declared field of a matched type with its raw string value,
/// keyed by the declared field name. Drives header-section coercion (the
/// pre-scan) and trailer-field capture.
fn named_pairs(
    rt: &ResolvedType,
    line: &ScannedLine,
) -> Result<Vec<(String, String)>, FormatError> {
    let mut pairs = Vec::with_capacity(rt.fields.len());
    for field in &rt.fields {
        pairs.push((field.name.clone(), raw_field_text(field, line)?));
    }
    Ok(pairs)
}

/// Read one fixed-width physical line (delimited or fixed-length) into
/// `line_buf`, returning `false` at end of input.
fn read_fixed_line<R: Read>(
    reader: &mut BufReader<SkipBom<R>>,
    separator: &LineSeparator,
    record_length: usize,
    line_buf: &mut Vec<u8>,
) -> Result<bool, FormatError> {
    line_buf.clear();
    match separator {
        LineSeparator::Lf => {
            let n = reader.read_until(b'\n', line_buf)?;
            if n == 0 {
                return Ok(false);
            }
            if line_buf.last() == Some(&b'\n') {
                line_buf.pop();
            }
        }
        LineSeparator::CrLf => {
            let n = reader.read_until(b'\n', line_buf)?;
            if n == 0 {
                return Ok(false);
            }
            if line_buf.ends_with(b"\r\n") {
                line_buf.truncate(line_buf.len() - 2);
            } else if line_buf.last() == Some(&b'\n') {
                line_buf.pop();
            }
        }
        LineSeparator::None => {
            line_buf.resize(record_length, 0);
            let mut total = 0;
            while total < record_length {
                let n = reader.read(&mut line_buf[total..])?;
                if n == 0 {
                    if total == 0 {
                        return Ok(false);
                    }
                    return Err(FormatError::InvalidRecord {
                        row: 0,
                        message: format!(
                            "incomplete fixed-width record: expected {record_length} bytes, got \
                             {total}"
                        ),
                    });
                }
                total += n;
            }
        }
    }
    Ok(true)
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

    fn fw_field(name: &str, start: usize, width: usize, ty: Option<FieldType>) -> FieldDef {
        FieldDef {
            name: name.into(),
            field_type: ty,
            required: None,
            format: None,
            coerce: None,
            default: None,
            allowed_values: None,
            alias: None,
            inherits: None,
            start: Some(start),
            width: Some(width),
            end: None,
            justify: None,
            pad: None,
            trim: None,
            truncation: None,
            precision: None,
            scale: None,
            path: None,
            drop: None,
            record: None,
        }
    }

    fn csv_field(name: &str, ty: Option<FieldType>) -> FieldDef {
        FieldDef {
            name: name.into(),
            field_type: ty,
            required: None,
            format: None,
            coerce: None,
            default: None,
            allowed_values: None,
            alias: None,
            inherits: None,
            start: None,
            width: None,
            end: None,
            justify: None,
            pad: None,
            trim: None,
            truncation: None,
            precision: None,
            scale: None,
            path: None,
            drop: None,
            record: None,
        }
    }

    fn rtype(id: &str, tag: &str, fields: Vec<FieldDef>) -> RecordTypeDef {
        RecordTypeDef {
            id: id.into(),
            tag: tag.into(),
            description: None,
            parent: None,
            join_key: None,
            fields,
        }
    }

    fn fw_disc(start: usize, width: usize) -> Discriminator {
        Discriminator {
            start: Some(start),
            width: Some(width),
            field: None,
        }
    }

    fn fw_types() -> Vec<RecordTypeDef> {
        vec![
            rtype("header", "H", vec![fw_field("batch", 1, 9, None)]),
            rtype(
                "detail",
                "D",
                vec![
                    fw_field("id", 1, 5, Some(FieldType::Integer)),
                    fw_field("amount", 6, 4, Some(FieldType::Integer)),
                ],
            ),
            rtype(
                "trailer",
                "T",
                vec![fw_field("count", 1, 5, Some(FieldType::Integer))],
            ),
        ]
    }

    fn collect<R: Read + Send>(mut reader: MultiRecordReader<R>) -> Vec<Record> {
        let mut out = Vec::new();
        while let Some(r) = reader.next_record().unwrap() {
            out.push(r);
        }
        out
    }

    #[test]
    fn fixed_width_discriminates_body_records_on_superset_schema() {
        // H header, two D bodies, T trailer. Without an envelope the header is
        // an ordinary record type, so the H row streams as a body record too;
        // the discriminator column tells them apart.
        let data = b"HREPORT   \nD00001 100\nD00002 200\nT00002    \n";
        let reader = MultiRecordReader::new_fixed_width(
            &data[..],
            MultiRecordSpec {
                discriminator: &fw_disc(0, 1),
                record_types: &fw_types(),
                structure: Vec::new(),
                header_tags: Vec::new(),
                fail_fast: true,
            },
            LineSeparator::Lf,
        )
        .unwrap();
        let recs = collect(reader);
        // 1 header + 2 detail + 1 trailer = 4 records (no envelope, no
        // structural constraint, so every type is a body record).
        assert_eq!(recs.len(), 4);
        assert_eq!(
            recs[0].get(RECORD_TYPE_COLUMN),
            Some(&Value::String("header".into()))
        );
        assert_eq!(
            recs[1].get(RECORD_TYPE_COLUMN),
            Some(&Value::String("detail".into()))
        );
        assert_eq!(recs[1].get("id"), Some(&Value::Integer(1)));
        assert_eq!(recs[1].get("amount"), Some(&Value::Integer(100)));
        // A detail row leaves the header/trailer-only `batch`/`count` columns
        // null — the superset slots it does not fill.
        assert_eq!(recs[1].get("batch"), Some(&Value::Null));
        assert_eq!(recs[1].get("count"), Some(&Value::Null));
        assert_eq!(
            recs[3].get(RECORD_TYPE_COLUMN),
            Some(&Value::String("trailer".into()))
        );
    }

    #[test]
    fn unknown_tag_fail_fast_errors_with_e345() {
        let data = b"D00001 100\nX99999 999\n";
        let mut reader = MultiRecordReader::new_fixed_width(
            &data[..],
            MultiRecordSpec {
                discriminator: &fw_disc(0, 1),
                record_types: &fw_types(),
                structure: Vec::new(),
                header_tags: Vec::new(),
                fail_fast: true,
            },
            LineSeparator::Lf,
        )
        .unwrap();
        // First row is a valid detail.
        assert!(reader.next_record().unwrap().is_some());
        let err = reader.next_record().unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("E345"), "expected E345 in: {msg}");
        assert!(msg.contains("\"X\""), "should name the unknown tag: {msg}");
    }

    #[test]
    fn unknown_tag_non_fail_fast_dead_letters_record() {
        let data = b"D00001 100\nX99999 999\nD00002 200\n";
        let mut reader = MultiRecordReader::new_fixed_width(
            &data[..],
            MultiRecordSpec {
                discriminator: &fw_disc(0, 1),
                record_types: &fw_types(),
                structure: Vec::new(),
                header_tags: Vec::new(),
                fail_fast: false,
            },
            LineSeparator::Lf,
        )
        .unwrap();
        // First detail streams.
        assert!(reader.next_record().unwrap().is_some());
        // The unknown row surfaces a record-level (DLQ-able) error, not a
        // fail-fast abort.
        let err = reader.next_record().unwrap_err();
        assert!(matches!(err, FormatError::InvalidRecord { .. }));
        assert!(err.to_string().contains("E345"));
    }

    #[test]
    fn trailer_count_matches_body_count() {
        // Envelope declares H as a header section and the trailer T's count is
        // structurally validated; two D body rows; T claims 2.
        let data = b"HREPORT   \nD00001 100\nD00002 200\nT00002    \n";
        let structure = vec![StructureConstraint {
            record: "trailer".into(),
            count: "count".into(),
        }];
        let mut reader = MultiRecordReader::new_fixed_width(
            &data[..],
            MultiRecordSpec {
                discriminator: &fw_disc(0, 1),
                record_types: &fw_types(),
                structure,
                header_tags: vec!["H".to_string()],
                fail_fast: true,
            },
            LineSeparator::Lf,
        )
        .unwrap();
        // Pre-scan the H header so the H row is captured (not a body record).
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
        // Body streams: two D rows; the T trailer feeds structural validation,
        // which passes at end of input (count 2 == 2 body rows).
        let recs = collect(reader);
        assert_eq!(recs.len(), 2);
        assert!(
            recs.iter()
                .all(|r| r.get(RECORD_TYPE_COLUMN) == Some(&Value::String("detail".into())))
        );
    }

    #[test]
    fn trailer_count_mismatch_errors() {
        // T claims 5 but only two D rows stream.
        let data = b"D00001 100\nD00002 200\nT00005    \n";
        let structure = vec![StructureConstraint {
            record: "trailer".into(),
            count: "count".into(),
        }];
        let mut reader = MultiRecordReader::new_fixed_width(
            &data[..],
            MultiRecordSpec {
                discriminator: &fw_disc(0, 1),
                record_types: &fw_types(),
                structure,
                header_tags: Vec::new(),
                fail_fast: true,
            },
            LineSeparator::Lf,
        )
        .unwrap();
        let err = loop {
            match reader.next_record() {
                Ok(Some(_)) => continue,
                Ok(None) => panic!("expected a trailer-count mismatch"),
                Err(e) => break e,
            }
        };
        let msg = err.to_string();
        assert!(msg.contains("declares count 5"), "msg: {msg}");
        assert!(msg.contains("2 body records"), "msg: {msg}");
    }

    #[test]
    fn csv_discriminates_on_named_column() {
        // rec_type is column 0 in every type; ragged rows (different column
        // counts per type) parse under flexible(true).
        let data = "H,2024-01-01\nD,1,100\nD,2,200\nT,2\n";
        let types = vec![
            rtype(
                "header",
                "H",
                vec![csv_field("rec_type", None), csv_field("run_date", None)],
            ),
            rtype(
                "detail",
                "D",
                vec![
                    csv_field("rec_type", None),
                    csv_field("id", Some(FieldType::Integer)),
                    csv_field("amount", Some(FieldType::Integer)),
                ],
            ),
            rtype(
                "trailer",
                "T",
                vec![
                    csv_field("rec_type", None),
                    csv_field("count", Some(FieldType::Integer)),
                ],
            ),
        ];
        let disc = Discriminator {
            start: None,
            width: None,
            field: Some("rec_type".into()),
        };
        let reader = MultiRecordReader::new_csv(
            Cursor::new(data.as_bytes().to_vec()),
            MultiRecordSpec {
                discriminator: &disc,
                record_types: &types,
                structure: Vec::new(),
                header_tags: Vec::new(),
                fail_fast: true,
            },
            b',',
            b'"',
        )
        .unwrap();
        let recs = collect(reader);
        // 1 H + 2 D + 1 T = 4 (no envelope/structure).
        assert_eq!(recs.len(), 4);
        assert_eq!(recs[1].get("id"), Some(&Value::Integer(1)));
        assert_eq!(recs[1].get("amount"), Some(&Value::Integer(100)));
        // A ragged D row does not fill the H-only run_date column.
        assert_eq!(recs[1].get("run_date"), Some(&Value::Null));
    }

    #[test]
    fn csv_discriminator_absent_from_a_type_errors() {
        let types = vec![
            rtype("header", "H", vec![csv_field("rec_type", None)]),
            // detail omits rec_type — construction must reject it.
            rtype("detail", "D", vec![csv_field("id", None)]),
        ];
        let disc = Discriminator {
            start: None,
            width: None,
            field: Some("rec_type".into()),
        };
        let result = MultiRecordReader::new_csv(
            Cursor::new(Vec::new()),
            MultiRecordSpec {
                discriminator: &disc,
                record_types: &types,
                structure: Vec::new(),
                header_tags: Vec::new(),
                fail_fast: true,
            },
            b',',
            b'"',
        );
        let err = match result {
            Ok(_) => panic!("expected discriminator-absent error"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("absent from record type"));
    }
}

//! Shared fixed-width field machinery.
//!
//! The single-record [`crate::fixed_width::reader::FixedWidthReader`] and the
//! multi-record [`crate::multi_record::MultiRecordReader`] both slice a line
//! into byte-positioned fields, strip padding by justification, and coerce the
//! trimmed text to a declared type. Hosting that logic here keeps the two
//! readers byte-for-byte identical — a declared `type: integer` (or `float`,
//! or `boolean`) parses the same way regardless of whether the file is single-
//! or multi-record — instead of two forks that drift apart.

use std::io::{BufRead, BufReader, Read};

use chrono::NaiveDate;

use clinker_record::Value;
use clinker_record::schema_def::{FieldDef, FieldType, Justify, LineSeparator};

use crate::error::FormatError;

/// A fixed-width field resolved to a concrete byte range and parse policy.
///
/// `width` is computed once from the declared `width` or `end - start`, so the
/// `width`/`end` mutual exclusivity and the `>0` invariant are enforced at
/// construction, not per row.
#[derive(Debug, Clone)]
pub struct ResolvedField {
    pub name: String,
    pub start: usize,
    pub width: usize,
    pub field_type: Option<FieldType>,
    pub format: Option<String>,
    pub justify: Option<Justify>,
    pub pad: String,
    pub trim: bool,
}

impl ResolvedField {
    /// Resolve a [`FieldDef`] into a byte-positioned field.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::InvalidRecord`] when the field omits `start`,
    /// declares both `width` and `end`, declares neither, declares `end <
    /// start`, or resolves to a zero width.
    pub fn from_field_def(f: &FieldDef) -> Result<Self, FormatError> {
        let start = f
            .start
            .ok_or_else(|| invalid_field(&f.name, "must have 'start'"))?;

        let width = match (f.width, f.end) {
            (Some(_), Some(_)) => {
                return Err(invalid_field(
                    &f.name,
                    "'width' and 'end' are mutually exclusive — declare one",
                ));
            }
            (Some(w), None) => w,
            (None, Some(end)) => end
                .checked_sub(start)
                .ok_or_else(|| invalid_field(&f.name, "'end' must be >= 'start'"))?,
            (None, None) => {
                return Err(invalid_field(&f.name, "must have 'width' or 'end'"));
            }
        };

        if width == 0 {
            return Err(invalid_field(&f.name, "width must be > 0"));
        }

        Ok(ResolvedField {
            name: f.name.clone(),
            start,
            width,
            field_type: f.field_type.clone(),
            format: f.format.clone(),
            justify: f.justify.clone(),
            pad: f.pad.clone().unwrap_or_else(|| " ".into()),
            trim: f.trim.unwrap_or(true),
        })
    }

    /// The first byte position past this field's range.
    pub fn end(&self) -> usize {
        self.start + self.width
    }
}

/// Read one physical record line into `buf`, returning `false` at end of input.
///
/// `Lf` / `CrLf` read up to (and strip) the newline; `None` reads exactly
/// `record_length` bytes (a delimiter-free fixed block). A short final block
/// under `None` is an incomplete-record error.
///
/// # Errors
///
/// Returns [`FormatError::Io`] on a read fault, or [`FormatError::InvalidRecord`]
/// when a `None`-separated block ends mid-record at `row + 1`.
pub fn read_physical_line<R: Read>(
    reader: &mut BufReader<R>,
    separator: &LineSeparator,
    record_length: usize,
    row: u64,
    buf: &mut Vec<u8>,
) -> Result<bool, FormatError> {
    buf.clear();
    match separator {
        LineSeparator::Lf => {
            if reader.read_until(b'\n', buf)? == 0 {
                return Ok(false);
            }
            if buf.last() == Some(&b'\n') {
                buf.pop();
            }
        }
        LineSeparator::CrLf => {
            if reader.read_until(b'\n', buf)? == 0 {
                return Ok(false);
            }
            if buf.ends_with(b"\r\n") {
                buf.truncate(buf.len() - 2);
            } else if buf.last() == Some(&b'\n') {
                buf.pop();
            }
        }
        LineSeparator::None => {
            buf.resize(record_length, 0);
            let mut total = 0;
            while total < record_length {
                let n = reader.read(&mut buf[total..])?;
                if n == 0 {
                    if total == 0 {
                        return Ok(false);
                    }
                    return Err(FormatError::InvalidRecord {
                        row: row + 1,
                        message: format!(
                            "incomplete record: expected {record_length} bytes, got {total}"
                        ),
                    });
                }
                total += n;
            }
        }
    }
    Ok(true)
}

/// `true` when a line carries no field content — empty, or only whitespace.
///
/// A blank line between records (common after file concatenation) is skipped
/// rather than parsed into a record of nulls or rejected for being too short.
pub fn is_blank_line(line: &[u8]) -> bool {
    line.iter().all(|b| b.is_ascii_whitespace())
}

/// Extract a field's trimmed, type-coerced [`Value`] from a record line.
///
/// # Errors
///
/// Returns [`FormatError::InvalidRecord`] at `row` when the field is present
/// but truncated mid-value, the slice is not valid UTF-8, or the trimmed text
/// fails to parse as the declared type.
pub fn extract_value(field: &ResolvedField, line: &[u8], row: u64) -> Result<Value, FormatError> {
    let raw = field_text(field, line, row)?;
    parse_field_value(field, &raw, row)
}

/// Borrow a field's raw (un-coerced) text from a record line, stripped of
/// padding per the field's justification and `trim` policy.
///
/// A field whose `start` is at or past the line end is **absent** — the line
/// stops before the field begins, the common case of a producer that omits
/// trailing padding on fields it has no value for — and yields empty text (a
/// later [`Value::Null`]). A field whose `start` is inside the line but whose
/// `end` overruns is **truncated** mid-value: rather than coerce a partial
/// slice (e.g. `20` from a truncated `200`) into a confidently-wrong value,
/// this is an error so the row can fail-fast or dead-letter.
///
/// # Errors
///
/// Returns [`FormatError::InvalidRecord`] at `row` when the field is present
/// but truncated, or the slice is not valid UTF-8.
pub fn field_text(field: &ResolvedField, line: &[u8], row: u64) -> Result<String, FormatError> {
    let end = field.end();
    if field.start >= line.len() {
        // The line ends before this field begins: an absent trailing field.
        return Ok(String::new());
    }
    let slice = line
        .get(field.start..end)
        .ok_or_else(|| FormatError::InvalidRecord {
            row,
            message: format!(
                "field '{}': value truncated — range {}..{end} overruns the {}-byte line",
                field.name,
                field.start,
                line.len()
            ),
        })?;
    let text = std::str::from_utf8(slice).map_err(|e| FormatError::InvalidRecord {
        row,
        message: format!("field '{}': invalid UTF-8: {e}", field.name),
    })?;
    Ok(strip_padding(text, field).to_string())
}

/// Strip a field's padding and surrounding whitespace per its justification.
///
/// A right-justified field strips leading pad runs; a left-justified (default)
/// field strips trailing pad runs. The full multi-character `pad` string is
/// stripped as a unit — a `pad: "0 "` strips `"0 "` runs, not just `'0'` —
/// before a final whitespace trim. With `trim: false` the raw slice is
/// returned verbatim.
pub fn strip_padding<'a>(raw: &'a str, field: &ResolvedField) -> &'a str {
    if !field.trim {
        return raw;
    }
    if field.pad.is_empty() {
        return raw.trim();
    }
    let stripped = match field.justify {
        Some(Justify::Right) => raw.trim_start_matches(field.pad.as_str()),
        Some(Justify::Left) | None => raw.trim_end_matches(field.pad.as_str()),
    };
    stripped.trim()
}

/// Parse a field's stripped text into its declared [`Value`] type.
///
/// Empty text (after padding strip) is [`Value::Null`]. A `String` or untyped
/// field carries the text verbatim. With `trim: false` the value is taken raw,
/// so an untyped field keeps its padding.
///
/// # Errors
///
/// Returns [`FormatError::InvalidRecord`] at `row` when the text fails to parse
/// as the declared type.
pub fn parse_field_value(field: &ResolvedField, raw: &str, row: u64) -> Result<Value, FormatError> {
    if raw.is_empty() {
        return Ok(Value::Null);
    }
    match &field.field_type {
        Some(FieldType::String) | None => Ok(Value::String(raw.into())),
        Some(ty) => coerce_scalar(ty, field.format.as_deref(), raw).map_err(|message| {
            FormatError::InvalidRecord {
                row,
                message: with_field(&field.name, &message),
            }
        }),
    }
}

/// Coerce trimmed text to a scalar [`Value`] of the given type.
///
/// Format-neutral: the caller owns the row/field framing. This is the single
/// canonical typed coercion shared by every fixed-width parse path. Boolean
/// matching is ASCII-case-insensitive (`TRUE`/`true`/`YES`/`Y`/`1` → true).
/// `String`/`Object`/`Array` are caller concerns and are not reachable here
/// (the field parse handles them).
///
/// # Errors
///
/// Returns the human-readable failure message (caller wraps it in a
/// format-specific error) when the text does not parse as `ty`.
pub fn coerce_scalar(ty: &FieldType, format: Option<&str>, raw: &str) -> Result<Value, String> {
    match ty {
        FieldType::Integer => raw
            .parse::<i64>()
            .map(Value::Integer)
            .map_err(|e| format!("cannot parse '{raw}' as integer: {e}")),
        FieldType::Float => raw
            .parse::<f64>()
            .map(Value::Float)
            .map_err(|e| format!("cannot parse '{raw}' as {}: {e}", ty_name(ty))),
        FieldType::Boolean => match raw.to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" | "y" => Ok(Value::Bool(true)),
            "false" | "0" | "no" | "n" => Ok(Value::Bool(false)),
            _ => Err(format!("cannot parse '{raw}' as boolean")),
        },
        FieldType::Date => {
            let fmt = format.unwrap_or("%Y-%m-%d");
            NaiveDate::parse_from_str(raw, fmt)
                .map(Value::Date)
                .map_err(|e| format!("cannot parse '{raw}' as date with format '{fmt}': {e}"))
        }
        FieldType::String | FieldType::Object | FieldType::Array => Ok(Value::String(raw.into())),
        FieldType::DateTime => Ok(Value::String(raw.into())),
    }
}

/// Lowercase name of a field type for diagnostics.
fn ty_name(ty: &FieldType) -> &'static str {
    match ty {
        FieldType::Integer => "integer",
        FieldType::Float => "float",
        FieldType::Boolean => "boolean",
        FieldType::Date => "date",
        FieldType::DateTime => "datetime",
        FieldType::String => "string",
        FieldType::Object => "object",
        FieldType::Array => "array",
    }
}

/// Prefix a coercion message with the offending field name.
fn with_field(name: &str, message: &str) -> String {
    format!("field '{name}': {message}")
}

/// Build an [`FormatError::InvalidRecord`] for a construction-time field defect
/// (row 0 — no record is being read yet).
fn invalid_field(name: &str, problem: &str) -> FormatError {
    FormatError::InvalidRecord {
        row: 0,
        message: format!("field '{name}': fixed-width field {problem}"),
    }
}

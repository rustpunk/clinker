//! Shared fixed-width field machinery.
//!
//! The single-record [`crate::fixed_width::reader::FixedWidthReader`] and the
//! multi-record [`crate::multi_record::MultiRecordReader`] both slice a line
//! into byte-positioned fields, strip padding by justification, and coerce the
//! trimmed text to a declared type. Hosting that logic here keeps the two
//! readers byte-for-byte identical — a declared `type: int` (or `float`, or
//! `bool`) parses the same way regardless of whether the file is single- or
//! multi-record — instead of two forks that drift apart.

use std::io::{BufRead, BufReader, Read};

use chrono::NaiveDate;

use clinker_record::Value;
use clinker_record::schema_def::{Justify, LineSeparator};
use cxl::typecheck::Type;

use crate::error::FormatError;
use crate::schema::Column;

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
    pub ty: Type,
    pub format: Option<String>,
    /// Decimal scale (fractional digits) a `decimal` column is rounded to at
    /// parse; `None` for non-decimal columns and unscaled decimals.
    pub scale: Option<u8>,
    pub justify: Option<Justify>,
    pub pad: String,
    pub trim: bool,
}

impl ResolvedField {
    /// Resolve a [`Column`] into a byte-positioned field.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::InvalidRecord`] when the field omits `start`,
    /// declares both `width` and `end`, declares neither, declares `end <
    /// start`, or resolves to a zero width.
    pub fn from_column(f: &Column) -> Result<Self, FormatError> {
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
            ty: f.ty.clone(),
            format: f.format.clone(),
            scale: f.scale,
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
    coerce_scalar(&field.ty, field.format.as_deref(), field.scale, raw).map_err(|message| {
        FormatError::InvalidRecord {
            row,
            message: with_field(&field.name, &message),
        }
    })
}

/// Coerce trimmed text to a scalar [`Value`] of the given [`Type`].
///
/// Format-neutral: the caller owns the row/field framing. This is the single,
/// complete coercion for every positional (fixed-width / multi-record) parse
/// path — the value emitted here is final, so no downstream re-coercion runs.
/// Boolean matching is ASCII-case-insensitive (`TRUE`/`true`/`YES`/`Y`/`1` →
/// true). `Date` / `DateTime` honor the column's `format:` strftime string;
/// with no explicit `format:`, `Date` uses the ISO `%Y-%m-%d` and `DateTime`
/// falls back to the shared default datetime chain
/// ([`coerce_to_datetime`](clinker_record::coercion::coerce_to_datetime) —
/// ISO-`T`, space-separated, and US variants), the same set the CSV/native
/// coercion path applies, so a positional `date_time` column parses the common
/// serializations rather than only strict ISO-`T`. `Numeric` parses as `int`
/// then falls back to `float`. `String` / `Any` / `Null` carry the text through
/// verbatim as a `Value::String`. `Map` / `Array` are rejected — a positional
/// field is a flat byte range and cannot yield a native composite; those types
/// belong to a JSON/XML source. `Nullable(T)` is peeled to `T`.
///
/// # Errors
///
/// Returns the human-readable failure message (caller wraps it in a
/// format-specific error) when the text does not parse as `ty`, or when `ty`
/// is a `Map` / `Array` a positional field cannot represent.
pub fn coerce_scalar(
    ty: &Type,
    format: Option<&str>,
    scale: Option<u8>,
    raw: &str,
) -> Result<Value, String> {
    match ty.unwrap_nullable() {
        Type::Int => raw
            .parse::<i64>()
            .map(Value::Integer)
            .map_err(|e| format!("cannot parse '{raw}' as int: {e}")),
        Type::Float => raw
            .parse::<f64>()
            .map(Value::Float)
            .map_err(|e| format!("cannot parse '{raw}' as float: {e}")),
        // Exact base-10 parse into `Value::Decimal`, rounded to the column
        // `scale` (banker's rounding). Never routed through a binary float.
        Type::Decimal => {
            clinker_record::coercion::coerce_to_decimal(&Value::String(raw.into()), scale)
                .map_err(|e| e.to_string())
        }
        Type::Numeric => raw
            .parse::<i64>()
            .map(Value::Integer)
            .or_else(|_| raw.parse::<f64>().map(Value::Float))
            .map_err(|e| format!("cannot parse '{raw}' as number: {e}")),
        Type::Bool => match raw.to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" | "y" => Ok(Value::Bool(true)),
            "false" | "0" | "no" | "n" => Ok(Value::Bool(false)),
            _ => Err(format!("cannot parse '{raw}' as bool")),
        },
        Type::Date => {
            let fmt = format.unwrap_or("%Y-%m-%d");
            NaiveDate::parse_from_str(raw, fmt)
                .map(Value::Date)
                .map_err(|e| format!("cannot parse '{raw}' as date with format '{fmt}': {e}"))
        }
        Type::DateTime => {
            // Honor an explicit `format:` (single format); otherwise fall back
            // to the shared default datetime chain — the same set the
            // CSV/native coercion path uses — so a positional `date_time`
            // column parses the common serializations (ISO-`T`, space-separated,
            // US) it did before this became a real reader-side parse.
            let chain: Vec<&str> = format.into_iter().collect();
            clinker_record::coercion::coerce_to_datetime(&Value::String(raw.into()), &chain)
                .map_err(|e| e.to_string())
        }
        Type::Map => Err(
            "a positional (fixed-width / multi-record) field cannot carry a `map` value; \
             declare this column on a native JSON/XML source"
                .to_string(),
        ),
        Type::Array => Err(
            "a positional (fixed-width / multi-record) field cannot carry an `array` value; \
             declare this column on a native JSON/XML source"
                .to_string(),
        ),
        _ => Ok(Value::String(raw.into())),
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    #[test]
    fn coerce_scalar_datetime_honors_format() {
        // The positional parse now produces a real `Value::DateTime`, honoring
        // the column's `format:` string — no punt-to-String / re-coerce.
        let v = coerce_scalar(
            &Type::DateTime,
            Some("%Y%m%d%H%M%S"),
            None,
            "20240115103000",
        )
        .unwrap();
        let expected = Value::DateTime(
            NaiveDate::from_ymd_opt(2024, 1, 15)
                .unwrap()
                .and_hms_opt(10, 30, 0)
                .unwrap(),
        );
        assert_eq!(v, expected);
    }

    #[test]
    fn coerce_scalar_datetime_default_chain_accepts_common_serializations() {
        // With no explicit `format:`, the default chain matches the CSV/native
        // path — ISO-`T`, space-separated, and US variants all parse — so a
        // positional `date_time` column keeps the parseable set it had when this
        // was a punt-to-String plus lenient re-coercion.
        let expected = Value::DateTime(
            NaiveDate::from_ymd_opt(2024, 1, 15)
                .unwrap()
                .and_hms_opt(10, 30, 0)
                .unwrap(),
        );
        for raw in [
            "2024-01-15T10:30:00",
            "2024-01-15 10:30:00",
            "01/15/2024 10:30:00",
        ] {
            assert_eq!(
                coerce_scalar(&Type::DateTime, None, None, raw).unwrap(),
                expected,
                "default datetime chain should accept {raw:?}"
            );
        }
    }

    #[test]
    fn coerce_scalar_datetime_bad_input_errors() {
        // A value matching neither an explicit format nor the default chain is a
        // loud parse error (positional formats are strict), not a silent String.
        assert!(coerce_scalar(&Type::DateTime, Some("%Y%m%d%H%M%S"), None, "not-a-time").is_err());
        assert!(coerce_scalar(&Type::DateTime, None, None, "definitely not a datetime").is_err());
    }

    #[test]
    fn coerce_scalar_numeric_int_then_float() {
        assert_eq!(
            coerce_scalar(&Type::Numeric, None, None, "42").unwrap(),
            Value::Integer(42)
        );
        assert_eq!(
            coerce_scalar(&Type::Numeric, None, None, "3.5").unwrap(),
            Value::Float(3.5)
        );
        assert!(coerce_scalar(&Type::Numeric, None, None, "abc").is_err());
    }

    #[test]
    fn coerce_scalar_map_and_array_are_rejected() {
        // A positional field cannot represent a native composite — declaring one
        // is an author error, not a silent String pass-through.
        assert!(coerce_scalar(&Type::Map, None, None, "{}").is_err());
        assert!(coerce_scalar(&Type::Array, None, None, "[]").is_err());
        // Nullable is peeled first, so the rejection still fires.
        assert!(coerce_scalar(&Type::nullable(Type::Array), None, None, "[]").is_err());
    }

    #[test]
    fn coerce_scalar_decimal_parses_exactly_and_rounds_to_scale() {
        // Exact base-10 parse (never via f64): "12.50" is exactly 12.50, and
        // Display preserves the column scale.
        let v = coerce_scalar(&Type::Decimal, None, Some(2), "12.50").unwrap();
        assert!(matches!(v, Value::Decimal(_)));
        assert_eq!(v.to_string(), "12.50");
        // Rounds to the column scale (banker's): "2.125" at scale 2 -> 2.12.
        let r = coerce_scalar(&Type::Decimal, None, Some(2), "2.125").unwrap();
        assert_eq!(r.to_string(), "2.12");
        // A non-numeric decimal field is a loud parse error.
        assert!(coerce_scalar(&Type::Decimal, None, Some(2), "abc").is_err());
    }

    #[test]
    fn coerce_scalar_string_any_pass_through() {
        assert_eq!(
            coerce_scalar(&Type::String, None, None, "hello").unwrap(),
            Value::String("hello".into())
        );
        assert_eq!(
            coerce_scalar(&Type::Any, None, None, "raw").unwrap(),
            Value::String("raw".into())
        );
    }
}

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

        let width = resolve_width(f, start)?;

        // `end()` is computed once per record on the hot read path, so the range
        // must be provably representable at construction to keep that a plain add
        // rather than a wrap. Mirrors the write path's identical guard.
        start
            .checked_add(width)
            .ok_or_else(|| invalid_field(&f.name, "'start' + width overflows"))?;

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

/// Resolve a column's declared `width` / `end` into a concrete byte width
/// from `start`, the one resolution shared by the read and write paths so a
/// schema means the same byte ranges in both directions.
///
/// # Errors
///
/// Returns [`FormatError::InvalidRecord`] when the column declares both
/// `width` and `end`, declares neither, declares `end < start`, or resolves
/// to a zero width.
pub fn resolve_width(f: &Column, start: usize) -> Result<usize, FormatError> {
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

    Ok(width)
}

/// Read one physical record line into `buf`, returning `false` at end of input.
///
/// `Lf` / `CrLf` read up to (and strip) the newline, but the buffered read is
/// bounded to the declared record width plus a line-terminator allowance: a
/// line-separated file with a missing terminator cannot buffer the rest of the
/// input into one record, preserving the engine's bounded-memory guarantee on
/// malformed input. A physical line wider than the declared width — trailing
/// filler beyond the last declared field, or a partial-schema read of a
/// fixed-LRECL extract — fills the cap before its terminator; the declared
/// fields are already captured, so the overrun is discarded up to the next
/// newline (in bounded memory) and the line resyncs to the following record.
/// `None` reads exactly `record_length` bytes (a delimiter-free fixed block). A
/// short final block under `None` is an incomplete-record error.
///
/// # Errors
///
/// Returns [`FormatError::Io`] on a read fault, or [`FormatError::InvalidRecord`]
/// at `row + 1` when a `None`-separated block ends mid-record.
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
            if !read_capped_line(reader, record_length, buf)? {
                return Ok(false);
            }
            if buf.last() == Some(&b'\n') {
                buf.pop();
            }
        }
        LineSeparator::CrLf => {
            if !read_capped_line(reader, record_length, buf)? {
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

/// Read up to one newline-terminated line into `buf`, bounding the buffered
/// portion to the declared record width so a line-separated file missing a
/// terminator cannot buffer the rest of the input into a single record. Returns
/// `Ok(false)` at clean EOF (nothing read); the trailing newline, if present, is
/// left on `buf` for the caller to strip per its separator.
///
/// The cap admits a full record, a CR/LF terminator, and one sentinel byte, so a
/// well-formed line — even one carrying a full-width record plus `\r\n` — always
/// fits within `buf`. A physical line wider than the declared width fills the cap
/// before its terminator: the declared fields have already been captured, so the
/// buffer is trimmed to the record width and the filler is discarded up to the
/// next newline via [`discard_to_newline`], keeping the working buffer bounded
/// regardless of physical line width and leaving the reader positioned at the
/// following record.
fn read_capped_line<R: Read>(
    reader: &mut BufReader<R>,
    record_length: usize,
    buf: &mut Vec<u8>,
) -> Result<bool, FormatError> {
    let cap = record_length + 3;
    // `Take` is created fresh per line and borrows the reader, so the limit
    // resets each call and the underlying buffer position carries over.
    let n = (&mut *reader).take(cap as u64).read_until(b'\n', buf)?;
    if n == 0 {
        return Ok(false);
    }
    if n == cap && buf.last() != Some(&b'\n') {
        // The physical line is wider than the declared record (trailing filler,
        // or a schema that maps only a prefix of a fixed-LRECL extract). Keep the
        // declared-width prefix and drop the overrun up to the next newline so
        // the following record starts clean.
        buf.truncate(record_length);
        discard_to_newline(reader)?;
    }
    Ok(true)
}

/// Consume and drop bytes up to and including the next newline (or clean EOF)
/// without buffering them into a record, so the reader resyncs to the next
/// physical line after an over-width record. Memory stays bounded by the
/// [`BufReader`]'s own buffer — the discarded bytes are never collected.
fn discard_to_newline<R: Read>(reader: &mut BufReader<R>) -> Result<(), FormatError> {
    loop {
        let chunk = reader.fill_buf()?;
        if chunk.is_empty() {
            return Ok(()); // EOF before any further newline.
        }
        let newline = chunk.iter().position(|&b| b == b'\n');
        let consumed = newline.map_or(chunk.len(), |i| i + 1);
        reader.consume(consumed);
        if newline.is_some() {
            return Ok(());
        }
    }
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
/// (row 0 — no record is being read or written yet).
pub(crate) fn invalid_field(name: &str, problem: &str) -> FormatError {
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

    fn read_line(sep: &LineSeparator, record_length: usize, row: u64, data: &[u8]) -> Vec<u8> {
        let mut reader = BufReader::new(data);
        let mut buf = Vec::new();
        assert!(read_physical_line(&mut reader, sep, record_length, row, &mut buf).unwrap());
        buf
    }

    #[test]
    fn lf_wellformed_lines_strip_newline() {
        let mut reader = BufReader::new(&b"Alice\nBobby\n"[..]);
        let mut buf = Vec::new();
        assert!(read_physical_line(&mut reader, &LineSeparator::Lf, 5, 0, &mut buf).unwrap());
        assert_eq!(buf, b"Alice");
        assert!(read_physical_line(&mut reader, &LineSeparator::Lf, 5, 1, &mut buf).unwrap());
        assert_eq!(buf, b"Bobby");
        // Clean EOF after the last terminated line.
        assert!(!read_physical_line(&mut reader, &LineSeparator::Lf, 5, 2, &mut buf).unwrap());
    }

    #[test]
    fn crlf_wellformed_lines_strip_crlf() {
        let mut reader = BufReader::new(&b"Alice\r\nBob\r\n"[..]);
        let mut buf = Vec::new();
        assert!(read_physical_line(&mut reader, &LineSeparator::CrLf, 5, 0, &mut buf).unwrap());
        assert_eq!(buf, b"Alice");
        assert!(read_physical_line(&mut reader, &LineSeparator::CrLf, 5, 1, &mut buf).unwrap());
        assert_eq!(buf, b"Bob");
    }

    #[test]
    fn lf_final_line_without_newline_within_cap_reads() {
        // A final line with no trailing terminator that fits within the declared
        // width still reads — this is the ordinary last-line-at-EOF case, not an
        // overrun.
        assert_eq!(
            read_line(&LineSeparator::Lf, 8, 0, b"ABCDEFGH"),
            b"ABCDEFGH"
        );
    }

    #[test]
    fn lf_terminated_line_at_cap_boundary_reads() {
        // The newline sitting on the final cap byte (content = record_length + 2)
        // is a terminated line, accepted; only a cap-filling run *without* a
        // terminator is an overrun. Record width 5 → cap 8 → 7 content + '\n'.
        assert_eq!(
            read_line(&LineSeparator::Lf, 5, 0, b"AAAAAAA\n"),
            b"AAAAAAA"
        );
    }

    #[test]
    fn lf_line_missing_newline_over_cap_reads_prefix_and_stays_bounded() {
        // A line-separated (LF) input whose line never terminates must not buffer
        // past the declared width: the declared-width prefix reads and the rest
        // is discarded to EOF, so the read buffer never exceeds the cap
        // regardless of input length.
        let record_length = 8;
        let data = vec![b'x'; 4096];
        let mut reader = BufReader::new(&data[..]);
        let mut buf = Vec::new();
        assert!(
            read_physical_line(&mut reader, &LineSeparator::Lf, record_length, 0, &mut buf)
                .unwrap()
        );
        assert_eq!(buf, vec![b'x'; record_length], "declared-width prefix kept");
        assert!(
            buf.len() <= record_length + 3,
            "read buffer bounded to the cap, got {}",
            buf.len()
        );
        // The whole (single, unterminated) physical line was consumed: EOF next.
        assert!(
            !read_physical_line(&mut reader, &LineSeparator::Lf, record_length, 1, &mut buf)
                .unwrap()
        );
    }

    #[test]
    fn lf_wide_terminated_line_reads_prefix_and_resyncs_to_next_record() {
        // The finding's shape: a physical line far wider than the declared width
        // (trailing filler the schema does not map) but properly terminated. The
        // declared-width prefix reads, the filler is discarded to the newline,
        // and the following record starts clean — a previously-valid file shape.
        let mut reader = BufReader::new(&b"AAAAAAAA\nBBB\n"[..]);
        let mut buf = Vec::new();
        // Record width 5 → cap 8; first line is 8 'A' + '\n' (filler past width).
        assert!(read_physical_line(&mut reader, &LineSeparator::Lf, 5, 0, &mut buf).unwrap());
        assert_eq!(buf, b"AAAAA", "declared-width prefix kept");
        assert!(read_physical_line(&mut reader, &LineSeparator::Lf, 5, 1, &mut buf).unwrap());
        assert_eq!(
            buf, b"BBB",
            "reader resynced to the next record after filler"
        );
        assert!(!read_physical_line(&mut reader, &LineSeparator::Lf, 5, 2, &mut buf).unwrap());
    }

    #[test]
    fn lf_line_one_byte_over_cap_reads_prefix_and_resyncs() {
        // The tightest boundary: 8 non-newline bytes then a newline at byte 9,
        // one past the 8-byte cap (record width 5). The prefix reads and the
        // reader resyncs to the following record rather than rejecting the line.
        let mut reader = BufReader::new(&b"AAAAAAAA\nrest\n"[..]);
        let mut buf = Vec::new();
        assert!(read_physical_line(&mut reader, &LineSeparator::Lf, 5, 0, &mut buf).unwrap());
        assert_eq!(buf, b"AAAAA");
        assert!(buf.len() <= 5 + 3);
        assert!(read_physical_line(&mut reader, &LineSeparator::Lf, 5, 1, &mut buf).unwrap());
        assert_eq!(buf, b"rest");
    }

    #[test]
    fn crlf_wide_line_reads_prefix_and_stays_bounded() {
        let record_length = 5;
        let data = vec![b'a'; 512];
        let mut reader = BufReader::new(&data[..]);
        let mut buf = Vec::new();
        assert!(
            read_physical_line(
                &mut reader,
                &LineSeparator::CrLf,
                record_length,
                3,
                &mut buf
            )
            .unwrap()
        );
        assert_eq!(buf, vec![b'a'; record_length], "declared-width prefix kept");
        assert!(buf.len() <= record_length + 3);
    }
}

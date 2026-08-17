//! Exact numeric evidence produced next to Clinker's canonical format parsers.
//!
//! Schema guessing consumes these observations instead of parsing numeric text
//! independently.  An observation deliberately keeps two facts separate:
//! the value the real parser produced, and the narrower exact vote that is safe
//! to turn into a concrete schema.  A finite `f64` parse can therefore be
//! recorded while underflow or discarded decimal significance still leaves
//! the guess unresolved.

use std::fmt;
use std::sync::Arc;

use clinker_record::Value;

/// Maximum combined bytes retained from one untrusted numeric lexeme.
pub const MAX_NUMERIC_LEXEME_EVIDENCE_BYTES: usize = 128;

/// The parser boundary that produced an observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumericBoundary {
    Json,
    Xml,
    Positional,
    SchemaCoerce,
}

/// Why a parser result cannot safely choose a concrete numeric schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumericIssue {
    InvalidNumeric,
    IntegerOverflow,
    NonFinite,
    UnsafeIntegerWidening,
    UnderflowToZero,
    PrecisionLoss,
    RepresentationChanged,
}

/// The exact result of the real parser path, before any guess policy is used.
#[derive(Debug, Clone, PartialEq)]
pub enum NumericParserOutcome {
    NoValue,
    Integer(i64),
    Float(f64),
    NonNumeric,
    Rejected(NumericIssue),
}

/// Exact acceptance of a lexeme by one candidate concrete numeric type.
#[derive(Debug, Clone, PartialEq)]
pub enum NumericAcceptance<T> {
    NoValue,
    Accepted(T),
    Rejected(NumericIssue),
}

/// The type vote a schema-guess aggregator may consume.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumericVote {
    NoValue,
    Int,
    Float,
    Unresolved(NumericIssue),
}

/// Bounded representative bytes from one original lexeme.
///
/// Short lexemes are retained in full. Long lexemes retain a UTF-8-safe head
/// and tail whose combined byte length never exceeds
/// [`MAX_NUMERIC_LEXEME_EVIDENCE_BYTES`]. The parser and exactness checks still
/// consume the borrowed complete input; only retained report evidence is
/// capped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NumericLexeme {
    head: Box<str>,
    tail: Box<str>,
    original_bytes: usize,
}

impl NumericLexeme {
    fn new(raw: &str) -> Self {
        if raw.len() <= MAX_NUMERIC_LEXEME_EVIDENCE_BYTES {
            return Self {
                head: raw.into(),
                tail: "".into(),
                original_bytes: raw.len(),
            };
        }

        let head_budget = MAX_NUMERIC_LEXEME_EVIDENCE_BYTES / 2;
        let tail_budget = MAX_NUMERIC_LEXEME_EVIDENCE_BYTES - head_budget;

        let mut head_end = head_budget;
        while !raw.is_char_boundary(head_end) {
            head_end -= 1;
        }

        let mut tail_start = raw.len() - tail_budget;
        while !raw.is_char_boundary(tail_start) {
            tail_start += 1;
        }

        Self {
            head: raw[..head_end].into(),
            tail: raw[tail_start..].into(),
            original_bytes: raw.len(),
        }
    }

    /// Complete lexeme when it fit inside the evidence cap.
    pub fn complete(&self) -> Option<&str> {
        self.tail.is_empty().then_some(self.head.as_ref())
    }

    pub fn head(&self) -> &str {
        &self.head
    }

    pub fn tail(&self) -> &str {
        &self.tail
    }

    pub fn original_bytes(&self) -> usize {
        self.original_bytes
    }

    pub fn is_truncated(&self) -> bool {
        !self.tail.is_empty()
    }
}

/// One parser-owned observation of a scalar lexeme.
#[derive(Debug, Clone, PartialEq)]
pub struct NumericObservation {
    boundary: NumericBoundary,
    lexeme: NumericLexeme,
    parser_outcome: NumericParserOutcome,
    int_acceptance: NumericAcceptance<i64>,
    float_acceptance: NumericAcceptance<f64>,
    vote: NumericVote,
}

impl NumericObservation {
    pub fn boundary(&self) -> NumericBoundary {
        self.boundary
    }

    pub fn lexeme(&self) -> &NumericLexeme {
        &self.lexeme
    }

    pub fn parser_outcome(&self) -> &NumericParserOutcome {
        &self.parser_outcome
    }

    pub fn int_acceptance(&self) -> &NumericAcceptance<i64> {
        &self.int_acceptance
    }

    /// Exact float compatibility is retained even for an integer parser
    /// outcome so a mixed int/float field can reject unsafe widening.
    pub fn float_acceptance(&self) -> &NumericAcceptance<f64> {
        &self.float_acceptance
    }

    pub fn vote(&self) -> NumericVote {
        self.vote
    }

    /// Convert the canonical parser outcome into the native record value.
    /// Rejected and non-numeric outcomes have no numeric value.
    pub fn parsed_value(&self) -> Option<Value> {
        match self.parser_outcome {
            NumericParserOutcome::NoValue => Some(Value::Null),
            NumericParserOutcome::Integer(value) => Some(Value::Integer(value)),
            NumericParserOutcome::Float(value) => Some(Value::Float(value)),
            NumericParserOutcome::NonNumeric | NumericParserOutcome::Rejected(_) => None,
        }
    }

    fn with_boundary(mut self, boundary: NumericBoundary) -> Self {
        self.boundary = boundary;
        self
    }
}

/// Authored field identity at the parser boundary that produced an
/// observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NumericObservationScope<'a> {
    field: &'a str,
    record: Option<&'a str>,
}

impl<'a> NumericObservationScope<'a> {
    pub fn field(self) -> &'a str {
        self.field
    }

    /// Record-type id for a multi-record field, or `None` for an ordinary
    /// source column.
    pub fn record(self) -> Option<&'a str> {
        self.record
    }
}

/// Cloneable callback used by streaming readers to publish one observation at
/// a time without retaining document-sized evidence.
type NumericObservationCallback =
    dyn for<'a> Fn(NumericObservationScope<'a>, NumericObservation) + Send + Sync;

#[derive(Clone)]
pub struct NumericObserver {
    callback: Arc<NumericObservationCallback>,
}

impl NumericObserver {
    pub fn new<F>(callback: F) -> Self
    where
        F: Fn(&str, NumericObservation) + Send + Sync + 'static,
    {
        Self {
            callback: Arc::new(move |scope, observation| callback(scope.field(), observation)),
        }
    }

    /// Build an observer that retains the record-type identity emitted by a
    /// multi-record parser boundary.
    pub fn new_scoped<F>(callback: F) -> Self
    where
        F: for<'a> Fn(NumericObservationScope<'a>, NumericObservation) + Send + Sync + 'static,
    {
        Self {
            callback: Arc::new(callback),
        }
    }

    pub fn observe(&self, field: &str, observation: NumericObservation) {
        (self.callback)(
            NumericObservationScope {
                field,
                record: None,
            },
            observation,
        );
    }

    pub fn observe_record_field(&self, record: &str, field: &str, observation: NumericObservation) {
        (self.callback)(
            NumericObservationScope {
                field,
                record: Some(record),
            },
            observation,
        );
    }
}

impl fmt::Debug for NumericObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("NumericObserver(..)")
    }
}

/// Observe a number already admitted by serde_json's arbitrary-precision
/// representation. `Number::to_string` supplies serde_json's parser-owned
/// representation (which may canonicalize spelling); no raw-value feature or
/// second JSON lexer is involved.
pub fn observe_json_number(number: &serde_json::Number) -> NumericObservation {
    let raw = number.to_string();
    let parser_outcome = if let Some(value) = number.as_i64() {
        NumericParserOutcome::Integer(value)
    } else if let Some(value) = number.as_f64() {
        NumericParserOutcome::Float(value)
    } else if is_integer_syntax(&raw) {
        NumericParserOutcome::Rejected(NumericIssue::IntegerOverflow)
    } else {
        NumericParserOutcome::Rejected(NumericIssue::NonFinite)
    };
    observation_from_outcome(NumericBoundary::Json, &raw, parser_outcome)
}

/// Observe the JSON scalar states that carry numeric-field evidence. Strings
/// continue to the schema-coercion boundary; objects, arrays, and booleans are
/// not interpreted as numeric by the format reader.
pub fn observe_json_value(value: &serde_json::Value) -> Option<NumericObservation> {
    match value {
        serde_json::Value::Null => Some(observation_from_outcome(
            NumericBoundary::Json,
            "null",
            NumericParserOutcome::NoValue,
        )),
        serde_json::Value::Number(number) => Some(observe_json_number(number)),
        _ => None,
    }
}

/// Observe XML's real scalar inference order: empty, signed `i64`, then `f64`
/// for a decimal/exponent spelling, then non-numeric. Exact acceptance rejects
/// a non-finite `f64` without changing the parser outcome.
pub fn observe_xml_scalar(raw: &str) -> NumericObservation {
    let parser_outcome = if raw.is_empty() {
        NumericParserOutcome::NoValue
    } else if let Ok(value) = raw.parse::<i64>() {
        NumericParserOutcome::Integer(value)
    } else if has_float_marker(raw) {
        match raw.parse::<f64>() {
            Ok(value) => NumericParserOutcome::Float(value),
            Err(_) => NumericParserOutcome::NonNumeric,
        }
    } else {
        NumericParserOutcome::NonNumeric
    };
    observation_from_outcome(NumericBoundary::Xml, raw, parser_outcome)
}

/// Observe the shared fixed-width/multi-record numeric parser: signed `i64`
/// first, then a finite `f64`.
pub fn observe_positional_numeric(raw: &str) -> NumericObservation {
    observe_positional_numeric_with_result(raw).0
}

/// Run the positional parser once and retain both its ordinary result and the
/// exact observation derived from that result.
pub(crate) fn observe_positional_numeric_with_result(
    raw: &str,
) -> (NumericObservation, Result<Value, String>) {
    let (parser_outcome, result) = if let Ok(value) = raw.parse::<i64>() {
        (
            NumericParserOutcome::Integer(value),
            Ok(Value::Integer(value)),
        )
    } else {
        match raw.parse::<f64>() {
            Ok(value) if value.is_finite() => {
                (NumericParserOutcome::Float(value), Ok(Value::Float(value)))
            }
            Ok(_) => (
                NumericParserOutcome::Rejected(NumericIssue::NonFinite),
                Err(format!(
                    "non-finite number '{raw}' is outside the declared type"
                )),
            ),
            Err(error) => (
                if is_integer_syntax(raw) {
                    NumericParserOutcome::Rejected(NumericIssue::IntegerOverflow)
                } else {
                    NumericParserOutcome::NonNumeric
                },
                Err(format!("cannot parse '{raw}' as float: {error}")),
            ),
        }
    };
    (
        observation_from_outcome(NumericBoundary::Positional, raw, parser_outcome),
        result,
    )
}

/// Observe the executor's string-to-schema numeric boundary.
///
/// Native values retain their already-established type. Text uses the same
/// int-then-finite-float path as runtime numeric coercion; composite, boolean,
/// and temporal values are not numeric evidence.
pub fn observe_schema_numeric(value: &Value) -> NumericObservation {
    match value {
        Value::Null => observation_from_outcome(
            NumericBoundary::SchemaCoerce,
            "",
            NumericParserOutcome::NoValue,
        ),
        Value::Integer(number) => observation_from_outcome(
            NumericBoundary::SchemaCoerce,
            &number.to_string(),
            NumericParserOutcome::Integer(*number),
        ),
        Value::Float(number) if number.is_finite() => observation_from_outcome(
            NumericBoundary::SchemaCoerce,
            &number.to_string(),
            NumericParserOutcome::Float(*number),
        ),
        Value::Float(number) => observation_from_outcome(
            NumericBoundary::SchemaCoerce,
            &number.to_string(),
            NumericParserOutcome::Rejected(NumericIssue::NonFinite),
        ),
        Value::String(raw) => {
            observe_positional_numeric(raw.as_str()).with_boundary(NumericBoundary::SchemaCoerce)
        }
        _ => observation_from_outcome(
            NumericBoundary::SchemaCoerce,
            value.type_name(),
            NumericParserOutcome::NonNumeric,
        ),
    }
}

fn observation_from_outcome(
    boundary: NumericBoundary,
    raw: &str,
    parser_outcome: NumericParserOutcome,
) -> NumericObservation {
    let int_acceptance = exact_int_acceptance(raw, &parser_outcome);
    let float_acceptance = exact_float_acceptance(raw, &parser_outcome, &int_acceptance);
    let vote = match (&parser_outcome, &int_acceptance, &float_acceptance) {
        (NumericParserOutcome::NoValue, _, _) => NumericVote::NoValue,
        (NumericParserOutcome::Integer(_), NumericAcceptance::Accepted(_), _) => NumericVote::Int,
        (NumericParserOutcome::Integer(_), NumericAcceptance::Rejected(issue), _) => {
            NumericVote::Unresolved(*issue)
        }
        (NumericParserOutcome::Float(_), _, NumericAcceptance::Accepted(_)) => NumericVote::Float,
        (NumericParserOutcome::Float(_), _, NumericAcceptance::Rejected(issue)) => {
            NumericVote::Unresolved(*issue)
        }
        (NumericParserOutcome::Rejected(issue), _, _)
        | (_, NumericAcceptance::Rejected(issue), NumericAcceptance::NoValue) => {
            NumericVote::Unresolved(*issue)
        }
        (NumericParserOutcome::NonNumeric, _, _) => {
            NumericVote::Unresolved(NumericIssue::InvalidNumeric)
        }
        _ => NumericVote::Unresolved(NumericIssue::InvalidNumeric),
    };

    NumericObservation {
        boundary,
        lexeme: NumericLexeme::new(raw),
        parser_outcome,
        int_acceptance,
        float_acceptance,
        vote,
    }
}

fn exact_int_acceptance(
    raw: &str,
    parser_outcome: &NumericParserOutcome,
) -> NumericAcceptance<i64> {
    match parser_outcome {
        NumericParserOutcome::NoValue => NumericAcceptance::NoValue,
        NumericParserOutcome::Integer(value) if raw == value.to_string() => {
            NumericAcceptance::Accepted(*value)
        }
        NumericParserOutcome::Integer(_) => {
            NumericAcceptance::Rejected(NumericIssue::RepresentationChanged)
        }
        _ if is_integer_syntax(raw) => NumericAcceptance::Rejected(NumericIssue::IntegerOverflow),
        _ => NumericAcceptance::Rejected(NumericIssue::InvalidNumeric),
    }
}

fn exact_float_acceptance(
    raw: &str,
    parser_outcome: &NumericParserOutcome,
    int_acceptance: &NumericAcceptance<i64>,
) -> NumericAcceptance<f64> {
    if matches!(parser_outcome, NumericParserOutcome::NoValue) {
        return NumericAcceptance::NoValue;
    }

    let parsed = match raw.parse::<f64>() {
        Ok(value) if value.is_finite() => value,
        Ok(_) => return NumericAcceptance::Rejected(NumericIssue::NonFinite),
        Err(_) if is_integer_syntax(raw) => {
            return NumericAcceptance::Rejected(NumericIssue::IntegerOverflow);
        }
        Err(_) => return NumericAcceptance::Rejected(NumericIssue::InvalidNumeric),
    };

    if parsed == 0.0 && contains_nonzero_digit(raw) {
        return NumericAcceptance::Rejected(NumericIssue::UnderflowToZero);
    }

    if is_integer_syntax(raw) {
        let NumericAcceptance::Accepted(integer) = int_acceptance else {
            return NumericAcceptance::Rejected(
                if matches!(parser_outcome, NumericParserOutcome::Float(_)) {
                    NumericIssue::UnsafeIntegerWidening
                } else {
                    match int_acceptance {
                        NumericAcceptance::Rejected(issue) => *issue,
                        _ => NumericIssue::UnsafeIntegerWidening,
                    }
                },
            );
        };
        if parsed as i128 != i128::from(*integer) {
            return NumericAcceptance::Rejected(NumericIssue::UnsafeIntegerWidening);
        }
        return NumericAcceptance::Accepted(parsed);
    }

    let Some(canonical) = serde_json::Number::from_f64(parsed) else {
        return NumericAcceptance::Rejected(NumericIssue::NonFinite);
    };
    if !same_decimal_value(raw, &canonical.to_string()) {
        return NumericAcceptance::Rejected(NumericIssue::PrecisionLoss);
    }

    NumericAcceptance::Accepted(parsed)
}

fn has_float_marker(raw: &str) -> bool {
    raw.bytes().any(|byte| matches!(byte, b'.' | b'e' | b'E'))
}

fn is_integer_syntax(raw: &str) -> bool {
    let digits = raw
        .strip_prefix('-')
        .or_else(|| raw.strip_prefix('+'))
        .unwrap_or(raw);
    !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
}

fn contains_nonzero_digit(raw: &str) -> bool {
    raw.bytes().any(|byte| matches!(byte, b'1'..=b'9'))
}

/// Compare two parser-accepted decimal spellings without accepting syntax.
/// This is a post-parse significance check only: the canonical parser has
/// already decided whether `raw` is numeric.
fn same_decimal_value(left: &str, right: &str) -> bool {
    let Some(left) = DecimalView::new(left) else {
        return false;
    };
    let Some(right) = DecimalView::new(right) else {
        return false;
    };
    if left.zero || right.zero {
        return left.zero && right.zero && left.negative == right.negative;
    }
    left.negative == right.negative
        && left.exponent == right.exponent
        && left.digits().eq(right.digits())
}

struct DecimalView<'a> {
    mantissa: &'a [u8],
    first: usize,
    last: usize,
    exponent: i32,
    negative: bool,
    zero: bool,
}

impl<'a> DecimalView<'a> {
    fn new(raw: &'a str) -> Option<Self> {
        let (negative, unsigned) = match raw.as_bytes().first() {
            Some(b'-') => (true, &raw[1..]),
            Some(b'+') => (false, &raw[1..]),
            _ => (false, raw),
        };
        let (mantissa, exponent_text) = match unsigned.find(['e', 'E']) {
            Some(index) => (&unsigned.as_bytes()[..index], Some(&unsigned[index + 1..])),
            None => (unsigned.as_bytes(), None),
        };

        let decimal = mantissa.iter().position(|byte| *byte == b'.');
        let fractional_digits = decimal
            .map(|index| mantissa.len().saturating_sub(index + 1))
            .unwrap_or(0);

        let first = mantissa.iter().position(|byte| matches!(byte, b'1'..=b'9'));
        let Some(first) = first else {
            return Some(Self {
                mantissa,
                first: 0,
                last: 0,
                exponent: 0,
                negative,
                zero: true,
            });
        };
        let explicit_exponent = match exponent_text {
            Some(text) => parse_exponent(text)?,
            None => 0,
        };
        let last = mantissa
            .iter()
            .rposition(|byte| matches!(byte, b'1'..=b'9'))?;
        let trailing_zeroes = mantissa[last + 1..]
            .iter()
            .filter(|byte| byte.is_ascii_digit())
            .count();
        let exponent = explicit_exponent
            .checked_sub(i32::try_from(fractional_digits).ok()?)?
            .checked_add(i32::try_from(trailing_zeroes).ok()?)?;

        Some(Self {
            mantissa,
            first,
            last,
            exponent,
            negative,
            zero: false,
        })
    }

    fn digits(&self) -> impl Iterator<Item = u8> + '_ {
        self.mantissa[self.first..=self.last]
            .iter()
            .copied()
            .filter(u8::is_ascii_digit)
    }
}

fn parse_exponent(raw: &str) -> Option<i32> {
    let (negative, digits) = match raw.as_bytes().first() {
        Some(b'-') => (true, &raw[1..]),
        Some(b'+') => (false, &raw[1..]),
        _ => (false, raw),
    };
    if digits.is_empty() {
        return None;
    }
    let mut value = 0i32;
    for digit in digits.bytes() {
        if !digit.is_ascii_digit() {
            return None;
        }
        value = value
            .checked_mul(10)?
            .checked_add(i32::from(digit - b'0'))?;
    }
    Some(if negative { -value } else { value })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_lexeme_retains_head_and_tail() {
        let raw = format!("{}e-{}", "1".repeat(200), "9".repeat(200));
        let lexeme = NumericLexeme::new(&raw);
        assert!(lexeme.is_truncated());
        assert_eq!(lexeme.original_bytes(), raw.len());
        assert!(lexeme.head().len() + lexeme.tail().len() <= MAX_NUMERIC_LEXEME_EVIDENCE_BYTES);
    }

    #[test]
    fn decimal_equivalence_handles_exponents_without_hiding_precision_loss() {
        assert!(same_decimal_value("1e3", "1000.0"));
        assert!(same_decimal_value("0.1", "0.1"));
        assert!(!same_decimal_value("0.10000000000000001", "0.1"));
    }
}

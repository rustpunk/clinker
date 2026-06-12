use std::path::Path;

use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike};
use clinker_record::Value;
use regex::Regex;

use super::context::{EvalContext, MAX_STRING_OUTPUT};
use super::error::EvalError;
use crate::lexer::Span;

/// Dispatch a method call on a receiver value.
/// Returns None if the method is not a known built-in (caller should error).
pub fn dispatch_method(
    receiver: &Value,
    method: &str,
    args: &[Value],
    regex: Option<&Regex>,
    span: Span,
    ctx: &EvalContext<'_>,
) -> Result<Option<Value>, EvalError> {
    // Null propagation: nullable receiver → Null for most methods
    if receiver.is_null() && !matches!(method, "is_null" | "type_of" | "is_empty" | "catch") {
        return Ok(Some(Value::Null));
    }

    match method {
        // ── String methods ──────────────────────────────────────
        "trim" => Ok(Some(string_op(receiver, span, |s| {
            Value::String(s.trim().into())
        }))),
        "trim_start" => Ok(Some(string_op(receiver, span, |s| {
            Value::String(s.trim_start().into())
        }))),
        "trim_end" => Ok(Some(string_op(receiver, span, |s| {
            Value::String(s.trim_end().into())
        }))),
        "upper" => Ok(Some(string_op(receiver, span, |s| {
            Value::String(s.to_uppercase().into())
        }))),
        "lower" => Ok(Some(string_op(receiver, span, |s| {
            Value::String(s.to_lowercase().into())
        }))),
        "length" => Ok(Some(match receiver {
            Value::String(s) => Value::Integer(s.chars().count() as i64),
            Value::Array(arr) => Value::Integer(arr.len() as i64),
            _ => Value::Null,
        })),
        "reverse" => Ok(Some(string_op(receiver, span, |s| {
            Value::String(s.chars().rev().collect::<String>().into())
        }))),
        "starts_with" => Ok(Some(string_op2(receiver, args, span, |s, arg| {
            Value::Bool(s.starts_with(arg))
        }))),
        "ends_with" => Ok(Some(string_op2(receiver, args, span, |s, arg| {
            Value::Bool(s.ends_with(arg))
        }))),
        "contains" => Ok(Some(string_op2(receiver, args, span, |s, arg| {
            Value::Bool(s.contains(arg))
        }))),
        "replace" => {
            if let (Value::String(s), Some(Value::String(find)), Some(Value::String(repl))) =
                (receiver, args.first(), args.get(1))
            {
                Ok(Some(Value::String(s.replace(&**find, repl).into())))
            } else {
                Ok(Some(Value::Null))
            }
        }
        "substring" => {
            if let Value::String(s) = receiver {
                let start = args.first().and_then(as_usize).unwrap_or(0);
                let len = args.get(1).and_then(as_usize);
                let chars: Vec<char> = s.chars().collect();
                let substr: String = if let Some(l) = len {
                    chars.iter().skip(start).take(l).collect()
                } else {
                    chars.iter().skip(start).collect()
                };
                Ok(Some(Value::String(substr.into())))
            } else {
                Ok(Some(Value::Null))
            }
        }
        "left" => {
            if let (Value::String(s), Some(n)) = (receiver, args.first().and_then(as_usize)) {
                Ok(Some(Value::String(
                    s.chars().take(n).collect::<String>().into(),
                )))
            } else {
                Ok(Some(Value::Null))
            }
        }
        "right" => {
            if let (Value::String(s), Some(n)) = (receiver, args.first().and_then(as_usize)) {
                let chars: Vec<char> = s.chars().collect();
                let start = chars.len().saturating_sub(n);
                Ok(Some(Value::String(
                    chars[start..].iter().collect::<String>().into(),
                )))
            } else {
                Ok(Some(Value::Null))
            }
        }
        "split" => {
            if let (Value::String(s), Some(Value::String(delim))) = (receiver, args.first()) {
                let parts: Vec<Value> =
                    s.split(&**delim).map(|p| Value::String(p.into())).collect();
                Ok(Some(Value::Array(parts)))
            } else {
                Ok(Some(Value::Null))
            }
        }
        "join" => {
            if let (Value::Array(arr), Some(Value::String(delim))) = (receiver, args.first()) {
                let joined: String = arr
                    .iter()
                    .map(|v| match v {
                        Value::String(s) => s.to_string(),
                        other => format!("{}", ValueDisplay(other)),
                    })
                    .collect::<Vec<_>>()
                    .join(delim);
                Ok(Some(Value::String(joined.into())))
            } else {
                Ok(Some(Value::Null))
            }
        }
        "concat" => {
            if let Value::String(s) = receiver {
                let mut result = s.to_string();
                for arg in args {
                    match arg {
                        Value::String(a) => result.push_str(a),
                        Value::Null => {} // Nulls become ""
                        other => result.push_str(&format!("{}", ValueDisplay(other))),
                    }
                }
                Ok(Some(Value::String(result.into())))
            } else {
                Ok(Some(Value::Null))
            }
        }
        "pad_left" => {
            if let (Value::String(s), Some(n)) = (receiver, args.first().and_then(as_usize)) {
                let ch = args
                    .get(1)
                    .and_then(|a| match a {
                        Value::String(c) => c.chars().next(),
                        _ => None,
                    })
                    .unwrap_or(' ');
                let current = s.chars().count();
                if n > current {
                    let pad_count = n - current;
                    let total = pad_count + s.len();
                    if total > MAX_STRING_OUTPUT {
                        return Err(EvalError::string_too_large(total, MAX_STRING_OUTPUT, span));
                    }
                    let padding: String = std::iter::repeat_n(ch, pad_count).collect();
                    Ok(Some(Value::String(format!("{}{}", padding, s).into())))
                } else {
                    Ok(Some(Value::String(s.clone())))
                }
            } else {
                Ok(Some(Value::Null))
            }
        }
        "pad_right" => {
            if let (Value::String(s), Some(n)) = (receiver, args.first().and_then(as_usize)) {
                let ch = args
                    .get(1)
                    .and_then(|a| match a {
                        Value::String(c) => c.chars().next(),
                        _ => None,
                    })
                    .unwrap_or(' ');
                let current = s.chars().count();
                if n > current {
                    let pad_count = n - current;
                    let total = pad_count + s.len();
                    if total > MAX_STRING_OUTPUT {
                        return Err(EvalError::string_too_large(total, MAX_STRING_OUTPUT, span));
                    }
                    let padding: String = std::iter::repeat_n(ch, pad_count).collect();
                    Ok(Some(Value::String(format!("{}{}", s, padding).into())))
                } else {
                    Ok(Some(Value::String(s.clone())))
                }
            } else {
                Ok(Some(Value::Null))
            }
        }
        "repeat" => {
            if let (Value::String(s), Some(n)) = (receiver, args.first().and_then(as_usize)) {
                let total = s.len().saturating_mul(n);
                if total > MAX_STRING_OUTPUT {
                    return Err(EvalError::string_too_large(total, MAX_STRING_OUTPUT, span));
                }
                Ok(Some(Value::String(s.repeat(n).into())))
            } else {
                Ok(Some(Value::Null))
            }
        }

        // ── Regex methods (use pre-compiled regex) ──────────────
        "matches" => {
            if let Value::String(s) = receiver {
                if let Some(re) = regex {
                    // Full match: ^...$
                    Ok(Some(Value::Bool(re.is_match(s))))
                } else {
                    Ok(Some(Value::Null))
                }
            } else {
                Ok(Some(Value::Null))
            }
        }
        "find" => {
            if let Value::String(s) = receiver {
                if let Some(re) = regex {
                    Ok(Some(Value::Bool(re.find(s).is_some())))
                } else {
                    Ok(Some(Value::Null))
                }
            } else {
                Ok(Some(Value::Null))
            }
        }
        "capture" => {
            if let Value::String(s) = receiver {
                if let Some(re) = regex {
                    let group = args.get(1).and_then(as_usize).unwrap_or(0);
                    if let Some(caps) = re.captures(s) {
                        if let Some(m) = caps.get(group) {
                            Ok(Some(Value::String(m.as_str().into())))
                        } else {
                            Ok(Some(Value::Null))
                        }
                    } else {
                        Ok(Some(Value::Null))
                    }
                } else {
                    Ok(Some(Value::Null))
                }
            } else {
                Ok(Some(Value::Null))
            }
        }

        // ── Path methods ────────────────────────────────────────
        "file_name" => Ok(Some(path_op(receiver, |p| {
            p.file_name().map(|n| n.to_string_lossy().into_owned())
        }))),
        "file_stem" => Ok(Some(path_op(receiver, |p| {
            p.file_stem().map(|n| n.to_string_lossy().into_owned())
        }))),
        "extension" => Ok(Some(path_op(receiver, |p| {
            p.extension().map(|n| n.to_string_lossy().into_owned())
        }))),
        "parent" => Ok(Some(path_op(receiver, |p| {
            p.parent().map(|n| n.to_string_lossy().into_owned())
        }))),
        "parent_name" => Ok(Some(path_op(receiver, |p| {
            p.parent()
                .and_then(|pp| pp.file_name())
                .map(|n| n.to_string_lossy().into_owned())
        }))),

        // ── Numeric methods ─────────────────────────────────────
        "abs" => Ok(Some(match receiver {
            Value::Integer(n) => Value::Integer(n.abs()),
            Value::Float(f) => Value::Float(f.abs()),
            _ => Value::Null,
        })),
        "ceil" => Ok(Some(match receiver {
            Value::Float(f) => Value::Integer(f.ceil() as i64),
            Value::Integer(n) => Value::Integer(*n),
            _ => Value::Null,
        })),
        "floor" => Ok(Some(match receiver {
            Value::Float(f) => Value::Integer(f.floor() as i64),
            Value::Integer(n) => Value::Integer(*n),
            _ => Value::Null,
        })),
        "round" => {
            let decimals = args.first().and_then(as_i64).unwrap_or(0);
            Ok(Some(match receiver {
                Value::Float(f) => {
                    let factor = 10f64.powi(decimals as i32);
                    Value::Float((f * factor).round() / factor)
                }
                Value::Integer(n) => Value::Integer(*n),
                _ => Value::Null,
            }))
        }
        "round_to" => {
            let decimals = args.first().and_then(as_i64).unwrap_or(0);
            Ok(Some(match receiver {
                Value::Float(f) => {
                    let factor = 10f64.powi(decimals as i32);
                    Value::Float((f * factor).round() / factor)
                }
                _ => Value::Null,
            }))
        }
        "clamp" => {
            if let (Some(min_v), Some(max_v)) = (args.first(), args.get(1)) {
                Ok(Some(clamp_value(receiver, min_v, max_v)))
            } else {
                Ok(Some(Value::Null))
            }
        }
        "min" => {
            if let Some(other) = args.first() {
                Ok(Some(
                    if compare_values(receiver, other) == Some(std::cmp::Ordering::Less) {
                        receiver.clone()
                    } else {
                        other.clone()
                    },
                ))
            } else {
                Ok(Some(Value::Null))
            }
        }
        "max" => {
            if let Some(other) = args.first() {
                Ok(Some(
                    if compare_values(receiver, other) == Some(std::cmp::Ordering::Greater) {
                        receiver.clone()
                    } else {
                        other.clone()
                    },
                ))
            } else {
                Ok(Some(Value::Null))
            }
        }

        // ── Date methods ────────────────────────────────────────
        "year" => Ok(Some(date_component(
            receiver,
            |d| d.year() as i64,
            |dt| dt.year() as i64,
        ))),
        "month" => Ok(Some(date_component(
            receiver,
            |d| d.month() as i64,
            |dt| dt.month() as i64,
        ))),
        "day" => Ok(Some(date_component(
            receiver,
            |d| d.day() as i64,
            |dt| dt.day() as i64,
        ))),
        "hour" => Ok(Some(match receiver {
            Value::DateTime(dt) => Value::Integer(dt.hour() as i64),
            _ => Value::Null,
        })),
        "minute" => Ok(Some(match receiver {
            Value::DateTime(dt) => Value::Integer(dt.minute() as i64),
            _ => Value::Null,
        })),
        "second" => Ok(Some(match receiver {
            Value::DateTime(dt) => Value::Integer(dt.second() as i64),
            _ => Value::Null,
        })),
        "add_days" => {
            let n = args.first().and_then(as_i64).unwrap_or(0);
            Ok(Some(match receiver {
                Value::Date(d) => d
                    .checked_add_signed(chrono::Duration::days(n))
                    .map(Value::Date)
                    .unwrap_or(Value::Null),
                Value::DateTime(dt) => dt
                    .checked_add_signed(chrono::Duration::days(n))
                    .map(Value::DateTime)
                    .unwrap_or(Value::Null),
                _ => Value::Null,
            }))
        }
        "add_months" => {
            let n = args.first().and_then(as_i64).unwrap_or(0) as i32;
            Ok(Some(match receiver {
                Value::Date(d) => add_months_date(*d, n)
                    .map(Value::Date)
                    .unwrap_or(Value::Null),
                _ => Value::Null,
            }))
        }
        "add_years" => {
            let n = args.first().and_then(as_i64).unwrap_or(0) as i32;
            Ok(Some(match receiver {
                Value::Date(d) => add_months_date(*d, n * 12)
                    .map(Value::Date)
                    .unwrap_or(Value::Null),
                _ => Value::Null,
            }))
        }
        "diff_days" => Ok(Some(match (receiver, args.first()) {
            (Value::Date(a), Some(Value::Date(b))) => Value::Integer((*a - *b).num_days()),
            _ => Value::Null,
        })),
        "diff_months" | "diff_years" => Ok(Some(Value::Null)), // Simplified for now
        "format_date" => {
            if let Some(Value::String(fmt)) = args.first() {
                Ok(Some(match receiver {
                    Value::Date(d) => Value::String(d.format(fmt).to_string().into()),
                    Value::DateTime(dt) => Value::String(dt.format(fmt).to_string().into()),
                    _ => Value::Null,
                }))
            } else {
                Ok(Some(Value::Null))
            }
        }

        // ── Conversion strict ───────────────────────────────────
        "to_int" => Ok(Some(match receiver {
            Value::Integer(n) => Value::Integer(*n),
            Value::Float(f) => Value::Integer(*f as i64),
            Value::String(s) => s
                .parse::<i64>()
                .map(Value::Integer)
                .map_err(|_| EvalError::conversion_failed(s.to_string(), "Int", span))?,
            Value::Bool(b) => Value::Integer(if *b { 1 } else { 0 }),
            _ => {
                return Err(EvalError::conversion_failed(
                    format!("{}", ValueDisplay(receiver)),
                    "Int",
                    span,
                ));
            }
        })),
        "to_float" => Ok(Some(match receiver {
            Value::Float(f) => Value::Float(*f),
            Value::Integer(n) => Value::Float(*n as f64),
            Value::String(s) => s
                .parse::<f64>()
                .map(Value::Float)
                .map_err(|_| EvalError::conversion_failed(s.to_string(), "Float", span))?,
            _ => {
                return Err(EvalError::conversion_failed(
                    format!("{}", ValueDisplay(receiver)),
                    "Float",
                    span,
                ));
            }
        })),
        "to_string" => Ok(Some(Value::String(
            format!("{}", ValueDisplay(receiver)).into(),
        ))),
        "to_bool" => Ok(Some(match receiver {
            Value::Bool(b) => Value::Bool(*b),
            Value::String(s) => match s.to_lowercase().as_str() {
                "true" | "1" | "yes" => Value::Bool(true),
                "false" | "0" | "no" => Value::Bool(false),
                _ => return Err(EvalError::conversion_failed(s.to_string(), "Bool", span)),
            },
            Value::Integer(n) => Value::Bool(*n != 0),
            _ => {
                return Err(EvalError::conversion_failed(
                    format!("{}", ValueDisplay(receiver)),
                    "Bool",
                    span,
                ));
            }
        })),
        "to_date" => {
            if let Value::String(s) = receiver {
                let fmt = args.first().and_then(|a| match a {
                    Value::String(f) => Some(&**f),
                    _ => None,
                });
                let parsed = if let Some(f) = fmt {
                    NaiveDate::parse_from_str(s, f).ok()
                } else {
                    s.parse::<NaiveDate>().ok()
                };
                match parsed {
                    Some(d) => Ok(Some(Value::Date(d))),
                    None => Err(EvalError::conversion_failed(s.to_string(), "Date", span)),
                }
            } else {
                Err(EvalError::conversion_failed(
                    format!("{}", ValueDisplay(receiver)),
                    "Date",
                    span,
                ))
            }
        }
        "to_datetime" => {
            if let Value::String(s) = receiver {
                let fmt = args.first().and_then(|a| match a {
                    Value::String(f) => Some(&**f),
                    _ => None,
                });
                let parsed = if let Some(f) = fmt {
                    NaiveDateTime::parse_from_str(s, f).ok()
                } else {
                    s.parse::<NaiveDateTime>().ok()
                };
                match parsed {
                    Some(dt) => Ok(Some(Value::DateTime(dt))),
                    None => Err(EvalError::conversion_failed(
                        s.to_string(),
                        "DateTime",
                        span,
                    )),
                }
            } else {
                Err(EvalError::conversion_failed(
                    format!("{}", ValueDisplay(receiver)),
                    "DateTime",
                    span,
                ))
            }
        }

        // ── Conversion lenient ──────────────────────────────────
        "try_int" => Ok(Some(match receiver {
            Value::Integer(n) => Value::Integer(*n),
            Value::Float(f) => Value::Integer(*f as i64),
            Value::String(s) => s.parse::<i64>().map(Value::Integer).unwrap_or(Value::Null),
            Value::Bool(b) => Value::Integer(if *b { 1 } else { 0 }),
            _ => Value::Null,
        })),
        "try_float" => Ok(Some(match receiver {
            Value::Float(f) => Value::Float(*f),
            Value::Integer(n) => Value::Float(*n as f64),
            Value::String(s) => s.parse::<f64>().map(Value::Float).unwrap_or(Value::Null),
            _ => Value::Null,
        })),
        "try_bool" => Ok(Some(match receiver {
            Value::Bool(b) => Value::Bool(*b),
            Value::String(s) => match s.to_lowercase().as_str() {
                "true" | "1" | "yes" => Value::Bool(true),
                "false" | "0" | "no" => Value::Bool(false),
                _ => Value::Null,
            },
            Value::Integer(n) => Value::Bool(*n != 0),
            _ => Value::Null,
        })),
        "try_date" => Ok(Some(match receiver {
            Value::String(s) => {
                let fmt = args.first().and_then(|a| match a {
                    Value::String(f) => Some(&**f),
                    _ => None,
                });
                if let Some(f) = fmt {
                    NaiveDate::parse_from_str(s, f)
                        .ok()
                        .map(Value::Date)
                        .unwrap_or(Value::Null)
                } else {
                    s.parse::<NaiveDate>()
                        .ok()
                        .map(Value::Date)
                        .unwrap_or(Value::Null)
                }
            }
            _ => Value::Null,
        })),
        "try_datetime" => Ok(Some(match receiver {
            Value::String(s) => {
                let fmt = args.first().and_then(|a| match a {
                    Value::String(f) => Some(&**f),
                    _ => None,
                });
                if let Some(f) = fmt {
                    NaiveDateTime::parse_from_str(s, f)
                        .ok()
                        .map(Value::DateTime)
                        .unwrap_or(Value::Null)
                } else {
                    s.parse::<NaiveDateTime>()
                        .ok()
                        .map(Value::DateTime)
                        .unwrap_or(Value::Null)
                }
            }
            _ => Value::Null,
        })),

        // ── Introspection ───────────────────────────────────────
        "type_of" => Ok(Some(Value::String(receiver.type_name().into()))),
        "is_null" => Ok(Some(Value::Bool(receiver.is_null()))),
        "is_empty" => Ok(Some(Value::Bool(match receiver {
            Value::String(s) => s.is_empty(),
            Value::Array(a) => a.is_empty(),
            Value::Null => true,
            _ => false,
        }))),
        "catch" => {
            // .catch(default) is equivalent to ?? but as a method
            // If receiver is Null, return the default
            if receiver.is_null() {
                Ok(Some(args.first().cloned().unwrap_or(Value::Null)))
            } else {
                Ok(Some(receiver.clone()))
            }
        }

        // ── Debug ───────────────────────────────────────────────
        "debug" => {
            // Level 3 passthrough: emit to tracing::trace! and return unchanged.
            // Zero overhead when trace level disabled.
            if let Some(Value::String(prefix)) = args.first() {
                tracing::trace!(
                    source_row = ctx.source_row,
                    source_file = %ctx.source_file,
                    "{}: {:?}", prefix, receiver
                );
            } else {
                tracing::trace!(
                    source_row = ctx.source_row,
                    source_file = %ctx.source_file,
                    "{:?}", receiver
                );
            }
            Ok(Some(receiver.clone()))
        }

        // ── Format ──────────────────────────────────────────────
        "format" => Ok(Some(Value::String(
            format!("{}", ValueDisplay(receiver)).into(),
        ))),

        // ── Array (non-closure) ─────────────────────────────────
        "remove" => Ok(Some(match (receiver, args.first()) {
            (Value::Array(arr), Some(Value::Integer(i))) => {
                if *i < 0 || *i as usize >= arr.len() {
                    // Out-of-bounds returns the original array
                    // unchanged — mirrors the policy in the array
                    // index access (return Null) when the caller
                    // can't usefully act on the miss.
                    Value::Array(arr.clone())
                } else {
                    let mut out = arr.clone();
                    out.remove(*i as usize);
                    Value::Array(out)
                }
            }
            _ => Value::Null,
        })),

        // ── Map ─────────────────────────────────────────────────
        "keys" => Ok(Some(match receiver {
            Value::Map(m) => {
                Value::Array(m.keys().map(|k| Value::String(k.as_ref().into())).collect())
            }
            _ => Value::Null,
        })),
        "values" => Ok(Some(match receiver {
            Value::Map(m) => Value::Array(m.values().cloned().collect()),
            _ => Value::Null,
        })),
        "merge" => Ok(Some(match (receiver, args.first()) {
            (Value::Map(a), Some(Value::Map(b))) => {
                let mut out = (**a).clone();
                for (k, v) in b.iter() {
                    out.insert(k.clone(), v.clone());
                }
                Value::Map(Box::new(out))
            }
            _ => Value::Null,
        })),
        "set" => Ok(Some(match (receiver, args.first(), args.get(1)) {
            (Value::Map(m), Some(Value::String(key)), Some(val)) => {
                // A bare key (no `.` / `[n]`) inserts at the top level. A
                // dotted/indexed path descends, auto-creating missing
                // intermediate Maps; see `map_set_path` for the conflict rules.
                map_set_path(m, key.as_str(), val)
            }
            _ => Value::Null,
        })),
        "remove_field" => Ok(Some(match (receiver, args.first()) {
            (Value::Map(m), Some(Value::String(key))) => {
                let mut out = (**m).clone();
                out.shift_remove(key.as_str());
                Value::Map(Box::new(out))
            }
            _ => Value::Null,
        })),
        "unset" => Ok(Some(match (receiver, args.first()) {
            (Value::Map(m), Some(Value::String(key))) => {
                // A bare key (no `.` / `[n]`) drops a top-level entry, mirroring
                // `remove_field`. A dotted/indexed path descends to the addressed
                // leaf and deletes it; see `map_unset_path` for the
                // shift-on-array-remove and missing-path-is-a-no-op rules.
                map_unset_path(m, key.as_str())
            }
            _ => Value::Null,
        })),

        _ => Ok(None), // Unknown method
    }
}

// ── `.set` path navigation ──────────────────────────────────
//
// `.set` accepts a dotted/indexed path as its key so a single call can write
// into a nested document: `m.set("address.city", "NYC")`, `m.set("items[0].sku",
// "x")`. The receiver is copy-on-write — every level on the write path is cloned,
// the upstream binding is never mutated.

/// One step along a `.set` path: a map key or an array position.
enum PathSeg<'a> {
    /// Field lookup into a `Value::Map` by string key.
    Field(&'a str),
    /// Positional access into a `Value::Array` by zero-based index.
    Index(usize),
}

/// Splits a `.set` key into navigation segments.
///
/// `.`-separated names become [`PathSeg::Field`]; a `[n]` suffix on a name or
/// on a prior index becomes [`PathSeg::Index`], so `a.b[0].c` parses as
/// `[Field("a"), Field("b"), Index(0), Field("c")]`. Returns `None` for a
/// malformed bracket group (empty, non-numeric, or unterminated) so the whole
/// `.set` resolves to `Null` rather than guessing intent. An empty field name
/// (a leading/trailing/doubled `.`) is a real key — a map can hold `""` — so it
/// is preserved rather than rejected.
fn parse_set_path(key: &str) -> Option<Vec<PathSeg<'_>>> {
    let bytes = key.as_bytes();
    let mut segs = Vec::new();
    let mut i = 0;
    // A path opens with a field name; thereafter each step is either `.field`
    // or `[index]`, both of which this loop consumes from the current cursor.
    let mut field_start = 0;
    let mut in_field = true;
    while i < bytes.len() {
        match bytes[i] {
            b'.' if in_field => {
                segs.push(PathSeg::Field(&key[field_start..i]));
                i += 1;
                field_start = i;
            }
            b'[' => {
                if in_field {
                    segs.push(PathSeg::Field(&key[field_start..i]));
                }
                let close = key[i + 1..].find(']')? + i + 1;
                let idx: usize = key[i + 1..close].parse().ok()?;
                segs.push(PathSeg::Index(idx));
                i = close + 1;
                // A `]` is followed by either `.field`, another `[index]`, or
                // end-of-path — never a bare field name glued onto the bracket.
                if i < bytes.len() {
                    match bytes[i] {
                        b'.' => {
                            i += 1;
                            field_start = i;
                            in_field = true;
                        }
                        b'[' => in_field = false,
                        _ => return None,
                    }
                } else {
                    in_field = false;
                }
            }
            _ => {
                in_field = true;
                i += 1;
            }
        }
    }
    if in_field {
        segs.push(PathSeg::Field(&key[field_start..]));
    }
    Some(segs)
}

/// Sets `val` at `key` within map `m`, returning a fresh `Value::Map`.
///
/// Missing intermediate map segments are auto-created as empty maps, so a path
/// can build structure that does not yet exist (jq `setpath` / Bloblang
/// convention). Returns `Value::Null` for the whole operation on a hard
/// conflict — an existing intermediate is present but the wrong kind for the
/// next segment (e.g. indexing a map, or fielding an array) — and on an array
/// index past the end (no silent auto-grow). A malformed path string is also
/// `Null`.
fn map_set_path(m: &indexmap::IndexMap<Box<str>, Value>, key: &str, val: &Value) -> Value {
    let Some(segs) = parse_set_path(key) else {
        return Value::Null;
    };
    let mut root = Value::Map(Box::new(m.clone()));
    if set_in(&mut root, &segs, val) {
        root
    } else {
        Value::Null
    }
}

/// Writes `val` at the path `segs` inside `target`, descending in place into
/// the caller's already-cloned copy. Returns `false` on any conflict so the
/// caller can discard the partial copy and yield `Null` for the whole `.set`.
fn set_in(target: &mut Value, segs: &[PathSeg<'_>], val: &Value) -> bool {
    let Some((head, rest)) = segs.split_first() else {
        *target = val.clone();
        return true;
    };
    match (head, target) {
        (PathSeg::Field(name), Value::Map(map)) => {
            if rest.is_empty() {
                map.insert(Box::from(*name), val.clone());
                true
            } else {
                // Auto-create a missing intermediate as an empty map so the
                // path can build structure; an existing wrong-kind child trips
                // the conflict check one level down.
                let child = map
                    .entry(Box::from(*name))
                    .or_insert_with(|| Value::Map(Box::new(indexmap::IndexMap::new())));
                set_in(child, rest, val)
            }
        }
        (PathSeg::Index(idx), Value::Array(items)) => match items.get_mut(*idx) {
            Some(child) => set_in(child, rest, val),
            None => false, // index past the end → Null, no auto-grow
        },
        // Field-into-array, index-into-map, or any scalar where a container is
        // required: a hard type conflict.
        _ => false,
    }
}

// ── `.unset` path deletion ──────────────────────────────────
//
// `.unset` is the deletion counterpart to `.set`: it reuses the very same path
// grammar (`parse_set_path`) to address one leaf and remove it. The contrasts
// with `.set` are deliberate:
//   * An array element is removed **and the tail shifts down** (jq `del`
//     semantics) — the array shrinks by one. This is distinct from
//     `set(path, null)`, which leaves a null hole.
//   * A path that does not resolve — missing intermediate, missing final key,
//     array index past the end, or a type conflict (a field segment into a
//     non-map, an index segment into a non-array) — is a **no-op returning the
//     receiver unchanged**, never `Null`. This mirrors `remove_field` returning
//     the receiver unchanged when the key is absent, and is the opposite of
//     `.set`, which yields `Null` on a conflicting path.
// Like every map method it is copy-on-write: the receiver is never mutated.

/// Deletes the leaf addressed by `key` within map `m`, returning a fresh
/// `Value::Map`.
///
/// A malformed path string, or any path that does not resolve to an existing
/// leaf, is a no-op: the receiver is returned unchanged (contrast `map_set_path`,
/// which yields `Null` on a conflicting path). Array elements are removed with a
/// down-shift so the array shrinks by one. The receiver is cloned before any
/// edit, so the caller's binding is untouched.
fn map_unset_path(m: &indexmap::IndexMap<Box<str>, Value>, key: &str) -> Value {
    let Some(segs) = parse_set_path(key) else {
        return Value::Map(Box::new(m.clone()));
    };
    let mut root = Value::Map(Box::new(m.clone()));
    // The cloned `root` is the unchanged receiver whether or not `unset_in`
    // deletes anything, so a no-op simply returns it as-is.
    unset_in(&mut root, &segs);
    root
}

/// Removes the leaf at path `segs` inside `target`, descending in place into the
/// caller's already-cloned copy. Returns `true` when a leaf was actually deleted
/// and `false` for any unresolved path (missing key, index past the end, or a
/// type conflict), leaving `target` untouched in the `false` case so the whole
/// `.unset` is a no-op.
fn unset_in(target: &mut Value, segs: &[PathSeg<'_>]) -> bool {
    let Some((head, rest)) = segs.split_first() else {
        // An empty path addresses no leaf to delete.
        return false;
    };
    match (head, target) {
        (PathSeg::Field(name), Value::Map(map)) => {
            if rest.is_empty() {
                // `shift_remove` deletes the entry and preserves the order of the
                // surviving keys; absence is a no-op.
                map.shift_remove(*name).is_some()
            } else {
                match map.get_mut(*name) {
                    Some(child) => unset_in(child, rest),
                    None => false, // missing intermediate → no-op
                }
            }
        }
        (PathSeg::Index(idx), Value::Array(items)) => {
            if rest.is_empty() {
                if *idx < items.len() {
                    // Remove-and-shift: the array shrinks by one (jq `del`),
                    // distinct from `set(path, null)` leaving a hole.
                    items.remove(*idx);
                    true
                } else {
                    false // index past the end → no-op
                }
            } else {
                match items.get_mut(*idx) {
                    Some(child) => unset_in(child, rest),
                    None => false, // index past the end → no-op
                }
            }
        }
        // Field-into-array, index-into-map, or a scalar where a container is
        // required: a type conflict, so a no-op.
        _ => false,
    }
}

// ── Helpers ──────────────────────────────────────────────────

fn string_op(receiver: &Value, _span: Span, f: impl FnOnce(&str) -> Value) -> Value {
    match receiver {
        Value::String(s) => f(s),
        _ => Value::Null,
    }
}

fn string_op2(
    receiver: &Value,
    args: &[Value],
    _span: Span,
    f: impl FnOnce(&str, &str) -> Value,
) -> Value {
    match (receiver, args.first()) {
        (Value::String(s), Some(Value::String(arg))) => f(s, arg),
        _ => Value::Null,
    }
}

fn path_op(receiver: &Value, f: impl FnOnce(&Path) -> Option<String>) -> Value {
    match receiver {
        Value::String(s) => {
            let path = Path::new(&**s);
            f(path)
                .map(|r| Value::String(r.into()))
                .unwrap_or(Value::Null)
        }
        _ => Value::Null,
    }
}

fn date_component(
    receiver: &Value,
    date_fn: impl FnOnce(&NaiveDate) -> i64,
    dt_fn: impl FnOnce(&NaiveDateTime) -> i64,
) -> Value {
    match receiver {
        Value::Date(d) => Value::Integer(date_fn(d)),
        Value::DateTime(dt) => Value::Integer(dt_fn(dt)),
        _ => Value::Null,
    }
}

fn add_months_date(d: NaiveDate, months: i32) -> Option<NaiveDate> {
    let total_months = d.year() * 12 + d.month() as i32 - 1 + months;
    let year = total_months.div_euclid(12);
    let month = (total_months.rem_euclid(12) + 1) as u32;
    let day = d.day().min(days_in_month(year, month));
    NaiveDate::from_ymd_opt(year, month, day)
}

fn days_in_month(year: i32, month: u32) -> u32 {
    NaiveDate::from_ymd_opt(year, month + 1, 1)
        .or_else(|| NaiveDate::from_ymd_opt(year + 1, 1, 1))
        .map(|d| d.pred_opt().unwrap().day())
        .unwrap_or(28)
}

fn as_i64(v: &Value) -> Option<i64> {
    match v {
        Value::Integer(n) => Some(*n),
        Value::Float(f) => Some(*f as i64),
        _ => None,
    }
}

fn as_usize(v: &Value) -> Option<usize> {
    as_i64(v).and_then(|n| if n >= 0 { Some(n as usize) } else { None })
}

fn compare_values(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Value::Integer(x), Value::Integer(y)) => Some(x.cmp(y)),
        (Value::Float(x), Value::Float(y)) => x.partial_cmp(y),
        (Value::Integer(x), Value::Float(y)) => (*x as f64).partial_cmp(y),
        (Value::Float(x), Value::Integer(y)) => x.partial_cmp(&(*y as f64)),
        _ => None,
    }
}

fn clamp_value(val: &Value, min: &Value, max: &Value) -> Value {
    if compare_values(val, min) == Some(std::cmp::Ordering::Less) {
        min.clone()
    } else if compare_values(val, max) == Some(std::cmp::Ordering::Greater) {
        max.clone()
    } else {
        val.clone()
    }
}

/// Display helper for Value — produces the string representation.
pub struct ValueDisplay<'a>(pub &'a Value);

impl std::fmt::Display for ValueDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Value::Null => write!(f, ""),
            Value::Bool(b) => write!(f, "{}", b),
            Value::Integer(n) => write!(f, "{}", n),
            Value::Float(v) => write!(f, "{}", v),
            Value::String(s) => write!(f, "{}", s),
            Value::Date(d) => write!(f, "{}", d),
            Value::DateTime(dt) => write!(f, "{}", dt),
            Value::Array(arr) => {
                write!(f, "[")?;
                for (i, v) in arr.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", ValueDisplay(v))?;
                }
                write!(f, "]")
            }
            Value::Map(m) => {
                write!(f, "{{")?;
                for (i, (k, v)) in m.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {}", k, ValueDisplay(v))?;
                }
                write!(f, "}}")
            }
        }
    }
}

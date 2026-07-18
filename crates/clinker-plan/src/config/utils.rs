//! Shared config value types and serde helpers (durations, byte sizes, timestamps).

use serde::de::{self};
use serde::{Deserialize, Deserializer, Serialize};

/// Parse a duration string with an `ms` / `s` / `m` / `h` / `d` unit
/// suffix into a [`std::time::Duration`].
///
/// `ms` is tested before the single-char `s` suffix so `"500ms"`
/// doesn't read as 500 seconds with a stray `"m"`. Shared by
/// [`WatermarkConfig::idle_timeout`], [`WatermarkConfig::delay`], and
/// duration fields on `TimeWindowSpec` so YAML duration parsing is a
/// single uniform contract across the schema.
pub(crate) fn parse_duration_with_suffix(s: &str) -> Option<std::time::Duration> {
    let s = s.trim();
    if let Some(rest) = s.strip_suffix("ms") {
        let n: u64 = rest.trim().parse().ok()?;
        return Some(std::time::Duration::from_millis(n));
    }
    let (num_str, unit) = s.split_at(s.len().checked_sub(1)?);
    let n: u64 = num_str.trim().parse().ok()?;
    let secs = match unit {
        "s" => n,
        "m" => n.checked_mul(60)?,
        "h" => n.checked_mul(60 * 60)?,
        "d" => n.checked_mul(60 * 60 * 24)?,
        _ => return None,
    };
    Some(std::time::Duration::from_secs(secs))
}

pub(crate) fn deserialize_optional_duration<'de, D: Deserializer<'de>>(
    d: D,
) -> Result<Option<std::time::Duration>, D::Error> {
    let raw: Option<String> = Option::deserialize(d)?;
    raw.map(|s| {
        parse_duration_with_suffix(&s).ok_or_else(|| {
            de::Error::custom(format!(
                "expected duration like \"500ms\"/\"30s\"/\"5m\"/\"2h\"/\"3d\"; got {s:?}"
            ))
        })
    })
    .transpose()
}

pub(crate) fn serialize_optional_duration<S: serde::Serializer>(
    v: &Option<std::time::Duration>,
    s: S,
) -> Result<S::Ok, S::Error> {
    match v {
        Some(d) => {
            // Round-trip as milliseconds — the most precise unit the
            // parser accepts.
            s.serialize_str(&format!("{}ms", d.as_millis()))
        }
        None => s.serialize_none(),
    }
}

/// Time bound for `modified_after` / `modified_before`. Resolves to a
/// concrete `SystemTime` at parse time. Two YAML forms:
///
/// - **Relative duration**: `"5m"`, `"2h"`, `"3d"`, `"30s"` — interpreted
///   relative to the time of deserialization (effectively "now").
/// - **Absolute timestamp**: any RFC3339 string (`"2026-05-01T00:00:00Z"`).
#[derive(Debug, Clone, Copy)]
pub struct TimeBound(pub std::time::SystemTime);

impl TimeBound {
    /// Parse `"<n><unit>"` with unit ∈ {s, m, h, d}. Returns `None` if the
    /// string is not in this form.
    fn parse_duration(s: &str) -> Option<std::time::Duration> {
        let (num_str, unit) = s.split_at(s.len().checked_sub(1)?);
        let n: u64 = num_str.parse().ok()?;
        let secs = match unit {
            "s" => n,
            "m" => n.checked_mul(60)?,
            "h" => n.checked_mul(60 * 60)?,
            "d" => n.checked_mul(60 * 60 * 24)?,
            _ => return None,
        };
        Some(std::time::Duration::from_secs(secs))
    }
}

impl Serialize for TimeBound {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        // Round-trip as RFC3339 — the relative form is parse-time only.
        let dt: chrono::DateTime<chrono::Utc> = self.0.into();
        s.serialize_str(&dt.to_rfc3339())
    }
}

impl<'de> Deserialize<'de> for TimeBound {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(d)?;
        if let Some(dur) = Self::parse_duration(&raw) {
            // Relative-to-now: clip rather than panic if the duration
            // somehow underflows (it won't for any reasonable value).
            let when = std::time::SystemTime::now()
                .checked_sub(dur)
                .unwrap_or(std::time::UNIX_EPOCH);
            return Ok(TimeBound(when));
        }
        let dt = chrono::DateTime::parse_from_rfc3339(&raw).map_err(|e| {
            de::Error::custom(format!(
                "expected duration like \"5m\"/\"2h\"/\"3d\" or RFC3339 timestamp; \
                 got {raw:?}: {e}"
            ))
        })?;
        Ok(TimeBound(dt.with_timezone(&chrono::Utc).into()))
    }
}

/// Memory budget used when `memory.limit` is omitted, empty, or
/// unparseable. The arbitrator ceiling, the planner's grace-hash
/// heuristic, and the `--explain` renderers all fall back to this single
/// value so every consumer agrees on the default budget.
pub const DEFAULT_MEMORY_LIMIT_BYTES: u64 = 512 * 1024 * 1024;

/// Soft-limit fraction of the hard `memory.limit` at which the arbitrator
/// begins proactive spill and pauses a back-pressureable producer (the
/// pause watermark). Plan-time validation and the exec-side arbitrator
/// builder both read this single value so the soft threshold has one
/// source of truth instead of a literal duplicated across crates.
pub const DEFAULT_SPILL_THRESHOLD: f64 = 0.80;

/// Fraction of the hard `memory.limit` at which a producer paused under
/// memory pressure resumes — the low watermark of the pause/resume
/// hysteresis band. Sits strictly below [`DEFAULT_SPILL_THRESHOLD`] so the
/// dead zone between the two damps thrash (a single admit/discharge cycle
/// cannot cross both thresholds). Applied when `memory.resume_threshold`
/// is omitted.
pub const DEFAULT_RESUME_THRESHOLD: f64 = 0.70;

/// Split a trimmed `memory.limit` string into its numeric text and the
/// binary multiplier its suffix selects.
///
/// A trailing `G`/`g`, `M`/`m`, or `K`/`k` scales by 1024^3 / 1024^2 /
/// 1024; anything else is a bare byte count (multiplier 1). The numeric
/// text is returned unparsed so callers apply their own lenient or strict
/// handling of a non-numeric part — both share this one grammar, so a
/// value can never scale differently depending on which parser saw it.
fn split_memory_limit_suffix(trimmed: &str) -> (&str, u64) {
    if let Some(num) = trimmed
        .strip_suffix('G')
        .or_else(|| trimmed.strip_suffix('g'))
    {
        (num, 1024 * 1024 * 1024)
    } else if let Some(num) = trimmed
        .strip_suffix('M')
        .or_else(|| trimmed.strip_suffix('m'))
    {
        (num, 1024 * 1024)
    } else if let Some(num) = trimmed
        .strip_suffix('K')
        .or_else(|| trimmed.strip_suffix('k'))
    {
        (num, 1024)
    } else {
        (trimmed, 1)
    }
}

/// Parse the `memory.limit` knob into a byte count, leniently.
///
/// Binary units follow [`split_memory_limit_suffix`]. `None`, an empty
/// value, or any unparseable input falls back to
/// [`DEFAULT_MEMORY_LIMIT_BYTES`] (512 MB) — the contract the YAML config
/// and executor paths depend on. A value whose numeric part parses but
/// whose binary-suffix scaling exceeds `u64::MAX` is rejected with
/// [`ConfigError::Validation`] rather than silently wrapping.
///
/// Input boundaries that must reject a typo rather than quietly shrink the
/// budget use [`parse_memory_limit_bytes_strict`] instead.
///
/// [`ConfigError::Validation`]: crate::config::ConfigError::Validation
pub fn parse_memory_limit_bytes(s: Option<&str>) -> Result<u64, crate::config::ConfigError> {
    let Some(raw) = s else {
        return Ok(DEFAULT_MEMORY_LIMIT_BYTES);
    };
    let trimmed = raw.trim();
    let (num_part, multiplier) = split_memory_limit_suffix(trimmed);
    // An unparseable numeric part keeps the lenient fallback contract: the
    // caller gets the default budget, matching the historical handling of
    // `None`/empty/garbage input. Only a value that parses cleanly yet
    // overflows the byte range when scaled by its binary suffix is an error.
    let Ok(value) = num_part.parse::<u64>() else {
        return Ok(DEFAULT_MEMORY_LIMIT_BYTES);
    };
    value.checked_mul(multiplier).ok_or_else(|| {
        crate::config::ConfigError::Validation(format!(
            "memory.limit {trimmed:?} overflows the maximum addressable byte count (u64::MAX)"
        ))
    })
}

/// Parse a `memory.limit` value strictly, for input boundaries where a
/// malformed value must fail loudly instead of silently falling back to the
/// default budget — a fallback that would quietly shrink a larger configured
/// limit and hide the operator's typo.
///
/// Shares the exact suffix grammar of the lenient [`parse_memory_limit_bytes`]
/// via [`split_memory_limit_suffix`], so any value this accepts scales
/// identically on the lenient path the executor re-reads. Returns the byte
/// budget, or a human-readable reason the value is not a valid limit. The
/// reason is a bare fragment with no leading subject, so each caller prefixes
/// the surface it owns — a CLI flag, a config key — and echoes the raw value
/// once.
pub fn parse_memory_limit_bytes_strict(raw: &str) -> Result<u64, String> {
    let (num_part, multiplier) = split_memory_limit_suffix(raw.trim());
    let value = num_part.parse::<u64>().map_err(|_| {
        "expected a non-negative byte count, optionally suffixed with a binary \
         K, M, or G unit (for example \"512M\" or \"4G\")"
            .to_string()
    })?;
    value.checked_mul(multiplier).ok_or_else(|| {
        "the value scaled by its binary suffix overflows the maximum addressable \
         byte count (u64::MAX)"
            .to_string()
    })
}

/// Byte size for `min_size` / `max_size`. Decimal units (1KB = 1000 bytes,
/// 1MB = 1_000_000) — matches the convention used by `du`, `df`, AWS CLI,
/// and most file-tooling. Plain `"1024"` (no unit) is interpreted as bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteSize(pub u64);

impl ByteSize {
    fn parse(s: &str) -> Option<u64> {
        let s = s.trim();
        let (num_part, mult) = if let Some(rest) = s.strip_suffix("GB") {
            (rest, 1_000_000_000)
        } else if let Some(rest) = s.strip_suffix("MB") {
            (rest, 1_000_000)
        } else if let Some(rest) = s.strip_suffix("KB") {
            (rest, 1_000)
        } else if let Some(rest) = s.strip_suffix('B') {
            (rest, 1)
        } else {
            (s, 1)
        };
        let n: u64 = num_part.trim().parse().ok()?;
        n.checked_mul(mult)
    }
}

impl Serialize for ByteSize {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&format!("{}B", self.0))
    }
}

impl<'de> Deserialize<'de> for ByteSize {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        // Accept either an integer (bytes) or a string with a unit suffix.
        struct V;
        impl<'de> de::Visitor<'de> for V {
            type Value = ByteSize;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a byte size as integer or string like \"1KB\"/\"100MB\"")
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<ByteSize, E> {
                Ok(ByteSize(v))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<ByteSize, E> {
                u64::try_from(v)
                    .map(ByteSize)
                    .map_err(|_| de::Error::custom("byte size cannot be negative"))
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<ByteSize, E> {
                ByteSize::parse(v).map(ByteSize).ok_or_else(|| {
                    de::Error::custom(format!(
                        "expected byte size like \"100\"/\"1KB\"/\"5MB\"/\"2GB\"; got {v:?}"
                    ))
                })
            }
        }
        d.deserialize_any(V)
    }
}

pub(crate) fn is_zero_u8(n: &u8) -> bool {
    *n == 0
}

pub(crate) fn is_false_bool(b: &bool) -> bool {
    !*b
}

pub(crate) fn default_true() -> bool {
    true
}

pub(crate) fn default_include_unmapped() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_MEMORY_LIMIT_BYTES, parse_memory_limit_bytes, parse_memory_limit_bytes_strict,
    };
    use crate::config::ConfigError;

    #[test]
    fn memory_limit_parses_unit_suffixes() {
        // `.ok()` collapses the success arm to a comparable `Option<u64>`;
        // `ConfigError` is not `PartialEq`, so the Result cannot be compared
        // directly.
        assert_eq!(
            parse_memory_limit_bytes(Some("512M")).ok(),
            Some(536_870_912)
        );
        assert_eq!(
            parse_memory_limit_bytes(Some("512m")).ok(),
            Some(536_870_912)
        );
        assert_eq!(
            parse_memory_limit_bytes(Some("2G")).ok(),
            Some(2_147_483_648)
        );
        assert_eq!(
            parse_memory_limit_bytes(Some("2g")).ok(),
            Some(2_147_483_648)
        );
        assert_eq!(parse_memory_limit_bytes(Some("512K")).ok(), Some(524_288));
        assert_eq!(parse_memory_limit_bytes(Some("1024")).ok(), Some(1024));
        assert_eq!(
            parse_memory_limit_bytes(None).ok(),
            Some(DEFAULT_MEMORY_LIMIT_BYTES)
        );
    }

    #[test]
    fn memory_limit_unparseable_falls_back_to_default() {
        // The lenient contract: a value that does not parse (no recognized
        // suffix, non-numeric body) yields the default budget, not an error.
        assert_eq!(
            parse_memory_limit_bytes(Some("garbage")).ok(),
            Some(DEFAULT_MEMORY_LIMIT_BYTES)
        );
        assert_eq!(
            parse_memory_limit_bytes(Some("")).ok(),
            Some(DEFAULT_MEMORY_LIMIT_BYTES)
        );
        // Numeric body that itself exceeds `u64` (parse failure, never
        // reaching the suffix multiply) also falls back rather than erroring.
        assert_eq!(
            parse_memory_limit_bytes(Some("99999999999999999999G")).ok(),
            Some(DEFAULT_MEMORY_LIMIT_BYTES)
        );
    }

    #[test]
    fn memory_limit_max_safe_value_does_not_overflow() {
        // `floor(u64::MAX / 1024^3) = 2^34 - 1` is the largest `G`-suffixed
        // value whose scaling still fits in `u64`.
        let max_safe_g = (1u64 << 34) - 1; // 17_179_869_183
        assert_eq!(
            parse_memory_limit_bytes(Some(&format!("{max_safe_g}G"))).ok(),
            Some(max_safe_g * 1024 * 1024 * 1024),
        );
        // A bare byte count at the ceiling has multiplier 1 — also safe.
        assert_eq!(
            parse_memory_limit_bytes(Some(&u64::MAX.to_string())).ok(),
            Some(u64::MAX),
        );
    }

    #[test]
    fn memory_limit_overflow_is_a_typed_error() {
        // `2^34 * 1024^3 = 2^64`, one past `u64::MAX` — the numeric part
        // parses, but the suffix multiply overflows and must be rejected.
        let overflow_g = 1u64 << 34; // 17_179_869_184
        match parse_memory_limit_bytes(Some(&format!("{overflow_g}G"))) {
            Err(ConfigError::Validation(msg)) => {
                assert!(
                    msg.contains("memory.limit") && msg.contains("overflow"),
                    "diagnostic must name the setting and the overflow; got: {msg}"
                );
            }
            other => panic!("expected ConfigError::Validation for overflow, got: {other:?}"),
        }
    }

    #[test]
    fn memory_limit_strict_rejects_malformed_value() {
        // Strict parsing is for input boundaries: unlike the lenient path, a
        // value with no recognized numeric body is an error rather than a
        // fallback. The decimal `4GB` (the binary `4G` was meant) is the
        // canonical operator typo the boundary must catch.
        for bad in ["4GB", "notanumber", "", "12x", "M"] {
            assert!(
                parse_memory_limit_bytes_strict(bad).is_err(),
                "strict parse must reject {bad:?}"
            );
        }
    }

    #[test]
    fn memory_limit_strict_agrees_with_lenient_scaling() {
        // A value both parsers accept must scale to the identical byte count,
        // so a strict-validated flag string re-read on the lenient executor
        // path yields exactly the same budget.
        for good in ["512M", "2G", "512K", "1024", "  256m  "] {
            assert_eq!(
                parse_memory_limit_bytes_strict(good).ok(),
                parse_memory_limit_bytes(Some(good)).ok(),
                "strict and lenient must agree for {good:?}"
            );
        }
    }

    #[test]
    fn memory_limit_strict_rejects_overflow() {
        // `2^34 G = 2^64` bytes: the numeric part parses but the suffix
        // multiply overflows — strict surfaces a reason rather than falling
        // back to the default budget.
        let overflow_g = 1u64 << 34;
        match parse_memory_limit_bytes_strict(&format!("{overflow_g}G")) {
            Err(reason) => assert!(
                reason.contains("overflow"),
                "reason must describe the overflow; got: {reason}"
            ),
            Ok(v) => panic!("expected overflow error, got {v}"),
        }
    }
}

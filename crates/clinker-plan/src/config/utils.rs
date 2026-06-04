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

/// Parse the `memory.limit` knob into a byte count.
///
/// Binary units: a trailing `G`/`g`, `M`/`m`, or `K`/`k` multiplies by
/// 1024^3 / 1024^2 / 1024; a bare integer is bytes. `None`, an empty
/// value, or any unparseable input falls back to the 512 MB default that
/// the arbitrator and the `--explain` renderers share.
pub fn parse_memory_limit_bytes(s: Option<&str>) -> u64 {
    s.and_then(|s| {
        let s = s.trim();
        if let Some(num) = s.strip_suffix('G').or_else(|| s.strip_suffix('g')) {
            num.parse::<u64>().ok().map(|n| n * 1024 * 1024 * 1024)
        } else if let Some(num) = s.strip_suffix('M').or_else(|| s.strip_suffix('m')) {
            num.parse::<u64>().ok().map(|n| n * 1024 * 1024)
        } else if let Some(num) = s.strip_suffix('K').or_else(|| s.strip_suffix('k')) {
            num.parse::<u64>().ok().map(|n| n * 1024)
        } else {
            s.parse::<u64>().ok()
        }
    })
    .unwrap_or(512 * 1024 * 1024) // 512MB default
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
    use super::parse_memory_limit_bytes;

    #[test]
    fn memory_limit_parses_unit_suffixes() {
        assert_eq!(parse_memory_limit_bytes(Some("512M")), 536_870_912);
        assert_eq!(parse_memory_limit_bytes(Some("512m")), 536_870_912);
        assert_eq!(parse_memory_limit_bytes(Some("2G")), 2_147_483_648);
        assert_eq!(parse_memory_limit_bytes(Some("2g")), 2_147_483_648);
        assert_eq!(parse_memory_limit_bytes(Some("512K")), 524_288);
        assert_eq!(parse_memory_limit_bytes(Some("1024")), 1024);
        assert_eq!(parse_memory_limit_bytes(None), 512 * 1024 * 1024);
        assert_eq!(parse_memory_limit_bytes(Some("garbage")), 512 * 1024 * 1024);
    }
}

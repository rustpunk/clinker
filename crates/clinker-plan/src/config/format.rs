//! Input/output format selectors.

use super::*;
use clinker_format::charset::Charset;
use serde::{Deserialize, Serialize};

fn resolve_encoding(name: Option<&str>) -> Result<Charset, String> {
    let Some(name) = name else {
        return Ok(Charset::Utf8);
    };
    Charset::from_name(name)
        .map_err(|error| format!("{error}; use `encoding: utf-8` or `encoding: iso-8859-1`"))
}

pub(super) fn deserialize_encoding<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<String>, D::Error> {
    let name = Option::<String>::deserialize(deserializer)?;
    resolve_encoding(name.as_deref()).map_err(serde::de::Error::custom)?;
    Ok(name)
}

pub(super) fn serialize_encoding<S: serde::Serializer>(
    name: &Option<String>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let charset = resolve_encoding(name.as_deref()).map_err(serde::ser::Error::custom)?;
    Some(match charset {
        Charset::Utf8 => "utf-8",
        Charset::Latin1 => "iso-8859-1",
    })
    .serialize(serializer)
}

fn serialize_resolved_options<T: Serialize + Default, S: serde::Serializer>(
    options: &Option<T>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match options {
        Some(options) => Some(options).serialize(serializer),
        None => Some(T::default()).serialize(serializer),
    }
}

/// Adjacently tagged format enum for inputs.
/// `type` selects the format, `options` provides format-specific settings.
/// `options` is optional — `type: csv` with no `options:` key gives `Csv(None)`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "options", rename_all = "snake_case")]
pub enum InputFormat {
    Csv(#[serde(serialize_with = "serialize_resolved_options")] Option<CsvInputOptions>),
    Json(Option<JsonInputOptions>),
    Xml(Option<XmlInputOptions>),
    FixedWidth(Option<FixedWidthInputOptions>),
    Edifact(Option<EdifactInputOptions>),
    X12(#[serde(serialize_with = "serialize_resolved_options")] Option<X12InputOptions>),
    Hl7(Option<Hl7InputOptions>),
    Swift(Option<SwiftInputOptions>),
}

impl InputFormat {
    /// Resolve external byte encoding without allocating on success. `None`
    /// means the format owns its in-band repertoire; callers must use its codec.
    /// Invalid programmatic options fail just like authored configuration.
    pub fn resolved_charset(&self) -> Result<Option<Charset>, String> {
        match self {
            Self::Csv(options) => {
                resolve_encoding(options.as_ref().and_then(|o| o.encoding.as_deref())).map(Some)
            }
            Self::X12(options) => {
                resolve_encoding(options.as_ref().and_then(|o| o.encoding.as_deref())).map(Some)
            }
            Self::Json(_) | Self::Xml(_) | Self::FixedWidth(_) | Self::Swift(_) => {
                Ok(Some(Charset::Utf8))
            }
            Self::Edifact(_) | Self::Hl7(_) => Ok(None),
        }
    }
    /// Short lowercase format name for display.
    pub fn format_name(&self) -> &'static str {
        match self {
            InputFormat::Csv(_) => "csv",
            InputFormat::Json(_) => "json",
            InputFormat::Xml(_) => "xml",
            InputFormat::FixedWidth(_) => "fixed_width",
            InputFormat::Edifact(_) => "edifact",
            InputFormat::X12(_) => "x12",
            InputFormat::Hl7(_) => "hl7",
            InputFormat::Swift(_) => "swift",
        }
    }
}

impl OutputFormat {
    /// Resolve the closed output repertoire without allocating on success.
    /// `None` delegates to the format's in-band policy, never to locale guessing.
    pub fn resolved_charset(&self) -> Result<Option<Charset>, String> {
        match self {
            Self::Csv(options) => {
                resolve_encoding(options.as_ref().and_then(|o| o.encoding.as_deref())).map(Some)
            }
            Self::X12(options) => {
                resolve_encoding(options.as_ref().and_then(|o| o.encoding.as_deref())).map(Some)
            }
            Self::Json(_) | Self::Xml(_) | Self::FixedWidth(_) | Self::Swift(_) => {
                Ok(Some(Charset::Utf8))
            }
            Self::Edifact(_) | Self::Hl7(_) => Ok(None),
        }
    }
    /// Short lowercase format name for display.
    pub fn format_name(&self) -> &'static str {
        match self {
            OutputFormat::Csv(_) => "csv",
            OutputFormat::Json(_) => "json",
            OutputFormat::Xml(_) => "xml",
            OutputFormat::FixedWidth(_) => "fixed_width",
            OutputFormat::Edifact(_) => "edifact",
            OutputFormat::X12(_) => "x12",
            OutputFormat::Hl7(_) => "hl7",
            OutputFormat::Swift(_) => "swift",
        }
    }

    /// Whether this output frames a SINGLE top-level document envelope for the
    /// entire record stream it receives — EDIFACT (`UNB..UNZ`), X12
    /// (`ISA..IEA`), HL7 v2 (`FHS..FTS`), SWIFT MT (`{1:}..{5:}`). Feeding such
    /// a format a body that carries more than one document collapses every
    /// input document's records into one envelope, silently merging distinct
    /// messages/interchanges (the E355 plan-time gate and the runtime guard in
    /// `clinker-exec` both key off this). The generic formats (CSV / JSON /
    /// XML / fixed-width) emit a valid document sequence and return `false`.
    ///
    /// Single source of truth for the single-document format set: the
    /// `clinker-plan` cardinality gate and the `clinker-exec` runtime guard
    /// both consult this so the two cannot diverge.
    pub fn is_single_document(&self) -> bool {
        matches!(
            self,
            OutputFormat::Edifact(_)
                | OutputFormat::X12(_)
                | OutputFormat::Hl7(_)
                | OutputFormat::Swift(_)
        )
    }
}

/// Adjacently tagged format enum for outputs.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "options", rename_all = "snake_case")]
pub enum OutputFormat {
    Csv(#[serde(serialize_with = "serialize_resolved_options")] Option<CsvOutputOptions>),
    Json(Option<JsonOutputOptions>),
    Xml(Option<XmlOutputOptions>),
    FixedWidth(Option<FixedWidthOutputOptions>),
    Edifact(Option<EdifactOutputOptions>),
    X12(#[serde(serialize_with = "serialize_resolved_options")] Option<X12OutputOptions>),
    Hl7(Option<Hl7OutputOptions>),
    Swift(Option<SwiftOutputOptions>),
}

/// Merge ordering discipline across declared inputs.
///
/// `Concat` is the deterministic default: each predecessor's records
/// drain in declaration order, in their per-source FIFO order. Output
/// is reproducible run-to-run.
///
/// `Interleave` reads concurrently from every upstream channel,
/// emitting records as they arrive. Per-source FIFO is preserved, but
/// cross-source order follows wall-clock arrival and is therefore
/// non-deterministic unless `interleave_seed` is set on
/// [`MergeBody`](crate::config::pipeline_node::MergeBody).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MergeMode {
    /// Drain each predecessor sequentially in declaration order.
    #[default]
    Concat,
    /// Drain predecessors concurrently; first-ready-wins.
    Interleave,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_document_predicate_matches_the_structured_formats() {
        for f in [
            OutputFormat::Edifact(None),
            OutputFormat::X12(None),
            OutputFormat::Hl7(None),
            OutputFormat::Swift(None),
        ] {
            assert!(
                f.is_single_document(),
                "{} frames one envelope and is single-document",
                f.format_name()
            );
        }
        for f in [
            OutputFormat::Csv(None),
            OutputFormat::Json(None),
            OutputFormat::Xml(None),
            OutputFormat::FixedWidth(None),
        ] {
            assert!(
                !f.is_single_document(),
                "{} emits a document sequence, not a single envelope",
                f.format_name()
            );
        }
    }
}

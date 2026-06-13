//! Maps `clinker_plan::config::InputFormat` to `clinker_bench_support::cache::DataFormat`.

use clinker_bench_support::cache::DataFormat;
use clinker_plan::config::{InputFormat, JsonFormat};

/// Error when an `InputFormat` variant has no `DataFormat` equivalent.
#[derive(Debug)]
pub enum FormatMappingError {
    UnsupportedJsonObject,
    UnsupportedEdifact,
    UnsupportedX12,
    UnsupportedHl7,
    UnsupportedSwift,
}

impl std::fmt::Display for FormatMappingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedJsonObject => {
                f.write_str("JSON object format has no benchmark DataFormat equivalent")
            }
            Self::UnsupportedEdifact => {
                f.write_str("EDIFACT format has no benchmark DataFormat equivalent")
            }
            Self::UnsupportedX12 => {
                f.write_str("X12 format has no benchmark DataFormat equivalent")
            }
            Self::UnsupportedHl7 => {
                f.write_str("HL7 format has no benchmark DataFormat equivalent")
            }
            Self::UnsupportedSwift => {
                f.write_str("SWIFT format has no benchmark DataFormat equivalent")
            }
        }
    }
}

impl std::error::Error for FormatMappingError {}

/// Convert an `InputFormat` to the corresponding `DataFormat` for data generation.
///
/// JSON defaults to NDJSON when no explicit format is specified (standard ETL streaming default).
pub fn input_format_to_data_format(input: &InputFormat) -> Result<DataFormat, FormatMappingError> {
    match input {
        InputFormat::Csv(_) => Ok(DataFormat::Csv),
        InputFormat::Xml(_) => Ok(DataFormat::Xml),
        InputFormat::FixedWidth(_) => Ok(DataFormat::FixedWidth),
        InputFormat::Edifact(_) => Err(FormatMappingError::UnsupportedEdifact),
        InputFormat::X12(_) => Err(FormatMappingError::UnsupportedX12),
        InputFormat::Hl7(_) => Err(FormatMappingError::UnsupportedHl7),
        InputFormat::Swift(_) => Err(FormatMappingError::UnsupportedSwift),
        InputFormat::Json(opts) => {
            let json_fmt = opts.as_ref().and_then(|o| o.format.as_ref());
            match json_fmt {
                Some(JsonFormat::Ndjson) | None => Ok(DataFormat::JsonNdjson),
                Some(JsonFormat::Array) => Ok(DataFormat::JsonArray),
                Some(JsonFormat::Object) => Err(FormatMappingError::UnsupportedJsonObject),
            }
        }
    }
}

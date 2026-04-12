//! Maps `clinker_core::config::InputFormat` to `clinker_bench_support::cache::DataFormat`.

use clinker_bench_support::cache::DataFormat;
use clinker_core::config::{InputFormat, JsonFormat};

/// Error when an `InputFormat` variant has no `DataFormat` equivalent.
#[derive(Debug)]
pub enum FormatMappingError {
    UnsupportedJsonObject,
}

impl std::fmt::Display for FormatMappingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedJsonObject => {
                f.write_str("JSON object format has no benchmark DataFormat equivalent")
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

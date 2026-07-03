//! Format-styling vocabulary shared by the fixed-width readers/writers.
//!
//! The typed source-schema vocabulary (columns, record types, discriminators)
//! lives above this crate in `clinker_format::schema`, where it can carry a
//! `cxl::typecheck::Type`. This module keeps only the parse-time formatting
//! enums that have no type dependency, so the foundation crate stays below
//! `cxl` in the layering.

use serde::{Deserialize, Serialize};

/// Field justification for fixed-width formatting.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Justify {
    Left,
    Right,
}

/// Line separator mode for fixed-width I/O.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LineSeparator {
    Lf,
    CrLf,
    None,
}

/// Truncation policy for fixed-width writer.
/// Default resolved by field type: numeric -> Error, string -> Warn.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TruncationPolicy {
    Error,
    Warn,
    Silent,
}

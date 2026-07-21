//! Plan-time validation of the multi-value surface.
//!
//! Two gates, both fired from `compile_with_diagnostics` so a failure is a
//! spanned `clinker explain` diagnostic rather than a mid-run abort:
//!
//! - **E358** — a malformed declaration on a source: two `split_to_rows`
//!   entries naming the same or nested field groups, a duplicate
//!   `split_values` field, or a `split_values` field the schema does not
//!   declare `multiple: true`.
//! - **E359** — a `multiple: true` column reaching an output whose format has
//!   no multi-value encoding, which would degrade or reject the value at the
//!   sink instead of at compile.

use clinker_format::{Column, SourceSchema, under_field_path};

use crate::config::format::OutputFormat;
use crate::config::source::SourceConfig;

/// Whether an output format can encode a `multiple: true` field.
///
/// The single per-format capability table behind `E359`. Each format's answer
/// sits on its own arm, so a writer gaining multi-value support flips exactly
/// one line here and nothing else in the gate changes.
///
/// - `json` — a JSON array is the format's own native shape.
/// - `xml` — repeated child elements: tracked at
///   https://github.com/rustpunk/clinker/issues/916.
/// - `csv` — delimited-in-cell: tracked at
///   https://github.com/rustpunk/clinker/issues/917.
/// - `fixed_width` — repeating field groups: tracked at
///   https://github.com/rustpunk/clinker/issues/918.
/// - `edifact` / `x12` / `hl7` / `swift` — these writers emit a fixed
///   positional segment grammar with no repetition slot to fill.
pub fn output_encodes_multi_value(format: &OutputFormat) -> bool {
    match format {
        OutputFormat::Json(_) => true,
        OutputFormat::Xml(_) => false,
        OutputFormat::Csv(_) => false,
        OutputFormat::FixedWidth(_) => false,
        OutputFormat::Edifact(_)
        | OutputFormat::X12(_)
        | OutputFormat::Hl7(_)
        | OutputFormat::Swift(_) => false,
    }
}

/// One malformed-declaration finding: the message body, and the help text that
/// says how to resolve it. The caller wraps these in a spanned diagnostic.
pub struct DeclarationFault {
    pub message: String,
    pub help: String,
}

/// Validate one source's `split_to_rows` / `split_values` declarations against
/// its resolved schema (E358).
///
/// Declared fan-out fields must name **disjoint** element groups: occurrence
/// tracking assigns each extracted field to at most one declared group, so a
/// duplicated field would fan the same group out twice and a nested one would
/// leave the inner group's membership ambiguous. This ran at XML reader
/// construction as an untyped, unspanned string error and never ran at all for
/// JSON; both formats are checked here instead, before any input is opened.
pub fn validate_source_declarations(
    source: &SourceConfig,
    schema: &SourceSchema,
) -> Vec<DeclarationFault> {
    let mut faults = Vec::new();
    let fan_out = source.split_to_rows.as_deref().unwrap_or(&[]);
    for (i, a) in fan_out.iter().enumerate() {
        for b in &fan_out[i + 1..] {
            if a.field == b.field {
                faults.push(DeclarationFault {
                    message: format!(
                        "source '{}': `split_to_rows` declares the field '{}' more than once",
                        source.name, a.field
                    ),
                    help: "declare each field once; a single entry already fans every \
                           occurrence of that element out to its own record"
                        .to_string(),
                });
            } else if under_field_path(&a.field, &b.field) || under_field_path(&b.field, &a.field) {
                faults.push(DeclarationFault {
                    message: format!(
                        "source '{}': `split_to_rows` fields '{}' and '{}' nest — declared \
                         fan-out fields must name disjoint (non-nested) element groups",
                        source.name, a.field, b.field
                    ),
                    help: "fan out on the outer field alone, or on the inner one alone; a \
                           nested pair leaves each occurrence's membership ambiguous"
                        .to_string(),
                });
            }
        }
    }

    let in_cell = source.split_values.as_deref().unwrap_or(&[]);
    for (i, a) in in_cell.iter().enumerate() {
        if in_cell[i + 1..].iter().any(|b| b.field == a.field) {
            faults.push(DeclarationFault {
                message: format!(
                    "source '{}': `split_values` declares the field '{}' more than once",
                    source.name, a.field
                ),
                help: "declare each field once, with the delimiter its text actually uses"
                    .to_string(),
            });
        }
    }

    // A `split_values` entry produces several values, which only a
    // `multiple: true` column can hold. Without the declaration the column
    // would be typed as a single value while carrying an array.
    if let Some(columns) = schema.as_columns() {
        for entry in in_cell {
            if !declares_multiple(columns, &entry.field) {
                faults.push(DeclarationFault {
                    message: format!(
                        "source '{}': `split_values` names the field '{}', which the schema \
                         does not declare `multiple: true` — splitting it would produce \
                         several values for a single-valued column",
                        source.name, entry.field
                    ),
                    help: format!(
                        "add `multiple: true` to the '{}' schema column, or drop the \
                         `split_values` entry",
                        entry.field
                    ),
                });
            }
        }
    }
    faults
}

/// Whether the schema declares a column (by exposed or physical name)
/// `multiple: true`. Both names are accepted because a `split_values` entry
/// names a field of the source document, which an aliased column reads through
/// its physical name while the author may think of it by either.
fn declares_multiple(columns: &[Column], field: &str) -> bool {
    columns
        .iter()
        .any(|c| c.is_multiple() && (c.name == field || c.physical_name() == field))
}

/// The `multiple: true` columns a source declares, by exposed name.
pub fn multi_value_columns(schema: &SourceSchema) -> Vec<String> {
    schema
        .as_columns()
        .unwrap_or(&[])
        .iter()
        .filter(|c| c.is_multiple())
        .map(|c| c.name.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_json_encodes_multi_value_today() {
        assert!(output_encodes_multi_value(&OutputFormat::Json(None)));
        for format in [
            OutputFormat::Csv(None),
            OutputFormat::Xml(None),
            OutputFormat::FixedWidth(None),
            OutputFormat::Edifact(None),
            OutputFormat::X12(None),
            OutputFormat::Hl7(None),
            OutputFormat::Swift(None),
        ] {
            assert!(
                !output_encodes_multi_value(&format),
                "{} has no multi-value encoding yet",
                format.format_name()
            );
        }
    }
}

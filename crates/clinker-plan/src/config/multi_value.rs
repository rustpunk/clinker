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

use std::collections::{BTreeSet, HashMap};

use clinker_format::{Column, SourceSchema, under_field_path};

use crate::config::format::OutputFormat;
use crate::config::pipeline_node::PipelineNode;
use crate::config::source::SourceConfig;
use crate::yaml::Spanned;

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

    // A `position_column` receives each occurrence's index, so two entries
    // writing the same column would leave one index silently overwriting the
    // other — and which one survives is not something the author chose.
    for (i, a) in fan_out.iter().enumerate() {
        let Some(column) = a.position_column.as_deref() else {
            continue;
        };
        if fan_out[i + 1..]
            .iter()
            .any(|b| b.position_column.as_deref() == Some(column))
        {
            faults.push(DeclarationFault {
                message: format!(
                    "source '{}': two `split_to_rows` entries write the position column '{}'",
                    source.name, column
                ),
                help: "give each entry its own `position_column`, or drop it from all but one \
                       — the two indexes are per-entry and cannot share a column"
                    .to_string(),
            });
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
        // `str::split("")` matches at every character boundary, so an empty
        // delimiter would turn one cell into a run of single characters
        // bracketed by two empty values. There is no reading of "split on
        // nothing" that produces what an author meant.
        if a.delimiter.is_empty() {
            faults.push(DeclarationFault {
                message: format!(
                    "source '{}': `split_values` on field '{}' declares an empty delimiter",
                    source.name, a.field
                ),
                help: "give a non-empty delimiter (the default is `;`), or drop the \
                       `split_values` entry if the cell holds a single value"
                    .to_string(),
            });
        }
        // Fanning a field out to rows consumes it; parsing it in-cell keeps it
        // and makes it multi-valued. One field cannot be both.
        if fan_out.iter().any(|e| e.field == a.field) {
            faults.push(DeclarationFault {
                message: format!(
                    "source '{}': field '{}' is declared in both `split_to_rows` and \
                     `split_values`",
                    source.name, a.field
                ),
                help: "fan the field out to rows, or parse it in-cell into several values \
                       — declaring both leaves no single shape for the column"
                    .to_string(),
            });
        }
    }

    // A `split_values` entry produces several values, which only a
    // `multiple: true` column can hold. Without the declaration the column
    // would be typed as a single value while carrying an array.
    if let Some(columns) = schema.bound_columns() {
        for entry in in_cell {
            if declares_multiple(&columns, &entry.field) {
                continue;
            }
            let known = columns
                .iter()
                .any(|c| c.name == entry.field || c.physical_name() == entry.field);
            if known {
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
            } else {
                faults.push(DeclarationFault {
                    message: format!(
                        "source '{}': `split_values` names the field '{}', which the schema \
                         does not declare at all",
                        source.name, entry.field
                    ),
                    help: format!(
                        "check the spelling against the `schema:` block, then declare the \
                         '{}' column `multiple: true` so it can hold the values the split \
                         produces",
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
///
/// Reads the schema's BOUND column list, so a multi-record schema's record
/// types are covered too: `Column` is the same type there, `deny_unknown_fields`
/// accepts `multiple: true` on it, and the superset the planner typechecks
/// against carries the attribute through. A `generated` or unresolved external
/// schema declares no columns to inspect.
pub fn multi_value_columns(schema: &SourceSchema) -> Vec<String> {
    schema
        .bound_columns()
        .unwrap_or_default()
        .into_iter()
        .filter(|c| c.is_multiple())
        .map(|c| c.name)
        .collect()
}

/// Map every node name to the set of source names whose DATA can reach it.
///
/// A deliberately different walk from the one that attributes `$doc.*` paths:
/// that one narrows a Combine to its DRIVING input, because a joined record
/// carries only the driver's document context. Column data has no such
/// narrowing — a combine's `emit` can project a column off the reference side —
/// so this walk unions every input, including a composition call site's full
/// port set (the primary-port-only `direct_input_names` would drop the rest).
/// Erring toward a larger set is the safe direction for `E359`: a false
/// positive is a compile error the author reads, a false negative is a run that
/// dies at the sink.
///
/// Nodes are walked in declaration order, which topology validation already
/// proved sound, so each node's set is the union of its inputs' sets and
/// Sources seed themselves.
pub fn source_data_reachability(
    nodes: &[Spanned<PipelineNode>],
) -> HashMap<String, BTreeSet<String>> {
    let mut reach: HashMap<String, BTreeSet<String>> = HashMap::new();
    for spanned in nodes {
        let name = spanned.value.name().to_string();
        let sources: BTreeSet<String> = match &spanned.value {
            PipelineNode::Source { .. } => std::iter::once(name.clone()).collect(),
            PipelineNode::Composition { inputs, header, .. } => {
                // `inputs:` values are `<node>` or `<node>.<port>`; the
                // producing node is the part before the dot. The primary
                // `input:` is carried too, since a single-port call declares
                // it there rather than in `inputs:`.
                let mut producers: Vec<&str> = inputs
                    .values()
                    .map(|upstream| upstream.split('.').next().unwrap_or(upstream))
                    .collect();
                producers.push(header.input.value.name());
                union_reach(&producers, &reach)
            }
            other => union_reach(&other.direct_input_names(), &reach),
        };
        reach.insert(name, sources);
    }
    reach
}

fn union_reach(inputs: &[&str], reach: &HashMap<String, BTreeSet<String>>) -> BTreeSet<String> {
    inputs
        .iter()
        .filter_map(|n| reach.get(*n))
        .flat_map(|s| s.iter().cloned())
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

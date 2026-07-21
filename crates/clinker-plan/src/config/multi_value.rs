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

use crate::config::format::{InputFormat, OutputFormat};
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
/// A duplicated fan-out field would fan the same group out twice, and on a
/// format whose reader tracks occurrences positionally a nested pair would
/// leave the inner group's membership ambiguous. This ran at XML reader
/// construction as an untyped, unspanned string error and never ran at all for
/// JSON; both formats are checked here instead, before any input is opened.
pub fn validate_source_declarations(
    source: &SourceConfig,
    schema: &SourceSchema,
) -> Vec<DeclarationFault> {
    let mut faults = Vec::new();
    let fan_out = source.split_to_rows.as_deref().unwrap_or(&[]);
    // Whether the reader assigns each extracted field to at most one declared
    // group by document position. The XML reader does — it records one index
    // range per occurrence while parsing, and a nested pair leaves a field
    // inside both ranges with no rule for which group owns it. The JSON reader
    // does not: it applies entries in declaration order over the flattened map,
    // so an outer fan-out leaves the inner field addressable for the next
    // entry, and a nested pair is a valid two-level expansion.
    let occurrence_tracked = matches!(source.format, InputFormat::Xml(_));
    for (i, a) in fan_out.iter().enumerate() {
        // A dotted element path with an empty segment matches no flattened key,
        // so the entry would fan nothing out and be a silent no-op.
        if a.field.is_empty() || a.field.split('.').any(str::is_empty) {
            faults.push(DeclarationFault {
                message: format!(
                    "source '{}': `split_to_rows` declares the field path '{}', which has an \
                     empty path segment",
                    source.name, a.field
                ),
                help: "name the repeated element by its record-relative dotted path \
                       (`line_items`, `Items.Item`) — no leading, trailing, or doubled dot"
                    .to_string(),
            });
        }
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
            } else if occurrence_tracked
                && (under_field_path(&a.field, &b.field) || under_field_path(&b.field, &a.field))
            {
                faults.push(DeclarationFault {
                    message: format!(
                        "source '{}': `split_to_rows` fields '{}' and '{}' nest — on an xml \
                         source, declared fan-out fields must name disjoint (non-nested) \
                         element groups",
                        source.name, a.field, b.field
                    ),
                    help: "fan out on the outer field alone, or on the inner one alone; the \
                           xml reader assigns each element to one occurrence group by document \
                           position, and a nested pair leaves that membership ambiguous"
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

    if let Some(columns) = schema.bound_columns() {
        // A `split_values` entry produces several values, which only a
        // `multiple: true` column can hold. Without the declaration the column
        // would be typed as a single value while carrying an array.
        for entry in in_cell {
            match resolve_declared_field(&columns, &entry.field) {
                DeclaredField::Multiple => {}
                DeclaredField::Single => faults.push(DeclarationFault {
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
                }),
                DeclaredField::AliasedTo(physical) => faults.push(DeclarationFault {
                    message: format!(
                        "source '{}': `split_values` names the field '{}', which is the \
                         column's exposed name — the column reads from the input field '{}', \
                         and the split runs against the names the document carries",
                        source.name, entry.field, physical
                    ),
                    help: format!(
                        "name the input field: `split_values` on '{physical}' (the column's \
                         `source_name`), not on the exposed column name"
                    ),
                }),
                DeclaredField::Absent => faults.push(DeclarationFault {
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
                }),
            }
        }
        // Fanning a field out to rows spends its occurrences one per record;
        // `multiple: true` collects them into one array on a single record.
        // Declaring both on the same field asks for both shapes at once.
        for entry in fan_out {
            if matches!(
                resolve_declared_field(&columns, &entry.field),
                DeclaredField::Multiple
            ) {
                faults.push(DeclarationFault {
                    message: format!(
                        "source '{}': field '{}' is declared `multiple: true` and also fanned \
                         out by `split_to_rows` — the first collects its occurrences into one \
                         array, the second spends them one per record",
                        source.name, entry.field
                    ),
                    help: format!(
                        "drop `multiple: true` from the '{}' schema column to fan its \
                         occurrences out to rows, or drop the `split_to_rows` entry to \
                         collect them into one array",
                        entry.field
                    ),
                });
            }
        }
    }
    faults
}

/// How a declared field name relates to the schema.
///
/// `split_values` and `split_to_rows` name fields of the source DOCUMENT, and
/// every reader matches them against the names the document carries — before
/// the declared-schema reprojection renames anything (the same rule
/// `multi_value_fields` applies when it hands the readers physical names). So
/// an aliased column is addressable only by its `source_name`, and naming its
/// exposed name instead is reported as its own fault rather than accepted:
/// accepting it would compile clean and then never match at run time.
enum DeclaredField {
    /// A column reads this input field and is declared `multiple: true`.
    Multiple,
    /// A column reads this input field, declared single-valued.
    Single,
    /// No column reads this input field, but one is EXPOSED under this name and
    /// reads the carried physical name instead.
    AliasedTo(String),
    /// The schema names this field nowhere.
    Absent,
}

fn resolve_declared_field(columns: &[Column], field: &str) -> DeclaredField {
    if let Some(column) = columns.iter().find(|c| c.physical_name() == field) {
        return if column.is_multiple() {
            DeclaredField::Multiple
        } else {
            DeclaredField::Single
        };
    }
    match columns.iter().find(|c| c.name == field) {
        Some(column) => DeclaredField::AliasedTo(column.physical_name().to_string()),
        None => DeclaredField::Absent,
    }
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

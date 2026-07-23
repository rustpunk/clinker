//! Plan-time validation of the multi-value surface.
//!
//! Gates fired from `compile_with_diagnostics`, so a failure is a spanned
//! `clinker explain` diagnostic rather than a mid-run abort:
//!
//! - **E358** — a malformed declaration on a source: two `split_to_rows`
//!   entries naming the same or nested field groups, a duplicate
//!   `split_values` field, or a `split_values` field the schema does not
//!   declare `multiple: true`.
//! - **E359** — a `multiple: true` column reaching an output whose format has
//!   no multi-value encoding, which would degrade or reject the value at the
//!   sink instead of at compile.
//! - **E361** — a `multiple: true` column on a source whose format has no way
//!   to PRODUCE one, which would bind the column as an array and then hand
//!   CXL a scalar.
//! - **E362** — a malformed `join_values` declaration on an output: declared on
//!   a format whose writer never consumes it, a duplicate field, an empty
//!   delimiter, or an escape/delimiter that is not a single character under
//!   `on_conflict: escape`. The write-side mirror of E358.
//!
//! E359 and E361 are the two arrows of the same rule, and each reads its own
//! per-format capability table: [`output_encodes_multi_value`] for the write
//! side, `input_multi_value_support` for the read side.

use std::collections::{BTreeSet, HashMap};

use clinker_format::{Column, OnConflict, SourceSchema, SplitToRowsMode, under_field_path};

use crate::config::OutputConfig;
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
/// - `csv` — a `multiple:` field joins into one delimited cell, defaulting to
///   `;` with `on_conflict: error`; `join_values` overrides the policy per
///   field (#917). No multi-record CSV writer exists, so the answer is
///   unconditional.
/// - `xml` — repeated child elements: tracked at
///   https://github.com/rustpunk/clinker/issues/916.
/// - `fixed_width` — repeating field groups: tracked at
///   https://github.com/rustpunk/clinker/issues/918.
/// - `edifact` / `x12` / `hl7` / `swift` — these writers emit a fixed
///   positional segment grammar with no repetition slot to fill.
fn output_encodes_multi_value(format: &OutputFormat) -> bool {
    match format {
        OutputFormat::Json(_) => true,
        OutputFormat::Csv(_) => true,
        OutputFormat::Xml(_) => false,
        OutputFormat::FixedWidth(_) => false,
        OutputFormat::Edifact(_)
        | OutputFormat::X12(_)
        | OutputFormat::Hl7(_)
        | OutputFormat::Swift(_) => false,
    }
}

/// How an input format can carry a column declared `multiple: true`.
///
/// The read-side counterpart of [`output_encodes_multi_value`], and the single
/// per-format capability table behind `E361`. A verdict rather than a bool
/// because the three answers need three different remediations, and collapsing
/// them would either reject a workable `csv` declaration or accept an
/// unworkable `x12` one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MultiValueInput {
    /// The reader materializes an array from the document's own repetition, so
    /// a bare `multiple: true` is enough.
    Native,
    /// The format carries one value per field, but a field's text can hold
    /// several separated by a delimiter. A `multiple: true` column is supplied
    /// once a `split_values` entry says what that delimiter is: the
    /// single-schema CSV and fixed-width readers then parse the cell into the
    /// array the column holds. A column no entry covers — or any such column on
    /// a multi-record source, whose backend never receives the block — is
    /// rejected all the same, since it would bind as an array and receive the
    /// raw cell. This verdict records that the format CAN support the
    /// declaration, which is what separates it from
    /// [`MultiValueInput::PositionalAxis`]; only the remediation differs.
    InCell,
    /// Repetition is a positional coordinate, not a list. A repeated composite
    /// serializes as two axes interleaved in one element (`11:B:1^12:B:2`),
    /// which a flat array cannot express; the faithful shape is one column per
    /// occurrence, which is what the HL7 `split_fields` declaration produces.
    /// This is permanent, not pending — wiring the array binding in would
    /// misrepresent the wire format rather than complete it.
    PositionalAxis,
}

/// Which of the three a format is.
///
/// One arm per format, so adding an input format forces a decision here rather
/// than defaulting into a verdict nobody chose.
///
/// - `json` — a JSON array is the format's own native shape.
/// - `xml` — repeated child elements, collected in document order.
/// - `csv` / `fixed_width` — one value per cell on the wire, but
///   delimited-in-cell text is a long-standing convention for both. A
///   `split_values` entry declares the delimiter, and the single-schema readers
///   parse the cell into the array a `multiple: true` column holds (#930).
/// - `edifact` / `x12` / `hl7` / `swift` — positional segment grammars.
fn input_multi_value_support(format: &InputFormat) -> MultiValueInput {
    match format {
        InputFormat::Json(_) => MultiValueInput::Native,
        InputFormat::Xml(_) => MultiValueInput::Native,
        InputFormat::Csv(_) => MultiValueInput::InCell,
        InputFormat::FixedWidth(_) => MultiValueInput::InCell,
        InputFormat::Edifact(_)
        | InputFormat::X12(_)
        | InputFormat::Hl7(_)
        | InputFormat::Swift(_) => MultiValueInput::PositionalAxis,
    }
}

/// Whether a format's reader is handed the source's `split_to_rows`
/// declarations. A declaration a reader never receives is a silent no-op — the
/// same failure `E360` exists to stop for the retired `array_paths:` key — so
/// each declaration kind carries its own capability. Only the JSON and XML
/// readers fan a repeated field out to rows (over a file, and for both formats
/// over a REST response body); the delimited-cell formats do not.
fn format_reads_split_to_rows(format: &InputFormat) -> bool {
    matches!(format, InputFormat::Json(_) | InputFormat::Xml(_))
}

/// Whether a format's reader is handed the source's `split_values` declarations.
///
/// JSON and XML always are. The delimited-cell formats (`csv`, `fixed_width`)
/// are too — a `split_values` entry parses a cell into the array a
/// `multiple: true` column holds — but only on their single-schema reader: the
/// multi-record backend does not consume the block, so declaring it on a
/// multi-record source of either format is a no-op the gate rejects. Segment
/// grammars never carry the block.
fn format_reads_split_values(format: &InputFormat, schema: &SourceSchema) -> bool {
    match format {
        InputFormat::Json(_) | InputFormat::Xml(_) => true,
        InputFormat::Csv(_) | InputFormat::FixedWidth(_) => {
            !matches!(schema, SourceSchema::MultiRecord { .. })
        }
        InputFormat::Edifact(_)
        | InputFormat::X12(_)
        | InputFormat::Hl7(_)
        | InputFormat::Swift(_) => false,
    }
}

/// The outer of two `split_to_rows` entries when one field path nests inside
/// the other, or `None` when the two name disjoint groups.
fn nesting_parent<'a>(
    a: &'a clinker_format::SplitToRows,
    b: &'a clinker_format::SplitToRows,
) -> Option<&'a clinker_format::SplitToRows> {
    if under_field_path(&b.field, &a.field) {
        Some(a)
    } else if under_field_path(&a.field, &b.field) {
        Some(b)
    } else {
        None
    }
}

/// One malformed-declaration finding: the message body, and the help text that
/// says how to resolve it. The gate below wraps these in a spanned diagnostic.
struct DeclarationFault {
    message: String,
    help: String,
}

/// Validate one source's `split_to_rows` / `split_values` declarations against
/// its resolved schema (E358).
///
/// A duplicated fan-out field would fan the same group out twice, and on a
/// format whose reader tracks occurrences positionally a nested pair would
/// leave the inner group's membership ambiguous. This ran at XML reader
/// construction as an untyped, unspanned string error and never ran at all for
/// JSON; both formats are checked here instead, before any input is opened.
fn validate_source_declarations(
    source: &SourceConfig,
    schema: &SourceSchema,
) -> Vec<DeclarationFault> {
    let mut faults = Vec::new();
    let fan_out = source.split_to_rows.as_deref().unwrap_or(&[]);
    let in_cell = source.split_values.as_deref().unwrap_or(&[]);

    // A declaration the format's reader is never handed does nothing at all,
    // and nothing downstream reports it. Reject the block itself rather than
    // only its contents, so the no-op fails at compile the way the retired
    // `array_paths:` key does under E360. Each declaration kind has its own
    // per-format capability: the delimited-cell formats read `split_values` but
    // not `split_to_rows`, so a source can legitimately carry one block and be
    // told to delete the other.
    let reads_fan_out = format_reads_split_to_rows(&source.format);
    let reads_in_cell = format_reads_split_values(&source.format, schema);
    let format = source.format.format_name();
    for (key, unread, remedy) in [
        (
            "split_to_rows",
            !reads_fan_out && !fan_out.is_empty(),
            "fanning a repeated field out to one record per occurrence is read by the `json` \
             and `xml` readers",
        ),
        (
            "split_values",
            !reads_in_cell && !in_cell.is_empty(),
            "parsing a delimited cell into several values is read by the `json`, `xml`, and — on \
             a single-schema source — `csv` and `fixed_width` readers",
        ),
    ] {
        if !unread {
            continue;
        }
        faults.push(DeclarationFault {
            message: format!(
                "source '{}': `{key}` is declared on a `{format}` source, whose reader is \
                 never handed it — the declaration would be a silent no-op",
                source.name
            ),
            help: format!("remove the `{key}` block from this source: {remedy}"),
        });
    }

    // The content checks below run only against declarations the reader
    // actually consumes; a block rejected above as an unread no-op is treated as
    // empty here, so the author is told to delete it rather than also what is
    // wrong inside it.
    let fan_out: &[clinker_format::SplitToRows] = if reads_fan_out { fan_out } else { &[] };
    let in_cell: &[clinker_format::SplitValues] = if reads_in_cell { in_cell } else { &[] };

    // Whether the reader assigns each extracted field to at most one declared
    // group by document position. The XML reader does — it records one index
    // range per occurrence while parsing, and a nested pair leaves a field
    // inside both ranges with no rule for which group owns it. The JSON reader
    // does not: it applies entries in declaration order over the flattened map,
    // so a nested pair composes into a two-level expansion — but only when the
    // outer entry keeps the group's dotted path, which `mode: split` does and
    // `mode: extract` does not (see the nesting arm below).
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
            } else if let Some(outer) = nesting_parent(a, b) {
                if occurrence_tracked {
                    faults.push(DeclarationFault {
                        message: format!(
                            "source '{}': `split_to_rows` fields '{}' and '{}' nest — on an xml \
                             source, declared fan-out fields must name disjoint (non-nested) \
                             element groups",
                            source.name, a.field, b.field
                        ),
                        help: "fan out on the outer field alone, or on the inner one alone; the \
                               xml reader assigns each element to one occurrence group by \
                               document position, and a nested pair leaves that membership \
                               ambiguous"
                            .to_string(),
                    });
                } else if outer.mode == SplitToRowsMode::Extract {
                    // `extract` lifts the occurrence's own keys to the top
                    // level, so the group's dotted path stops existing and the
                    // inner entry — which addresses the field BY that path —
                    // matches nothing. `keep_empty`'s default then passes the
                    // record through unfanned, one record where the author
                    // asked for one per inner occurrence.
                    faults.push(DeclarationFault {
                        message: format!(
                            "source '{}': `split_to_rows` fields '{}' and '{}' nest, and the \
                             outer entry's `mode: extract` lifts the occurrence's own keys out \
                             from under '{outer_field}' — the inner entry addresses a path that \
                             no longer exists and would fan nothing out",
                            source.name,
                            a.field,
                            b.field,
                            outer_field = outer.field
                        ),
                        help: format!(
                            "declare the outer entry `mode: split` (`- {{ field: {outer_field}, \
                             mode: split }}`), which keeps each occurrence's fields under their \
                             dotted path so the inner entry still addresses them; or fan out on \
                             one of the two fields alone",
                            outer_field = outer.field
                        ),
                    });
                }
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
        // nothing" that produces what an author meant. `json: true` reads the
        // whole cell as a JSON array and ignores the delimiter, so it is exempt —
        // symmetric with the write-side E362, which exempts an empty delimiter
        // under `encode_json`.
        if a.delimiter.is_empty() && !a.json {
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
        // The `escape` / `json` recovery modes — the read-side inverses of the
        // CSV sink's `join_values` `on_conflict: escape` / `encode_json` — are
        // honored only by the single-schema CSV reader (this loop runs on a block
        // the reader consumes, so a CSV block here is single-schema). On any other
        // read format the reader splits on the bare delimiter and silently ignores
        // them, so declaring them is a no-op the gate rejects.
        let csv_source = matches!(source.format, InputFormat::Csv(_));
        if (!a.escape.is_empty() || a.json) && !csv_source {
            faults.push(DeclarationFault {
                message: format!(
                    "source '{}': `split_values` on field '{}' sets `{}`, which only the CSV \
                     reader honors — a `{}` reader splits on the bare delimiter and ignores it",
                    source.name,
                    a.field,
                    if a.json { "json" } else { "escape" },
                    source.format.format_name(),
                ),
                help: "drop `escape` / `json` on a non-CSV source: a JSON or XML array recovers \
                       natively, and the delimited readers split on the plain delimiter"
                    .to_string(),
            });
        }
        // `json: true` reads the whole cell as an embedded JSON array and ignores
        // the delimiter, so a co-declared `escape` would never apply.
        if a.json && !a.escape.is_empty() {
            faults.push(DeclarationFault {
                message: format!(
                    "source '{}': `split_values` on field '{}' sets both `json: true` and \
                     `escape` — `json` reads the whole cell as a JSON array, so `escape` is unused",
                    source.name, a.field
                ),
                help: "drop `escape` when `json: true`, or drop `json` to split on the delimiter \
                       with escape handling"
                    .to_string(),
            });
        }
        // The escape-aware split un-escapes character by character (the read-side
        // inverse of the sink's `on_conflict: escape`), so under `escape` the
        // delimiter and escape must each be a single character and must differ.
        // Gated on `csv_source`: a non-CSV source already got the "only the CSV
        // reader honors escape/json" fault above, so running the shape checks
        // there too would emit a redundant second diagnostic for a declaration
        // that is already rejected and whose escape the reader ignores anyway.
        if csv_source && !a.escape.is_empty() && !a.json {
            if a.delimiter.chars().count() != 1 {
                faults.push(DeclarationFault {
                    message: format!(
                        "source '{}': `split_values` on field '{}' sets `escape` with a \
                         multi-character delimiter '{}'",
                        source.name, a.field, a.delimiter
                    ),
                    help: "use a single-character `delimiter` with `escape`, or drop `escape` to \
                           split on the plain multi-character delimiter"
                        .to_string(),
                });
            } else if a.escape.chars().count() != 1 {
                faults.push(DeclarationFault {
                    message: format!(
                        "source '{}': `split_values` on field '{}' sets an `escape` '{}' that is \
                         not a single character",
                        source.name, a.field, a.escape
                    ),
                    help: "give a single-character `escape` (`\\` matches the sink default)"
                        .to_string(),
                });
            } else if a.escape == a.delimiter {
                faults.push(DeclarationFault {
                    message: format!(
                        "source '{}': `split_values` on field '{}' sets the `escape` equal to the \
                         delimiter '{}'",
                        source.name, a.field, a.delimiter
                    ),
                    help:
                        "give an `escape` different from the `delimiter` — sharing one character \
                           makes an escaped delimiter and an escape marker indistinguishable"
                            .to_string(),
                });
            }
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

/// Validate that a source's format can produce the `multiple: true` columns
/// its schema declares (E361).
///
/// The read-side arrow of the rule `E359` enforces on the write side.
/// `Column::bound_type` binds every `multiple: true` column as `Type::Array`
/// regardless of format, so without this a `csv` or `x12` source typechecks
/// downstream code against an array and then delivers the scalar its reader
/// actually produced.
///
/// A per-source check, not a reachability walk: the mismatch is between one
/// source's schema and its own format, with no path through the DAG involved.
///
/// A delimited-cell format supplies the column only when a `split_values` entry
/// names its input field: the single-schema CSV and fixed-width readers then
/// parse the cell into the array the column holds (see
/// [`format_reads_split_values`]). A `multiple: true` column no entry covers —
/// or any such column on a multi-record source, whose backend never receives
/// the block — is rejected together with the segment formats, since accepting
/// it would bind the column as an array and then deliver the raw cell, the exact
/// disagreement this gate exists to prevent. Only the remediation differs
/// between the verdicts. Reported per source, listing every column the format
/// cannot supply, so one source yields one diagnostic.
fn validate_multi_value_input(
    source: &SourceConfig,
    schema: &SourceSchema,
) -> Option<DeclarationFault> {
    let support = input_multi_value_support(&source.format);
    if support == MultiValueInput::Native {
        return None;
    }
    let columns = schema.bound_columns()?;
    // A delimited-cell format supplies a `multiple: true` column when a
    // `split_values` entry names its input field: the reader parses the cell
    // into the array the column holds. That excuse applies only where the reader
    // consumes the block (single-schema csv / fixed_width — json/xml are Native
    // and never reach here); a column no entry covers stays unsupplied and
    // still faults.
    let reads_in_cell = format_reads_split_values(&source.format, schema);
    let declared = source.split_values.as_deref().unwrap_or(&[]);
    let unsupplied: Vec<&Column> = columns
        .iter()
        .filter(|c| {
            c.is_multiple()
                && !(reads_in_cell && declared.iter().any(|e| e.field == c.physical_name()))
        })
        .collect();
    if unsupplied.is_empty() {
        return None;
    }
    let format = source.format.format_name();
    let named = unsupplied
        .iter()
        .map(|c| format!("'{}'", c.name))
        .collect::<Vec<_>>()
        .join(", ");
    // `None` for a format that needs no remediation, which is also the answer
    // to whether there is a fault at all — so the native arm short-circuits the
    // whole function rather than asserting it cannot be reached.
    let help = match support {
        MultiValueInput::Native => None,
        // A single-schema delimited-cell reader consumes `split_values`, so
        // declaring one is the fix. A multi-record reader does not (E358 rejects
        // the block as a no-op on the same source), so recommending it here would
        // contradict that gate — point only at the transform route instead.
        MultiValueInput::InCell if reads_in_cell => Some(format!(
            "declare a `split_values` entry so the {format} reader parses the cell into the \
             array the column holds (`split_values:` `- {{ field: {phys}, delimiter: \";\" }}`), \
             naming the column's input field and the delimiter its text uses; or drop \
             `multiple: true` and split the cell in a transform where the parts are needed \
             (`{col}.split(\";\")`)",
            phys = unsupplied[0].physical_name(),
            col = unsupplied[0].name,
        )),
        MultiValueInput::InCell => Some(format!(
            "this multi-record {format} source's reader does not consume `split_values`, so \
             declaring the split there is a no-op — drop `multiple: true` and split the cell in a \
             transform where the parts are needed (`{col}.split(\";\")`), or restructure the \
             source as a single-schema {format} where a `split_values` entry is read",
            col = unsupplied[0].name,
        )),
        // HL7 exposes the positional axes as its own declaration; the other
        // three segment formats do not, so promising one would dangle.
        MultiValueInput::PositionalAxis if matches!(source.format, InputFormat::Hl7(_)) => Some(
            "HL7 repetition is a positional axis, not a list: declare the field under \
             `options.split_fields` with its repetition and component counts so each occurrence \
             gets its own column, and drop `multiple: true` from the schema column"
                .to_string(),
        ),
        MultiValueInput::PositionalAxis => Some(format!(
            "{format} repetition is a positional axis, not a list, and a flat array cannot \
             express it — drop `multiple: true` from the column; each occurrence arrives \
             verbatim in the element text"
        )),
    }?;
    Some(DeclarationFault {
        message: format!(
            "source '{name}': the schema declares the multi-value column(s) {named} \
             (`multiple: true`), and a `{format}` source has no way to produce a field holding \
             more than one value",
            name = source.name
        ),
        help,
    })
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
fn multi_value_columns(schema: &SourceSchema) -> Vec<String> {
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
/// Sources seed themselves and every other node takes the union of its inputs'
/// sets. The sweep repeats to a fixpoint rather than running once in
/// declaration order: nothing requires a node to be declared after its
/// producer, and a single pass would leave any node declared before its
/// producer holding an empty set — which reads as "no source reaches this
/// output" and turns the gate off exactly where it is needed. Each pass only
/// ever adds source names to a set, so the sets climb a finite lattice
/// (node count × source count) and the loop terminates.
fn source_data_reachability(nodes: &[Spanned<PipelineNode>]) -> HashMap<String, BTreeSet<String>> {
    let mut reach: HashMap<String, BTreeSet<String>> = HashMap::new();
    let mut changed = true;
    while changed {
        changed = false;
        for spanned in nodes {
            let name = spanned.value.name().to_string();
            let sources: BTreeSet<String> = match &spanned.value {
                PipelineNode::Source { .. } => std::iter::once(name.clone()).collect(),
                PipelineNode::Composition { inputs, header, .. } => {
                    // `inputs:` values are `<node>` or `<node>.<port>`; the
                    // producing node is the part before the dot. The primary
                    // `input:` is carried too, since a single-port call
                    // declares it there rather than in `inputs:`.
                    let mut producers: Vec<&str> = inputs
                        .values()
                        .map(|upstream| upstream.split('.').next().unwrap_or(upstream))
                        .collect();
                    producers.push(header.input.value.name());
                    union_reach(&producers, &reach)
                }
                other => union_reach(&other.direct_input_names(), &reach),
            };
            if reach.get(&name) != Some(&sources) {
                reach.insert(name, sources);
                changed = true;
            }
        }
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

/// One finding from a multi-value gate, carrying its own diagnostic code and
/// the index of the node it belongs to in the list that was handed in.
///
/// The gates below take a node LIST rather than reading the top-level pipeline,
/// because a composition body's nodes need the identical checks and never
/// appear in the call-site pipeline's `nodes:` — the body file is re-read and
/// bound separately. Returning an index instead of a span lets each caller
/// anchor the diagnostic the way its own nodes are anchored: the top-level
/// pipeline by document line, a body by its node span and body-file prefix.
pub struct NodeFault {
    pub node_index: usize,
    pub code: &'static str,
    pub message: String,
    pub help: String,
}

/// Every source's declared schema, by node name, with an external
/// `.schema.yaml` folded inline.
///
/// The gates all read declared columns, so they cannot run off `body.schema`
/// directly: at the point they fire, a [`SourceSchema::File`] is still
/// unresolved — the resolution pass that folds it inline sits further down,
/// past the fatal-error return — and an unresolved `File` declares no columns,
/// which would leave every gate inert for a pipeline that keeps its schema in a
/// file. A file that fails to LOAD is left out entirely: the resolution pass
/// reports that as E157, and reporting it twice from two passes would be worse
/// than these gates skipping the source.
fn resolved_source_schemas(nodes: &[Spanned<PipelineNode>]) -> HashMap<&str, SourceSchema> {
    nodes
        .iter()
        .filter_map(|spanned| match &spanned.value {
            PipelineNode::Source {
                header,
                config: body,
            } => {
                let schema = match &body.schema {
                    SourceSchema::File(path) => {
                        crate::schema::load_source_schema(std::path::Path::new(path)).ok()?
                    }
                    inline => inline.clone(),
                };
                Some((header.name.as_str(), schema))
            }
            _ => None,
        })
        .collect()
}

/// The per-source multi-value gates — E360, E361, E358 — over one node list.
pub fn source_node_faults(nodes: &[Spanned<PipelineNode>]) -> Vec<NodeFault> {
    let schemas = resolved_source_schemas(nodes);
    let mut faults = Vec::new();
    for (node_index, spanned) in nodes.iter().enumerate() {
        let PipelineNode::Source {
            header,
            config: body,
        } = &spanned.value
        else {
            continue;
        };
        // E360 — a source still carrying the superseded `array_paths:` block.
        // Nothing reads it any more, and a source config is flattened into its
        // node so serde discards an unrecognized key silently: without this
        // gate the pipeline compiles and runs, emitting one record per document
        // where it used to emit one per array element. Outside the schema guard
        // below, since the retired key is wrong whatever the schema says.
        if body.source.array_paths.is_some() {
            faults.push(NodeFault {
                node_index,
                code: "E360",
                message: format!(
                    "source '{}' declares `array_paths:`, which the multi-value declarations \
                     replaced — the key is no longer read, so leaving it in place would drop the \
                     fan-out silently",
                    header.name
                ),
                help: "replace it: an `explode` path becomes a `split_to_rows:` entry (`mode: \
                       extract` lifts the element's fields to the top level, `mode: split` keeps \
                       the record shape; add `keep_empty: false` to keep the old behavior of \
                       dropping a record whose array was empty), a delimited cell becomes a \
                       `split_values:` entry, and a path kept as an array becomes `multiple: \
                       true` on the schema column"
                    .to_string(),
            });
        }
        let Some(schema) = schemas.get(header.name.as_str()) else {
            continue;
        };
        // E361 — a `multiple: true` column on a source whose format has no way
        // to produce one. The read-side arrow of E359: `bound_type` binds the
        // column as an array for every format, so without this the typechecker
        // sees an array downstream while the reader delivers the scalar it
        // actually parsed.
        if let Some(fault) = validate_multi_value_input(&body.source, schema) {
            faults.push(NodeFault {
                node_index,
                code: "E361",
                message: fault.message,
                help: fault.help,
            });
        }
        // E358 — a malformed multi-value declaration: duplicate or nested
        // `split_to_rows` fields, a duplicate `split_values` field, a
        // `split_values` field the schema does not declare `multiple: true`, or
        // either block on a format whose reader is never handed it. The
        // nesting/duplicate rejection used to run at XML reader construction as
        // an unspanned string error and never ran for JSON at all.
        for fault in validate_source_declarations(&body.source, schema) {
            faults.push(NodeFault {
                node_index,
                code: "E358",
                message: fault.message,
                help: fault.help,
            });
        }
    }
    faults
}

/// Validate an output's `join_values` declarations (E362), the write-side
/// mirror of E358's source-declaration checks.
///
/// The block is consumed by the CSV writer only, so declaring it on any other
/// output format is a silent no-op the gate rejects — the same LD-011 spirit as
/// E358's "reader never handed it". On a CSV output each entry is checked: no
/// duplicate field, a non-empty delimiter, and — under `on_conflict: escape`,
/// whose invertible escaping is defined char-wise — a single-character delimiter
/// and escape.
///
/// Deliberately does NOT require the field to be a statically-declared
/// `multiple:` column, unlike E358's `split_values` check. A write-side array
/// can arise at runtime (a `match: collect` combine output), so a static
/// requirement would reject a legitimate override; an entry that never matches
/// an array is simply an inert override.
fn validate_join_declarations(output: &OutputConfig) -> Vec<DeclarationFault> {
    let mut faults = Vec::new();
    let entries = output.join_values.as_deref().unwrap_or(&[]);
    if entries.is_empty() {
        return faults;
    }
    let format = output.format.format_name();
    if !matches!(output.format, OutputFormat::Csv(_)) {
        faults.push(DeclarationFault {
            message: format!(
                "output '{}': `join_values` is declared on a `{format}` output, whose writer \
                 never consumes it — the declaration would be a silent no-op",
                output.name
            ),
            help: "remove the `join_values` block: joining several values into one delimited \
                   cell is written by the `csv` writer only. A `json` output emits a native \
                   array, and the `xml` / `fixed_width` / segment writers have no delimited-cell \
                   join."
                .to_string(),
        });
        // Once the format cannot consume the block at all, the per-entry checks
        // are moot — the author is told to delete the whole block.
        return faults;
    }
    for (i, entry) in entries.iter().enumerate() {
        if entries[i + 1..].iter().any(|o| o.field == entry.field) {
            faults.push(DeclarationFault {
                message: format!(
                    "output '{}': `join_values` declares the field '{}' more than once",
                    output.name, entry.field
                ),
                help: "declare each field once, with the delimiter and policy it should join with"
                    .to_string(),
            });
        }
        // The delimiter matters only for the delimited policies (`error`,
        // `escape`); `encode_json` ignores it, so an empty or multi-character
        // delimiter under `encode_json` is harmless and exempt.
        if entry.on_conflict != OnConflict::EncodeJson && entry.delimiter.is_empty() {
            faults.push(DeclarationFault {
                message: format!(
                    "output '{}': `join_values` on field '{}' declares an empty delimiter",
                    output.name, entry.field
                ),
                help: "give a non-empty delimiter (the default is `;`) to separate the values in \
                       the joined cell"
                    .to_string(),
            });
        }
        // The delimited policies (`error`, `escape`) need a single-character
        // delimiter. Under `error` the collision check tests whether a value
        // contains the delimiter, which misses a spurious boundary a
        // multi-character delimiter can form between two adjacent values; under
        // `escape` the char-by-char escaping cannot round-trip a multi-character
        // delimiter.
        if entry.on_conflict != OnConflict::EncodeJson && entry.delimiter.chars().count() > 1 {
            faults.push(DeclarationFault {
                message: format!(
                    "output '{}': `join_values` on field '{}' uses a multi-character delimiter \
                     '{}' with `on_conflict: {}`",
                    output.name,
                    entry.field,
                    entry.delimiter,
                    on_conflict_name(entry.on_conflict),
                ),
                help: "use a single-character `delimiter` for `on_conflict: error` / `escape`, or \
                       switch to `encode_json`, which encodes the whole field as a JSON array and \
                       places no constraint on the delimiter"
                    .to_string(),
            });
        }
        // `on_conflict: escape` escapes and un-escapes character by character, so
        // the escape must be a single character and must differ from the
        // delimiter — otherwise an escaped delimiter and an escape marker are
        // indistinguishable on read.
        if entry.on_conflict == OnConflict::Escape {
            if entry.escape.chars().count() != 1 {
                faults.push(DeclarationFault {
                    message: format!(
                        "output '{}': `join_values` on field '{}' uses `on_conflict: escape` with \
                         an escape '{}' that is not a single character",
                        output.name, entry.field, entry.escape
                    ),
                    help: "give a single-character `escape` (the default is `\\`) — the reader \
                           un-escapes character by character"
                        .to_string(),
                });
            } else if entry.escape == entry.delimiter {
                faults.push(DeclarationFault {
                    message: format!(
                        "output '{}': `join_values` on field '{}' uses `on_conflict: escape` with \
                         the escape equal to the delimiter '{}'",
                        output.name, entry.field, entry.delimiter
                    ),
                    help: "give an `escape` different from the `delimiter`; sharing one character \
                           makes an escaped delimiter and an escape marker indistinguishable on \
                           read"
                        .to_string(),
                });
            }
        }
    }
    faults
}

/// The `on_conflict` policy's YAML spelling, for a diagnostic message.
fn on_conflict_name(policy: OnConflict) -> &'static str {
    match policy {
        OnConflict::Error => "error",
        OnConflict::Escape => "escape",
        OnConflict::EncodeJson => "encode_json",
    }
}

/// The E359 output gate over one node list (and the E362 `join_values` gate): a
/// `multiple: true` column reaching — or declared directly on — an output whose
/// format has no multi-value encoding.
///
/// The remaining non-encoding writers reject a multi-value payload at run time,
/// so without this the failure lands mid-stream on record N rather than at
/// compile. Reachability is source-to-output through the DAG: a column a
/// transform drops before the sink is still reported, which is the conservative
/// direction — a false positive is a compile error the author can see and
/// resolve, a false negative is a run that dies partway through. The walk is
/// [`source_data_reachability`], NOT the `$doc` attribution walk: that one
/// narrows a Combine to its driving input, which would miss a column the combine
/// projects off its reference side.
///
/// An output's OWN `schema:` block is checked too, for a non-encoding format: it
/// is the same `Column` type, it accepts `multiple: true`, and a non-encoding
/// writer does not honor the attribute — the fixed-width writer's
/// `Column -> FieldDef` conversion drops it outright — so an author who declares
/// repetition on the surface they are writing to would otherwise get it silently
/// ignored. `json` and `csv` encode it, so the gate skips them.
///
/// Also runs the per-output E362 gate ([`validate_join_declarations`]) over the
/// output's `join_values` block, which — unlike E359 — applies whatever the
/// format is, since a `csv` output legitimately carries it.
pub fn output_node_faults(nodes: &[Spanned<PipelineNode>]) -> Vec<NodeFault> {
    let schemas = resolved_source_schemas(nodes);
    let multi_value_by_source: HashMap<&str, Vec<String>> = schemas
        .iter()
        .map(|(name, schema)| (*name, multi_value_columns(schema)))
        .filter(|(_, columns)| !columns.is_empty())
        .collect();
    let mut faults = Vec::new();
    let reachability = source_data_reachability(nodes);
    for (node_index, spanned) in nodes.iter().enumerate() {
        let PipelineNode::Output {
            header,
            config: body,
        } = &spanned.value
        else {
            continue;
        };
        let output = &body.output;
        // E362 — a malformed `join_values` declaration, checked before the
        // encoding gate below: `csv` (the format that consumes it) short-circuits
        // that gate, so its own per-entry checks must run here or never.
        for fault in validate_join_declarations(output) {
            faults.push(NodeFault {
                node_index,
                code: "E362",
                message: fault.message,
                help: fault.help,
            });
        }
        if output_encodes_multi_value(&output.format) {
            continue;
        }
        let format = output.format.format_name();
        if let Some(declared) = output.schema.as_ref().map(multi_value_columns)
            && !declared.is_empty()
        {
            faults.push(NodeFault {
                node_index,
                code: "E359",
                message: format!(
                    "output '{out}': its own `schema:` declares the multi-value column(s) \
                     {columns} (`multiple: true`), and a `{format}` output has no encoding for a \
                     field holding more than one value",
                    out = header.name,
                    columns = quoted_list(&declared),
                ),
                help: "drop `multiple: true` from the output column — no writer encodes \
                       repetition today, so the declaration would be accepted and ignored — or \
                       write this stream to a `json` output, which does"
                    .to_string(),
            });
        }
        let Some(feeding) = reachability.get(header.name.as_str()) else {
            continue;
        };
        for source_name in feeding {
            let Some(columns) = multi_value_by_source.get(source_name.as_str()) else {
                continue;
            };
            faults.push(NodeFault {
                node_index,
                code: "E359",
                message: format!(
                    "output '{out}': source '{source_name}' declares the multi-value column(s) \
                     {columns} (`multiple: true`), and a `{format}` output has no encoding for a \
                     field holding more than one value",
                    out = header.name,
                    columns = quoted_list(columns),
                ),
                help: "write this stream to a `json` output, collapse the column to a single \
                       value in a transform before the sink, or drop `multiple: true` from the \
                       source column if the field never actually repeats; when the schema is \
                       shared across sources and only some of them repeat the field, clear it \
                       for this one with a channel source patch (`schema: { <column>: { \
                       multiple: false } }`) rather than editing the shared declaration"
                    .to_string(),
            });
        }
    }
    faults
}

fn quoted_list(names: &[String]) -> String {
    names
        .iter()
        .map(|c| format!("'{c}'"))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The read-side table, pinned verdict by verdict. Adding an input format
    /// fails to compile at `input_multi_value_support` — the match names every
    /// variant with no wildcard — and this pins what each existing one answers,
    /// so a verdict cannot be changed silently either.
    #[test]
    fn every_input_formats_multi_value_verdict_is_pinned() {
        let cases = [
            (InputFormat::Json(None), MultiValueInput::Native),
            (InputFormat::Xml(None), MultiValueInput::Native),
            (InputFormat::Csv(None), MultiValueInput::InCell),
            (InputFormat::FixedWidth(None), MultiValueInput::InCell),
            (InputFormat::Edifact(None), MultiValueInput::PositionalAxis),
            (InputFormat::X12(None), MultiValueInput::PositionalAxis),
            (InputFormat::Hl7(None), MultiValueInput::PositionalAxis),
            (InputFormat::Swift(None), MultiValueInput::PositionalAxis),
        ];
        for (format, expected) in cases {
            assert_eq!(
                input_multi_value_support(&format),
                expected,
                "{}",
                format.format_name()
            );
        }
    }

    #[test]
    fn json_and_csv_encode_multi_value_today() {
        // `json` (native arrays) and `csv` (join into a delimited cell, #917)
        // encode a `multiple:` field; the remaining writers do not yet.
        assert!(output_encodes_multi_value(&OutputFormat::Json(None)));
        assert!(output_encodes_multi_value(&OutputFormat::Csv(None)));
        for format in [
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

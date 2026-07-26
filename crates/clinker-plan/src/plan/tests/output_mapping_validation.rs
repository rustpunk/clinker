//! The plan-time gates on an Output's `mapping:` block (E364 / E365).
//!
//! `mapping:` used to be a map whose keys the executor read as SOURCE column
//! names and whose keys the documentation described as OUTPUT column names. An
//! entry that matched nothing fell through, the column kept its upstream name,
//! and the run exited 0 — so a block written to the documented contract renamed
//! nothing, silently. These tests pin the replacement: the block is an ordered
//! sequence with the output name on the left, and every way of writing one that
//! cannot mean what it says is a spanned compile-time diagnostic.

use clinker_core_types::Diagnostic;

use crate::config::{CompileContext, parse_config};

/// A source declaring `first_name` / `last_name` / `department`, feeding one
/// CSV output whose `config:` carries the spliced-in lines.
fn pipeline(output_extra: &str) -> String {
    format!(
        r#"
pipeline:
  name: output_mapping_gate
nodes:
  - type: source
    name: people
    config:
      name: people
      type: csv
      path: ./people.csv
      schema:
        - {{ name: first_name, type: string }}
        - {{ name: last_name, type: string }}
        - {{ name: department, type: string }}
  - type: output
    name: out
    input: people
    config:
      name: out
      type: csv
      path: out.csv
{output_extra}"#
    )
}

fn compile_err(yaml: &str) -> Vec<Diagnostic> {
    let config = parse_config(yaml).expect("pipeline parses");
    config
        .compile(&CompileContext::default())
        .expect_err("compile must fail")
}

fn only(diags: &[Diagnostic], code: &str) -> Diagnostic {
    let matching: Vec<&Diagnostic> = diags.iter().filter(|d| d.code == code).collect();
    assert_eq!(
        matching.len(),
        1,
        "expected exactly one {code}, got {:?}",
        diags.iter().map(|d| &d.code).collect::<Vec<_>>()
    );
    matching[0].clone()
}

/// The migration diagnostic. An author arriving from the map form gets a code,
/// a span, and their own block already rewritten.
#[test]
fn map_form_is_rejected_with_the_authors_own_block_rewritten() {
    let diags = compile_err(&pipeline(
        "      mapping:\n        first_name: first_name\n        surname: last_name\n",
    ));
    let d = only(&diags, "E364");
    assert!(
        d.message.contains("not a map"),
        "message must name the rule broken: {}",
        d.message
    );
    let help = d.help.as_deref().unwrap_or_default();
    assert!(
        help.contains("- first_name\n") && help.contains("- surname: last_name"),
        "help must echo the block in the sequence form, identity entries collapsed \
         to a bare name: {help}"
    );
    assert!(
        help.contains("OUTPUT name is on the left"),
        "help must state the pair direction, since the map form was documented one way \
         round and implemented the other: {help}"
    );
}

/// A YAML map gave output-name uniqueness for free. The sequence has to enforce
/// it: two columns cannot share one CSV header.
#[test]
fn duplicate_output_name_is_rejected() {
    let diags = compile_err(&pipeline(
        "      mapping:\n        - department\n        - department: last_name\n",
    ));
    let d = only(&diags, "E364");
    assert!(d.message.contains("'department'"), "{}", d.message);
    assert!(d.message.contains("more than once"), "{}", d.message);
}

/// `exclude:` runs before `mapping:` reads the column, so the entry could only
/// ever produce nothing — the same silent-no-op shape this change exists to
/// remove.
#[test]
fn a_mapping_entry_the_same_output_excludes_is_rejected() {
    let diags = compile_err(&pipeline(
        "      exclude: [last_name]\n      mapping:\n        - surname: last_name\n",
    ));
    let d = only(&diags, "E364");
    assert!(d.message.contains("'last_name'"), "{}", d.message);
    assert!(d.message.contains("exclude"), "{}", d.message);
}

/// The headline defect: a column name that matches nothing. It used to rename
/// nothing and exit 0.
#[test]
fn unknown_source_column_is_rejected_with_a_suggestion() {
    let diags = compile_err(&pipeline(
        "      mapping:\n        - given_name: firstname\n",
    ));
    let d = only(&diags, "E365");
    assert!(
        d.message.contains("'firstname'"),
        "message must name the offending column: {}",
        d.message
    );
    let help = d.help.as_deref().unwrap_or_default();
    assert!(
        help.contains("did you mean `- <output_name>: first_name`"),
        "help must offer the near miss in the corrected shape: {help}"
    );
    assert!(
        help.contains("'department'"),
        "help must list the columns that are available: {help}"
    );
}

/// The direction contract, seen from the diagnostic: naming the OUTPUT column
/// on the right of the pair is rejected, because the right-hand side is the
/// column being read.
#[test]
fn naming_the_output_column_on_the_right_is_rejected() {
    let diags = compile_err(&pipeline("      mapping:\n        - last_name: surname\n"));
    let d = only(&diags, "E365");
    assert!(
        d.message.contains("'surname'"),
        "the RIGHT-hand name is the one that must exist upstream: {}",
        d.message
    );
}

/// No near miss: the help falls back to the available-columns list plus the way
/// to make an auto-widened column nameable.
#[test]
fn unknown_column_with_no_near_miss_names_the_auto_widen_route() {
    let diags = compile_err(&pipeline(
        "      mapping:\n        - x: totally_unrelated\n",
    ));
    let d = only(&diags, "E365");
    let help = d.help.as_deref().unwrap_or_default();
    assert!(help.contains("auto_widen"), "{help}");
    assert!(help.contains("'first_name'"), "{help}");
}

/// Over-rejection guard: a well-formed block against real columns compiles.
#[test]
fn a_well_formed_mapping_compiles() {
    let yaml = pipeline(
        "      include_unmapped: false\n      mapping:\n        - department\n        \
         - surname: last_name\n        - first_name\n",
    );
    let config = parse_config(&yaml).expect("pipeline parses");
    config
        .compile(&CompileContext::default())
        .expect("a mapping over declared columns must compile");
}

/// Over-rejection guard: one upstream column feeding two output columns is
/// legal — uniqueness is a constraint on the output side only.
#[test]
fn one_source_column_feeding_two_output_columns_compiles() {
    let yaml = pipeline("      mapping:\n        - department\n        - dept: department\n");
    let config = parse_config(&yaml).expect("pipeline parses");
    config
        .compile(&CompileContext::default())
        .expect("two output columns may read one source column");
}

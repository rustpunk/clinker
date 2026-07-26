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
///
/// `on_unmapped: drop` is deliberate. The engine-wide default is `auto_widen`,
/// which reserves the `$widened` sidecar and — under `include_unmapped: true` —
/// can legitimately supply a column the declared schema does not name, so E365
/// stands down there (see [`auto_widen_under_include_unmapped_suppresses_e365`]).
/// These fixtures want the gate live, so they use a source policy that cannot
/// carry an undeclared column.
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
      on_unmapped:
        mode: drop
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
    // The pairs come back SWAPPED. The engine looked map entries up by the
    // incoming field name, so `surname: last_name` renamed the SOURCE column
    // `surname` to `last_name`; the sequence form spells that
    // `- last_name: surname`. Emitting `- surname: last_name` would hand the
    // author a block that inverts the rename their pipeline was performing.
    assert!(
        help.contains("- first_name\n") && help.contains("- last_name: surname"),
        "help must rewrite the block into the sequence form with each pair swapped \
         into the new direction, identity entries collapsed to a bare name: {help}"
    );
    assert!(
        !help.contains("- surname: last_name"),
        "the unswapped pair would invert the rename the pipeline was performing: {help}"
    );
    assert!(
        help.contains("OUTPUT name is on the left"),
        "help must state the pair direction, since the map form was documented one way \
         round and implemented the other: {help}"
    );
    assert!(
        help.contains("SWAPPED") && help.contains("old documentation"),
        "help must say the swap is deliberate and name the one block it is wrong for — \
         one written to the old documentation, which renamed nothing: {help}"
    );
}

#[test]
fn map_form_rewrite_quotes_names_that_are_not_plain_yaml_scalars() {
    let diags = compile_err(&pipeline(
        "      mapping:\n        \"customer: id\": \"sold to\"\n",
    ));
    let d = only(&diags, "E364");
    let help = d.help.as_deref().unwrap_or_default();
    assert!(
        help.contains(r#"- "sold to": "customer: id""#),
        "the swapped rewrite must remain pasteable YAML: {help}"
    );
}

/// An empty block passes every content-based check — no legacy pairs, no
/// duplicates, no exclude clash — but under `include_unmapped: false` it states
/// that the file carries no columns at all, which is a header line and one blank
/// row per record. Both spellings are rejected.
#[test]
fn an_empty_mapping_is_rejected() {
    for block in ["      mapping: {}\n", "      mapping: []\n"] {
        let diags = compile_err(&pipeline(block));
        let d = only(&diags, "E364");
        assert!(
            d.message.contains("mapping:"),
            "message must name the block: {}",
            d.message
        );
        let help = d.help.as_deref().unwrap_or_default();
        assert!(
            help.contains("remove the `mapping:` block") || help.contains("list the columns"),
            "help must say what to write instead: {help}"
        );
    }
}

/// A YAML map gave output-name uniqueness for free. The sequence has to enforce
/// it: two columns cannot share one CSV header.
#[test]
fn duplicate_output_name_is_rejected() {
    let yaml = pipeline("      mapping:\n        - department\n        - department: last_name\n");
    let duplicate_line = yaml
        .lines()
        .position(|line| line.contains("- department: last_name"))
        .map(|index| index as u32 + 1)
        .expect("duplicate item line");
    let diags = compile_err(&yaml);
    let d = only(&diags, "E364");
    assert!(d.message.contains("'department'"), "{}", d.message);
    assert!(d.message.contains("more than once"), "{}", d.message);
    assert_eq!(
        d.primary.span.synthetic_line_number(),
        Some(duplicate_line),
        "the duplicate item, not the Output node, is highlighted"
    );
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

/// `exclude:` operates on INCOMING column names, so naming one the mapping also
/// produces as an output name is not a fault — it removes the upstream column of
/// that name and leaves the mapped one standing. That is precisely the fix the
/// collision diagnostic (E364) hands the author, so rejecting it here would make
/// one diagnostic reject the form another prescribes.
#[test]
fn excluding_a_mapping_output_name_compiles() {
    let yaml = pipeline("      exclude: [surname]\n      mapping:\n        - surname: last_name\n");
    let config = parse_config(&yaml).expect("pipeline parses");
    config
        .compile(&CompileContext::default())
        .expect("`exclude:` naming a produced output name is the documented collision fix");
}

/// End to end on the collision advice: an upstream column named the same as a
/// mapped output, resolved by excluding the upstream one, compiles clean.
#[test]
fn the_collision_diagnostics_own_advice_compiles() {
    let yaml = pipeline(
        "      include_unmapped: true\n      exclude: [department]\n      mapping:\n        \
         - department: first_name\n",
    );
    let config = parse_config(&yaml).expect("pipeline parses");
    config
        .compile(&CompileContext::default())
        .expect("excluding the colliding upstream column must resolve the collision");
}

/// The headline defect: a column name that matches nothing. It used to rename
/// nothing and exit 0.
#[test]
fn unknown_source_column_is_rejected_with_a_suggestion() {
    let yaml = pipeline("      mapping:\n        - given_name: firstname\n");
    let item_line = yaml
        .lines()
        .position(|line| line.contains("- given_name: firstname"))
        .map(|index| index as u32 + 1)
        .expect("mapping item line");
    let diags = compile_err(&yaml);
    let d = only(&diags, "E365");
    assert!(
        d.message.contains("'firstname'"),
        "message must name the offending column: {}",
        d.message
    );
    let help = d.help.as_deref().unwrap_or_default();
    assert!(
        help.contains("did you mean `- given_name: first_name`"),
        "help must preserve the authored output name and offer the corrected source: {help}"
    );
    assert!(
        help.contains("'department'"),
        "help must list the columns that are available: {help}"
    );
    assert_eq!(
        d.primary.span.synthetic_line_number(),
        Some(item_line),
        "the unmatched item, not the Output node, is highlighted"
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

/// A renamed column landing on a name `include_unmapped` also carries through
/// would put two same-named columns on one schema. The schema's name index is
/// last-write-wins, so the passthrough column would answer for the renamed one
/// and serve its value under the renamed header — the rename silently lost.
#[test]
fn an_output_name_colliding_with_a_passthrough_column_is_rejected() {
    let diags = compile_err(&pipeline(
        "      include_unmapped: true\n      mapping:\n        - department: first_name\n",
    ));
    let d = only(&diags, "E364");
    assert!(d.message.contains("'department'"), "{}", d.message);
    assert!(
        d.message.contains("two"),
        "message must say the file would carry the column twice: {}",
        d.message
    );
    let help = d.help.as_deref().unwrap_or_default();
    assert!(
        help.contains("exclude:") && help.contains("include_unmapped: false"),
        "help must offer both ways out: {help}"
    );
}

/// The same collision is fine under `include_unmapped: false` — nothing is
/// appended, so nothing can collide.
#[test]
fn an_output_name_matching_an_unlisted_column_is_fine_when_nothing_is_appended() {
    let yaml = pipeline(
        "      include_unmapped: false\n      mapping:\n        - department: first_name\n",
    );
    let config = parse_config(&yaml).expect("pipeline parses");
    config
        .compile(&CompileContext::default())
        .expect("with no passthrough columns there is nothing to collide with");
}

/// An `auto_widen` source can carry a column its declared schema does not name,
/// and `include_unmapped: true` expands the sidecar into the field map BEFORE
/// the mapping reads it — so the column is genuinely reachable and E365 must
/// stand down. Rejecting it would break a pipeline that worked, and the only
/// remedy the diagnostic could offer (declare the column) defeats auto-widen.
#[test]
fn auto_widen_under_include_unmapped_suppresses_e365() {
    let yaml = r#"
pipeline:
  name: output_mapping_widen
nodes:
  - type: source
    name: people
    config:
      name: people
      type: csv
      path: ./people.csv
      schema:
        - { name: first_name, type: string }
  - type: output
    name: out
    input: people
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
      mapping:
        - nickname: drifted_column
"#;
    let config = parse_config(yaml).expect("pipeline parses");
    config
        .compile(&CompileContext::default())
        .expect("a sidecar-reserving row may supply an undeclared column");
}

/// Similarity is not proof of absence: a real drift column may differ from a
/// declared column only by punctuation. The runtime W365 report, which sees
/// delivered records, owns the unresolved case.
#[test]
fn auto_widen_near_match_still_suppresses_e365() {
    let yaml = r#"
pipeline:
  name: output_mapping_widen_near_match
nodes:
  - type: source
    name: people
    config:
      name: people
      type: csv
      path: ./people.csv
      schema:
        - { name: first_name, type: string }
  - type: output
    name: out
    input: people
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
      mapping:
        - nickname: firstname
"#;
    let config = parse_config(yaml).expect("pipeline parses");
    config
        .compile(&CompileContext::default())
        .expect("a similarly named sidecar column may be real drift");
}

/// The relaxation is conditioned on expansion actually happening. Under
/// `include_unmapped: false` the sidecar stays packed, so the same mapping
/// cannot resolve and E365 still applies.
#[test]
fn auto_widen_without_include_unmapped_still_fires_e365() {
    let yaml = r#"
pipeline:
  name: output_mapping_widen_packed
nodes:
  - type: source
    name: people
    config:
      name: people
      type: csv
      path: ./people.csv
      schema:
        - { name: first_name, type: string }
  - type: output
    name: out
    input: people
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: false
      mapping:
        - nickname: drifted_column
"#;
    let diags = compile_err(yaml);
    let d = only(&diags, "E365");
    assert!(d.message.contains("'drifted_column'"), "{}", d.message);
    let help = d.help.as_deref().unwrap_or_default();
    assert!(
        help.contains("include_unmapped: true"),
        "help must name the flag that would make the sidecar column reachable: {help}"
    );
}

//! Two destinations that name one physical file must be refused before the
//! run starts, whatever spelling each one uses.
//!
//! `out/data.csv` and `out/pending/../data.csv` are one file once the run
//! creates `pending` on its way there, and the identity every collision check
//! keys on now says so (`config::fs_type`'s
//! `a_dot_dot_past_the_existing_prefix_is_cancelled`). At *this* surface the
//! second spelling never reaches that check: a `..` in an authored path is a
//! traversal, and traversal is refused outright. This test pins which of the
//! two rules answers first, so a later relaxation of the traversal rule is
//! seen for what it is -- a change that hands these paths to the collision
//! check instead.

use clinker_plan::config::{CompileContext, parse_config};

fn compile_errors(yaml: &str) -> String {
    let config = parse_config(yaml).expect("the pipeline parses");
    let diags = config
        .compile(&CompileContext::default())
        .expect_err("a traversal in an output path must be refused");
    diags
        .iter()
        .map(|d| format!("{}: {}", d.code, d.message))
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn a_dot_dot_spelling_of_an_admitted_destination_is_refused() {
    let combined = compile_errors(
        r#"
pipeline:
  name: dot_dot_destination
nodes:
  - type: source
    name: src_a
    config:
      name: src_a
      type: csv
      path: a.csv
      schema:
        - { name: id, type: int }
  - type: sink
    name: direct
    input: src_a
    config:
      name: direct
      type: csv
      path: out/data.csv
  - type: sink
    name: through_pending
    input: src_a
    config:
      name: through_pending
      type: csv
      path: out/pending/../data.csv
"#,
    );
    assert!(
        combined.contains("E-SEC-001") && combined.contains("out/pending/../data.csv"),
        "expected the traversal to be named and refused, got:\n{combined}"
    );
}

//! Cull and Reshape predicate programs travel on the plan node.
//!
//! `bind_schema` + lowering compile each operator's CXL once and stamp it
//! onto the runtime node — Cull's OR-combined `drop_group_when` decision
//! aggregate onto `PlanNode::Cull.compiled` / `.typed`, and Reshape's
//! per-rule `when` / `set` / `overrides` programs onto
//! `PlanNode::Reshape.compiled_rules` — mirroring `Aggregation` / `Route`.
//! The executor reads these off the node and only rebuilds the cheap
//! per-run `ProgramEvaluator` at dispatch, instead of re-parsing and
//! re-typechecking the rule source every dispatch. These tests pin that the
//! on-node programs are populated after a successful compile.

use crate::config::pipeline_node::CULL_DROP_DECISION_COLUMN;
use crate::config::{CompileContext, parse_config};
use crate::plan::execution::PlanNode;

/// Compile `yaml` to a `CompiledPlan`, panicking on any diagnostic.
fn compile_ok(yaml: &str) -> crate::plan::CompiledPlan {
    parse_config(yaml)
        .expect("parse_config")
        .compile(&CompileContext::default())
        .expect("compile should succeed")
}

/// A multi-rule Cull carries its OR-combined `drop_group_when` decision
/// aggregate on the node: the typed program emits the reserved decision
/// column and the extracted aggregate groups by the cull's `partition_by`.
#[test]
fn cull_decision_program_travels_on_node() {
    let yaml = r#"
pipeline:
  name: cull_on_node
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: id, type: string }
        - { name: amount, type: int }
        - { name: status, type: string }
  - type: cull
    name: cd
    input: src
    config:
      partition_by: [id]
      removed_to: removed
      rules:
        - name: drop_errors
          drop_group_when: "sum(if status == 'error' then 1 else 0) > 0"
        - name: drop_big
          drop_group_when: "sum(amount) > 100"
  - type: output
    name: out
    input: cd
    config:
      name: out
      type: csv
      path: out.csv
  - type: output
    name: audit
    input: cd.removed
    config:
      name: audit
      type: csv
      path: audit.csv
"#;
    let compiled = compile_ok(yaml);
    let g = &compiled.dag().graph;
    let idx = g
        .node_indices()
        .find(|&i| g[i].name() == "cd")
        .expect("cull node `cd` present");
    let PlanNode::Cull {
        typed,
        compiled: agg,
        ..
    } = &g[idx]
    else {
        panic!("node `cd` is not a Cull");
    };
    // The on-node typed program is the OR-combined decision program: it
    // carries statements, and the extracted aggregate emits the reserved
    // decision column the executor reads back per group.
    assert!(
        !typed.program.statements.is_empty(),
        "cull decision program should carry statements"
    );
    let emit_names: Vec<&str> = agg.emits.iter().map(|e| e.output_name.as_ref()).collect();
    assert!(
        emit_names.contains(&CULL_DROP_DECISION_COLUMN),
        "cull decision aggregate should emit {CULL_DROP_DECISION_COLUMN:?}, got {emit_names:?}"
    );
    // The extracted aggregate groups by the cull's partition key and folds
    // the predicate's aggregate functions (both rules use `sum`).
    assert_eq!(
        agg.group_by_fields,
        vec!["id".to_string()],
        "cull decision aggregate should group by partition_by"
    );
    assert!(
        !agg.bindings.is_empty(),
        "cull decision aggregate should carry the predicate's aggregate bindings"
    );
}

/// A multi-rule Cull's decision program stamps the reconstructed one-line
/// decision source, and each disjunct's spans index that exact string:
/// slicing the stamped source at the OR body's `lhs` / `rhs` spans recovers
/// each rule's `drop_group_when` verbatim. This keeps a runtime eval-error
/// caret — whose byte offsets are resolved against the stamped source — over
/// the rule text that actually failed, rather than an off-by-prefix slice.
#[test]
fn cull_decision_disjunct_spans_index_the_stamped_source() {
    // These two literals must match the `drop_group_when` values in the YAML
    // below exactly; the test asserts each disjunct's span slices back to them.
    let rule0_src = "sum(if status == 'error' then 1 else 0) > 0";
    let rule1_src = "sum(amount) > 100";
    let yaml = r#"
pipeline:
  name: cull_spans
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: id, type: string }
        - { name: amount, type: int }
        - { name: status, type: string }
  - type: cull
    name: cd
    input: src
    config:
      partition_by: [id]
      removed_to: removed
      rules:
        - name: drop_errors
          drop_group_when: "sum(if status == 'error' then 1 else 0) > 0"
        - name: drop_big
          drop_group_when: "sum(amount) > 100"
  - type: output
    name: out
    input: cd
    config:
      name: out
      type: csv
      path: out.csv
  - type: output
    name: audit
    input: cd.removed
    config:
      name: audit
      type: csv
      path: audit.csv
"#;
    let compiled = compile_ok(yaml);
    let g = &compiled.dag().graph;
    let idx = g
        .node_indices()
        .find(|&i| g[i].name() == "cd")
        .expect("cull node `cd` present");
    let PlanNode::Cull { typed, .. } = &g[idx] else {
        panic!("node `cd` is not a Cull");
    };

    // The decision program stamps the reconstructed one-line source.
    let src = typed
        .source
        .as_deref()
        .expect("cull decision program stamps its display source");
    let expected = format!("emit {CULL_DROP_DECISION_COLUMN} = ({rule0_src}) or ({rule1_src})");
    assert_eq!(
        src, expected,
        "decision source is the parenthesized OR of the rule predicates"
    );

    // The single emit's body is the OR of the two rule predicates.
    let stmt = typed
        .program
        .statements
        .first()
        .expect("decision program carries one emit");
    let cxl::ast::Statement::Emit { expr, .. } = stmt else {
        panic!("decision program statement is not an emit");
    };
    let cxl::ast::Expr::Binary {
        op: cxl::ast::BinOp::Or,
        lhs,
        rhs,
        ..
    } = expr
    else {
        panic!("two-rule decision body is not an OR");
    };

    // Each disjunct's top-level span slices the stamped source back to the
    // exact rule predicate it was parsed from.
    let slice = |e: &cxl::ast::Expr| -> &str {
        let s = e.span();
        &src[s.start as usize..s.end as usize]
    };
    assert_eq!(
        slice(lhs),
        rule0_src,
        "left disjunct span must slice the stamped source to rule 0"
    );
    assert_eq!(
        slice(rhs),
        rule1_src,
        "right disjunct span must slice the stamped source to rule 1"
    );
}

/// A `#` line-comment in a Cull rule predicate compiles: each rule is parsed
/// independently, so a trailing comment terminates at its own end-of-line
/// instead of swallowing the following disjuncts, and the parsed predicates
/// are OR-combined at the AST level. The commented rule and its uncommented
/// neighbour both fold into the decision aggregate.
#[test]
fn cull_rule_with_line_comment_compiles() {
    let yaml = r#"
pipeline:
  name: cull_comment
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: id, type: string }
        - { name: amount, type: int }
        - { name: status, type: string }
  - type: cull
    name: cd
    input: src
    config:
      partition_by: [id]
      removed_to: removed
      rules:
        - name: drop_errors
          drop_group_when: "sum(if status == 'error' then 1 else 0) > 0  # any error in group"
        - name: drop_big
          drop_group_when: "sum(amount) > 100"
  - type: output
    name: out
    input: cd
    config:
      name: out
      type: csv
      path: out.csv
  - type: output
    name: audit
    input: cd.removed
    config:
      name: audit
      type: csv
      path: audit.csv
"#;
    let compiled = compile_ok(yaml);
    let g = &compiled.dag().graph;
    let idx = g
        .node_indices()
        .find(|&i| g[i].name() == "cd")
        .expect("cull node `cd` present");
    let PlanNode::Cull {
        typed,
        compiled: agg,
        ..
    } = &g[idx]
    else {
        panic!("node `cd` is not a Cull");
    };
    assert!(
        !typed.program.statements.is_empty(),
        "cull decision program should carry statements"
    );
    let emit_names: Vec<&str> = agg.emits.iter().map(|e| e.output_name.as_ref()).collect();
    assert!(
        emit_names.contains(&CULL_DROP_DECISION_COLUMN),
        "cull decision aggregate should emit {CULL_DROP_DECISION_COLUMN:?}, got {emit_names:?}"
    );
    // Both rules contribute a `sum` aggregate to the OR-combined decision, so
    // the extracted aggregate carries at least two bindings.
    assert!(
        agg.bindings.len() >= 2,
        "both rules' aggregates should fold into the decision, got {} bindings",
        agg.bindings.len()
    );
}

/// An error inside a `#`-commented rule is still attributed to that rule by
/// name: the OR-combined program fails, then the per-rule attribution
/// fallback re-typechecks each rule independently (comment-tolerant), so the
/// offending rule surfaces by name even though its predicate carries a
/// trailing comment. The uncommented sibling rule is well-formed, so only
/// the bad rule is named.
#[test]
fn cull_error_in_commented_rule_names_the_rule() {
    let yaml = r#"
pipeline:
  name: cull_comment_error
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: id, type: string }
        - { name: amount, type: int }
        - { name: status, type: string }
  - type: cull
    name: cd
    input: src
    config:
      partition_by: [id]
      removed_to: removed
      rules:
        - name: bad_rule
          drop_group_when: "sum(nonexistent) > 0  # references a missing column"
        - name: drop_big
          drop_group_when: "sum(amount) > 100"
  - type: output
    name: out
    input: cd
    config:
      name: out
      type: csv
      path: out.csv
  - type: output
    name: audit
    input: cd.removed
    config:
      name: audit
      type: csv
      path: audit.csv
"#;
    let diags = parse_config(yaml)
        .expect("parse_config")
        .compile(&CompileContext::default())
        .expect_err("a predicate referencing an unknown column must fail to compile");
    // The diagnostic names both the offending rule (only the per-rule
    // attribution path, which is comment-tolerant, adds the rule name) and
    // the unknown column. An unknown column is a CXL name-resolution
    // failure, so the code is E203.
    assert!(
        diags.iter().any(|d| d.code == "E203"
            && d.message.contains("bad_rule")
            && d.message.contains("nonexistent")),
        "expected an E203 naming rule `bad_rule` and the unknown column, got {:?}",
        diags
            .iter()
            .map(|d| (d.code.clone(), d.message.clone()))
            .collect::<Vec<_>>()
    );
    // The well-formed sibling rule is not blamed.
    assert!(
        !diags
            .iter()
            .any(|d| d.message.contains("drop_big") && d.message.contains("nonexistent")),
        "the well-formed rule `drop_big` must not be blamed, got {:?}",
        diags
            .iter()
            .map(|d| (d.code.clone(), d.message.clone()))
            .collect::<Vec<_>>()
    );
}

/// A Reshape carries one `CompiledReshapeRule` per config rule, each with
/// its `when` / `set` / `overrides` typed programs populated.
#[test]
fn reshape_rule_programs_travel_on_node() {
    let yaml = r#"
pipeline:
  name: reshape_on_node
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: id, type: string }
        - { name: amount, type: int }
        - { name: label, type: string }
  - type: reshape
    name: rs
    input: src
    config:
      partition_by: [id]
      rules:
        - name: bump
          when: "amount > 0"
          mutate:
            set:
              amount: "amount + 1"
        - name: synth
          when: "label == 'seed'"
          synthesize:
            copy_from: trigger
            overrides:
              label: "'synthesized'"
  - type: output
    name: out
    input: rs
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let compiled = compile_ok(yaml);
    let g = &compiled.dag().graph;
    let idx = g
        .node_indices()
        .find(|&i| g[i].name() == "rs")
        .expect("reshape node `rs` present");
    let PlanNode::Reshape {
        compiled_rules,
        config,
        ..
    } = &g[idx]
    else {
        panic!("node `rs` is not a Reshape");
    };
    // One compiled rule per config rule, in declaration order.
    assert_eq!(
        compiled_rules.len(),
        config.rules.len(),
        "one CompiledReshapeRule per config rule"
    );
    assert_eq!(compiled_rules.len(), 2);

    // Rule 0 mutates `amount`: a `when` program plus one `set` entry, no synth.
    let bump = &compiled_rules[0];
    assert_eq!(bump.name, "bump");
    assert!(
        !bump.when.program.statements.is_empty(),
        "`when` program should carry statements"
    );
    assert_eq!(bump.set.len(), 1);
    assert_eq!(bump.set[0].0, "amount");
    assert!(bump.synth.is_none(), "mutate-only rule has no synth");

    // Rule 1 synthesizes: a `synth` block with one override on `label`.
    let synth_rule = &compiled_rules[1];
    assert_eq!(synth_rule.name, "synth");
    assert!(
        synth_rule.set.is_empty(),
        "synth-only rule has no mutate set"
    );
    let synth = synth_rule
        .synth
        .as_ref()
        .expect("synthesize rule carries a synth block");
    assert_eq!(synth.overrides.len(), 1);
    assert_eq!(synth.overrides[0].0, "label");
    assert!(
        !synth.overrides[0].1.program.statements.is_empty(),
        "override program should carry statements"
    );
}

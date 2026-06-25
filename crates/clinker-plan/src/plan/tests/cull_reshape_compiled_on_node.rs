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

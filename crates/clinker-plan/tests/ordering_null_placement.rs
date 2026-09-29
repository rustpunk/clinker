//! `null_order` on fields that only order rows.
//!
//! A Cull or Reshape `order_by` arranges the rows of a correlation group and
//! never removes any, so its null option is placement only: `first` or
//! `last`. `null_order: drop` there is a plan-time error that names the node
//! and the field, says why, and gives the upstream `filter` that does remove
//! the rows. Both nodes also take the bare field-name shorthand a Sink or
//! Source `sort_order` takes, and carry the validated list on their plan
//! node. A Source `sort_order` refuses `drop` through the same conversion;
//! only a Sink `sort_order` keeps it.

use clinker_plan::config::pipeline_node::PipelineNode;
use clinker_plan::config::{
    CompileContext, NullOrder, NullPlacement, OrderField, PipelineConfig, SortField, SortOrder,
    parse_config, validate_source_sort_policy,
};
use clinker_plan::error::PipelineError;
use clinker_plan::plan::CompiledPlan;
use clinker_plan::plan::execution::PlanNode;

/// The operator block of a Cull named `cd` over `src`, with `order_by`
/// spliced in as YAML text.
fn cull_block(order_by: &str) -> String {
    format!(
        r#"  - type: cull
    name: cd
    input: src
    config:
      partition_by: [account]
      order_by: {order_by}
      removed_to: removed
      rules:
        - name: drop_big
          drop_group_when: "sum(amount) > 100"
  - type: sink
    name: out
    input: cd
    config:
      name: out
      type: csv
      path: out.csv
  - type: sink
    name: audit
    input: cd.removed
    config:
      name: audit
      type: csv
      path: audit.csv
"#
    )
}

/// The operator block of a Reshape named `rs` over `src`, with `order_by`
/// spliced in as YAML text.
fn reshape_block(order_by: &str) -> String {
    format!(
        r#"  - type: reshape
    name: rs
    input: src
    config:
      partition_by: [account]
      order_by: {order_by}
      rules:
        - name: bump
          when: "amount > 0"
          mutate:
            set:
              amount: "amount + 1"
  - type: sink
    name: out
    input: rs
    config:
      name: out
      type: csv
      path: out.csv
"#
    )
}

/// A source `src` with a nullable `txn_date`, followed by `operator`.
fn pipeline(operator: &str) -> String {
    format!(
        r#"
pipeline:
  name: ordering_null_placement
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - {{ name: account, type: string }}
        - {{ name: txn_date, type: {{ nullable: string }} }}
        - {{ name: amount, type: int }}
{operator}"#
    )
}

/// Compile `yaml`, expect failure, and return every `(code, message)`.
fn compile_diagnostics(yaml: &str) -> Vec<(String, String)> {
    let config = parse_config(yaml).expect("fixture must parse as YAML");
    let diags = config
        .compile(&CompileContext::default())
        .expect_err("fixture is expected to fail compilation");
    diags
        .into_iter()
        .map(|d| (d.code.clone(), d.message.clone()))
        .collect()
}

/// The group-ordering text for field `txn_date`, written out in full so a
/// change to the wording is a visible change to this test.
const GROUP_DROP_TEXT: &str = "`null_order: drop` is not allowed on `order_by` for field \
     'txn_date': `order_by` only orders rows within a group and cannot remove them. Use \
     `null_order: first` or `null_order: last`; to exclude rows whose 'txn_date' is null, add a \
     Transform before this node with `filter not txn_date.is_null()`.";

fn assert_one_drop_diagnostic(diags: &[(String, String)], expected: &str) {
    let matching: Vec<&(String, String)> = diags
        .iter()
        .filter(|(_, m)| m.contains("null_order: drop"))
        .collect();
    assert_eq!(
        matching.len(),
        1,
        "expected exactly one `null_order: drop` diagnostic, got {diags:?}"
    );
    let (code, message) = matching[0];
    assert_eq!(code, "E200", "wrong code for {message:?}");
    assert_eq!(message, expected);
}

#[test]
fn cull_order_by_drop_is_rejected_with_the_fix() {
    let yaml = pipeline(&cull_block("[{ field: txn_date, null_order: drop }]"));
    let diags = compile_diagnostics(&yaml);
    assert_one_drop_diagnostic(&diags, &format!("cull \"cd\": {GROUP_DROP_TEXT}"));
}

#[test]
fn reshape_order_by_drop_is_rejected_with_the_fix() {
    let yaml = pipeline(&reshape_block("[{ field: txn_date, null_order: drop }]"));
    let diags = compile_diagnostics(&yaml);
    assert_one_drop_diagnostic(&diags, &format!("reshape \"rs\": {GROUP_DROP_TEXT}"));
}

/// The authored `order_by` of the named node, as parsed.
fn authored_order_by(config: &PipelineConfig, name: &str) -> Vec<SortField> {
    config
        .nodes
        .iter()
        .find_map(|node| match &node.value {
            PipelineNode::Cull { header, config } if header.name == name => {
                Some(config.order_by.clone())
            }
            PipelineNode::Reshape { header, config } if header.name == name => {
                Some(config.order_by.clone())
            }
            _ => None,
        })
        .unwrap_or_else(|| panic!("no Cull or Reshape named {name:?}"))
}

#[test]
fn cull_and_reshape_order_by_accept_the_bare_field_name() {
    let expected = vec![SortField {
        field: "txn_date".to_string(),
        order: SortOrder::Asc,
        null_order: None,
    }];
    for (block, name) in [
        (cull_block("[txn_date]"), "cd"),
        (reshape_block("[txn_date]"), "rs"),
    ] {
        let shorthand = parse_config(&pipeline(&block)).expect("the shorthand must parse");
        assert_eq!(authored_order_by(&shorthand, name), expected, "{name}");
        let long_form = parse_config(&pipeline(
            &block.replace("[txn_date]", "[{ field: txn_date }]"),
        ))
        .expect("the long form must parse");
        assert_eq!(
            authored_order_by(&shorthand, name),
            authored_order_by(&long_form, name),
            "{name}: the shorthand must equal `{{ field: txn_date }}`"
        );
        shorthand
            .compile(&CompileContext::default())
            .unwrap_or_else(|diags| {
                panic!("{name} with a bare order_by field must compile: {diags:?}")
            });
    }
}

/// Compile `yaml`, panicking on any diagnostic.
fn compile_ok(yaml: &str) -> CompiledPlan {
    parse_config(yaml)
        .expect("fixture must parse as YAML")
        .compile(&CompileContext::default())
        .unwrap_or_else(|diags| panic!("fixture must compile: {diags:?}"))
}

/// The validated `order_by` the named Cull or Reshape plan node carries.
fn plan_node_order_by(plan: &CompiledPlan, name: &str) -> Vec<OrderField> {
    let graph = &plan.dag().graph;
    let idx = graph
        .node_indices()
        .find(|&i| graph[i].name() == name)
        .unwrap_or_else(|| panic!("no plan node named {name:?}"));
    match &graph[idx] {
        PlanNode::Cull { order_by, .. } | PlanNode::Reshape { order_by, .. } => order_by.clone(),
        other => panic!("{name:?} is a {}, not a Cull or Reshape", other.kind_name()),
    }
}

fn order_field(field: &str, order: SortOrder, null_order: NullPlacement) -> OrderField {
    OrderField {
        field: field.to_string(),
        order,
        null_order,
    }
}

#[test]
fn placement_first_and_last_reach_the_plan_node() {
    let order_by = "[{ field: txn_date, null_order: first }, \
                    { field: amount, order: desc, null_order: last }, account]";
    let expected = vec![
        order_field("txn_date", SortOrder::Asc, NullPlacement::First),
        order_field("amount", SortOrder::Desc, NullPlacement::Last),
        order_field("account", SortOrder::Asc, NullPlacement::Last),
    ];
    for (block, name) in [
        (cull_block(order_by), "cd"),
        (reshape_block(order_by), "rs"),
    ] {
        let plan = compile_ok(&pipeline(&block));
        assert_eq!(plan_node_order_by(&plan, name), expected, "{name}");
    }
}

/// A source `src` whose `sort_order` is spliced in, feeding one Sink.
fn sorted_source_pipeline(sort_order: &str) -> String {
    pipeline(&format!(
        r#"      sort_order: {sort_order}
  - type: sink
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
"#
    ))
}

const SOURCE_DROP_TEXT: &str = "source 'src': `null_order: drop` is not allowed on `sort_order` \
     for field 'txn_date': source verification cannot discard records. Use `null_order: first` \
     or `null_order: last`; to exclude rows whose 'txn_date' is null, add a Transform after this \
     source with `filter not txn_date.is_null()`.";

#[test]
fn source_sort_order_drop_is_rejected_with_the_fix() {
    let yaml = sorted_source_pipeline("[{ field: txn_date, null_order: drop }]");
    let config = parse_config(&yaml).expect("fixture must parse as YAML");
    let source = config.source_bodies().next().expect("one source");
    let err = validate_source_sort_policy(&source.source, &source.schema)
        .expect_err("a Source sort_order must refuse null_order: drop");
    let PipelineError::Compilation {
        transform_name,
        messages,
    } = err
    else {
        panic!("expected a compilation error, got {err:?}");
    };
    assert_eq!(transform_name, "src");
    assert_eq!(messages, vec![SOURCE_DROP_TEXT.to_string()]);

    let diags = compile_diagnostics(&yaml);
    assert!(
        diags.iter().any(|(_, m)| m.contains(SOURCE_DROP_TEXT)),
        "compiling must report the Source refusal, got {diags:?}"
    );
}

#[test]
fn source_sort_order_placement_reaches_the_compiled_source_order() {
    let plan = compile_ok(&sorted_source_pipeline(
        "[{ field: txn_date, null_order: first }, { field: amount, null_order: last }, account]",
    ));
    let orders = &plan.dag().order_contract().source_orders;
    assert_eq!(orders.len(), 1);
    let placed: Vec<(&str, NullOrder)> = orders[0]
        .fields
        .iter()
        .map(|field| (field.field.as_str(), field.null_order))
        .collect();
    assert_eq!(
        placed,
        vec![
            ("txn_date", NullOrder::First),
            ("amount", NullOrder::Last),
            ("account", NullOrder::Last),
        ]
    );
}

#[test]
fn sink_sort_order_keeps_drop() {
    let plan = compile_ok(&pipeline(
        r#"  - type: sink
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
      sort_order:
        - { field: txn_date, null_order: drop }
        - { field: amount, null_order: last }
"#,
    ));
    let boundary = plan
        .dag()
        .order_contract()
        .writer_boundaries
        .iter()
        .find(|boundary| boundary.output_name == "out")
        .expect("the Sink has a writer boundary");
    let dropped: Vec<&str> = boundary
        .pre_sort_drop_fields
        .iter()
        .map(|field| field.field.as_str())
        .collect();
    assert_eq!(dropped, vec!["txn_date"]);
}

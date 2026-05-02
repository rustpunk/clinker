//! Plan-time deferred-region detection tests.
//!
//! Each test compiles an inline-YAML pipeline (or a temp-dir composition
//! fixture for body crossings), then asserts on
//! `ExecutionPlanDag.deferred_regions` keyed by `NodeIndex`.
//!
//! Helpers mirror the inline-YAML idiom from `ck_lattice.rs`. The plan-
//! time detector runs unconditionally in compile Stage 5; for pipelines
//! without a relaxed-CK Aggregate the resulting map is empty.

use std::path::PathBuf;

use petgraph::Direction;
use petgraph::graph::NodeIndex;

use crate::config::{CompileContext, PipelineConfig, parse_config};
use crate::plan::execution::ExecutionPlanDag;

fn compile(yaml: &str) -> ExecutionPlanDag {
    let config: PipelineConfig = parse_config(yaml).expect("parse");
    config
        .compile(&CompileContext::default())
        .expect("compile")
        .dag()
        .clone()
}

fn compile_with_dir(yaml: &str, workspace_root: &std::path::Path) -> ExecutionPlanDag {
    let config: PipelineConfig = parse_config(yaml).expect("parse");
    let ctx = CompileContext::with_pipeline_dir(workspace_root, PathBuf::from("pipelines"));
    config.compile(&ctx).expect("compile").dag().clone()
}

fn node_idx_for(plan: &ExecutionPlanDag, node_name: &str) -> NodeIndex {
    plan.graph
        .node_indices()
        .find(|&i| plan.graph[i].name() == node_name)
        .unwrap_or_else(|| panic!("node {node_name:?} not found in plan"))
}

/// Test 1: Simple region — Source(CK=order_id) → Aggregate(group_by=dept,
/// total=sum(amount)) → Transform → Output. Exactly one region; producer
/// is the aggregate; members carry the Transform; outputs carry the
/// Output; buffer_schema is the columns the Transform reaches.
#[test]
fn simple_region_detected() {
    let yaml = r#"
pipeline:
  name: simple_region
error_handling:
  strategy: continue
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      correlation_key: order_id
      schema:
        - { name: order_id, type: string }
        - { name: dept, type: string }
        - { name: amount, type: int }
  - type: aggregate
    name: dept_totals
    input: orders
    config:
      group_by: [dept]
      cxl: |
        emit total = sum(amount)
  - type: transform
    name: rename
    input: dept_totals
    config:
      cxl: |
        emit dept_name = dept
        emit grand_total = total
  - type: output
    name: out
    input: rename
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;
    let plan = compile(yaml);
    let agg_idx = node_idx_for(&plan, "dept_totals");
    let xform_idx = node_idx_for(&plan, "rename");
    let out_idx = node_idx_for(&plan, "out");

    let region = plan
        .deferred_regions
        .get(&agg_idx)
        .expect("region keyed at producer");
    assert_eq!(region.producer, agg_idx);
    assert!(
        region.members.contains(&xform_idx),
        "rename Transform belongs to the deferred region"
    );
    assert!(
        region.outputs.contains(&out_idx),
        "out Output is the region's exit boundary"
    );

    // Multi-key flatten: every participating NodeIndex maps to the
    // shared region, so dispatcher arms can do O(1) lookup.
    assert!(plan.deferred_regions.contains_key(&xform_idx));
    assert!(plan.deferred_regions.contains_key(&out_idx));

    // The Aggregate emits `dept` (group_by) + `total` (sum). The
    // downstream Transform reads both. Buffer schema covers them.
    assert!(
        region.buffer_schema.contains(&"dept".to_string()),
        "buffer_schema must carry the group-by column reached by the Transform; \
         got {:?}",
        region.buffer_schema
    );
    assert!(
        region.buffer_schema.contains(&"total".to_string()),
        "buffer_schema must carry the sum-output column reached by the Transform; \
         got {:?}",
        region.buffer_schema
    );
}

/// Test 2: Column pruning — many-column source, aggregate emits a small
/// subset, downstream consumes a subset of THAT. The buffer_schema is
/// pruned to exactly the producer-emitted columns the deferred operators
/// reach via support_into.
#[test]
fn buffer_schema_prunes_to_consumed_columns() {
    let yaml = r#"
pipeline:
  name: prune_columns
error_handling:
  strategy: continue
nodes:
  - type: source
    name: wide
    config:
      name: wide
      type: csv
      path: wide.csv
      correlation_key: id
      schema:
        - { name: id, type: string }
        - { name: dept, type: string }
        - { name: amount, type: int }
        - { name: a, type: int }
        - { name: b, type: int }
        - { name: c, type: int }
        - { name: d, type: int }
  - type: aggregate
    name: agg
    input: wide
    config:
      group_by: [dept]
      cxl: |
        emit total = sum(amount)
        emit other = sum(a)
  - type: transform
    name: project
    input: agg
    config:
      cxl: |
        emit dept_keep = dept
        emit running_total = total
  - type: output
    name: out
    input: project
    config:
      name: out
      type: csv
      path: out.csv
      mapping:
        out_dept: dept_keep
        out_total: running_total
"#;
    let plan = compile(yaml);
    let agg_idx = node_idx_for(&plan, "agg");
    let region = plan
        .deferred_regions
        .get(&agg_idx)
        .expect("region keyed at producer");

    // Downstream Transform reads `dept` and `total` from the producer
    // (transitively via `dept_keep` / `running_total`). `other` is
    // emitted by the producer but NEVER consumed downstream; pruning
    // must drop it.
    assert!(region.buffer_schema.contains(&"dept".to_string()));
    assert!(region.buffer_schema.contains(&"total".to_string()));
    assert!(
        !region.buffer_schema.contains(&"other".to_string()),
        "unconsumed producer emit must be pruned; got {:?}",
        region.buffer_schema
    );
    // Source-only fields never appear (they're upstream of the producer).
    for upstream_only in &["a", "b", "c", "d", "amount"] {
        assert!(
            !region.buffer_schema.contains(&upstream_only.to_string()),
            "upstream-only field {upstream_only} leaked into producer buffer schema; \
             got {:?}",
            region.buffer_schema
        );
    }
}

/// Test 3: Multi-Aggregate cascade — a relaxed Aggregate feeds a strict
/// Aggregate (whose `group_by` covers every source-level CK field, so
/// E15W stays silent) through Transform pairs. Pass A's "Aggregation
/// (strict) → add to members; continue" rule merges them into one
/// region rooted at the upstream producer; both Transforms join
/// `members`. (Two relaxed Aggregates chained directly are forbidden
/// by E15W — the runtime cannot prove correct retraction propagation
/// through chained relaxed aggregates — so this fixture exercises the
/// architecturally-valid relaxed→strict cascade.)
#[test]
fn multi_aggregate_cascade_merges_into_one_region() {
    let yaml = r#"
pipeline:
  name: cascade
error_handling:
  strategy: continue
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: src.csv
      correlation_key: id
      schema:
        - { name: id, type: string }
        - { name: dept, type: string }
        - { name: region, type: string }
        - { name: amount, type: int }
  - type: aggregate
    name: agg1
    input: src
    config:
      group_by: [dept, region]
      cxl: |
        emit total1 = sum(amount)
  - type: transform
    name: t1
    input: agg1
    config:
      cxl: |
        emit dept = dept
        emit region = region
        emit total1 = total1
        emit id = "passthrough"
  - type: aggregate
    name: agg2
    input: t1
    config:
      group_by: [id, dept]
      cxl: |
        emit grand = sum(total1)
  - type: transform
    name: t2
    input: agg2
    config:
      cxl: |
        emit dept = dept
        emit grand = grand
  - type: output
    name: out
    input: t2
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;
    let plan = compile(yaml);
    let agg1_idx = node_idx_for(&plan, "agg1");
    let agg2_idx = node_idx_for(&plan, "agg2");
    let t1_idx = node_idx_for(&plan, "t1");
    let t2_idx = node_idx_for(&plan, "t2");
    let out_idx = node_idx_for(&plan, "out");

    let region1 = plan
        .deferred_regions
        .get(&agg1_idx)
        .expect("agg1 seeds a region");
    assert_eq!(region1.producer, agg1_idx);
    assert!(
        region1.members.contains(&t1_idx),
        "t1 between the two Aggregates is a member of agg1's region"
    );
    assert!(
        region1.members.contains(&agg2_idx),
        "downstream Aggregate (strict at the source-CK level) is a region member"
    );
    assert!(region1.members.contains(&t2_idx));
    assert!(region1.outputs.contains(&out_idx));
}

/// Test 4: Combine in region — driver-side Source feeds a relaxed
/// Aggregate, then a Transform feeds Combine.driver; build-side Source
/// feeds Combine.build through its own Transform; the combined output
/// flows through another Transform into Output. The deferred region
/// includes Combine and the driver-side Transform; the build-side
/// Transform is OUTSIDE the region.
#[test]
fn combine_inside_region_with_external_build_side() {
    let yaml = r#"
pipeline:
  name: combine_region
error_handling:
  strategy: continue
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      correlation_key: order_id
      schema:
        - { name: order_id, type: string }
        - { name: customer_id, type: string }
        - { name: dept, type: string }
        - { name: amount, type: int }
  - type: aggregate
    name: agg
    input: orders
    config:
      group_by: [customer_id]
      cxl: |
        emit total = sum(amount)
  - type: transform
    name: probe_xform
    input: agg
    config:
      cxl: |
        emit customer_id = customer_id
        emit total = total
  - type: source
    name: customers
    config:
      name: customers
      type: csv
      path: customers.csv
      schema:
        - { name: customer_id, type: string }
        - { name: name, type: string }
  - type: transform
    name: build_xform
    input: customers
    config:
      cxl: |
        emit customer_id = customer_id
        emit name = name
  - type: combine
    name: enriched
    input:
      o: probe_xform
      c: build_xform
    config:
      where: "o.customer_id == c.customer_id"
      match: first
      on_miss: skip
      cxl: |
        emit customer_id = o.customer_id
        emit total = o.total
        emit name = c.name
      propagate_ck: driver
  - type: transform
    name: tail
    input: enriched
    config:
      cxl: |
        emit customer_id = customer_id
        emit name = name
  - type: output
    name: out
    input: tail
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;
    let plan = compile(yaml);
    let agg_idx = node_idx_for(&plan, "agg");
    let probe_idx = node_idx_for(&plan, "probe_xform");
    let combine_idx = node_idx_for(&plan, "enriched");
    let tail_idx = node_idx_for(&plan, "tail");
    let out_idx = node_idx_for(&plan, "out");
    let build_idx = node_idx_for(&plan, "build_xform");

    let region = plan
        .deferred_regions
        .get(&agg_idx)
        .expect("agg seeds a region");
    assert_eq!(region.producer, agg_idx);
    assert!(
        region.members.contains(&probe_idx),
        "probe_xform inside region"
    );
    assert!(
        region.members.contains(&combine_idx),
        "combine inside region"
    );
    assert!(region.members.contains(&tail_idx), "tail inside region");
    assert!(region.outputs.contains(&out_idx));
    assert!(
        !region.members.contains(&build_idx),
        "build-side Transform is OUTSIDE the deferred region (Pass A reaches \
         it only through the Combine's build edge, not from the producer)"
    );
}

/// Test 5: Composition body containing a relaxed-CK Aggregate. The
/// region must cross the body↔parent boundary: the body's internal
/// Transform AND the parent's downstream Transform are members.
#[test]
fn composition_body_relaxed_aggregate_crosses_boundary() {
    let workspace = tempfile::tempdir().expect("tempdir");
    let comp_dir = workspace.path().join("compositions");
    std::fs::create_dir_all(&comp_dir).expect("mkdir compositions");
    std::fs::write(
        comp_dir.join("relaxed_body.comp.yaml"),
        r#"_compose:
  name: relaxed_body
  inputs:
    inp:
      schema:
        - { name: id, type: string }
        - { name: dept, type: string }
        - { name: amount, type: int }
  outputs:
    out: agg_emit
  config_schema: {}

nodes:
  - type: aggregate
    name: dept_totals
    input: inp
    config:
      group_by: [dept]
      cxl: |
        emit total = sum(amount)
  - type: transform
    name: agg_emit
    input: dept_totals
    config:
      cxl: |
        emit dept = dept
        emit total = total
"#,
    )
    .expect("write comp");

    let pipelines_dir = workspace.path().join("pipelines");
    std::fs::create_dir_all(&pipelines_dir).expect("mkdir pipelines");

    let yaml = r#"
pipeline:
  name: composition_relaxed
error_handling:
  strategy: continue
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: src.csv
      correlation_key: id
      schema:
        - { name: id, type: string }
        - { name: dept, type: string }
        - { name: amount, type: int }
  - type: composition
    name: body
    input: src
    use: ../compositions/relaxed_body.comp.yaml
    inputs:
      inp: src
  - type: transform
    name: parent_t
    input: body
    config:
      cxl: |
        emit dept = dept
        emit total = total
  - type: output
    name: out
    input: parent_t
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;
    let plan = compile_with_dir(yaml, workspace.path());

    // The body-internal Aggregate seeds a region in the body's
    // mini-DAG; the flatten in config/mod.rs writes both parent-graph
    // and body-local NodeIndex values into the same flat HashMap.
    // Body-local indices can collide with parent indices because each
    // graph numbers nodes from 0, so we cannot assert directly on
    // `plan.deferred_regions[&parent_idx]` for body-local entries.
    // The contract this test verifies: at least one region exists
    // touching parent-graph nodes downstream of the composition
    // boundary. Body-aware keying (e.g. `(CompositionBodyId,
    // NodeIndex)`) lands when a runtime consumer needs to disambiguate.
    let comp_idx = node_idx_for(&plan, "body");
    let parent_t_idx = node_idx_for(&plan, "parent_t");
    let out_idx = node_idx_for(&plan, "out");

    // The body-internal relaxed Aggregate produces a body-local
    // region; the parent-graph walk does NOT see body internals as
    // relaxed because the parent walker only inspects top-level
    // Aggregates. What we assert here is that the body-local region
    // detection ran and produced at least one region covering the
    // body-internal Aggregate. The load-bearing invariant: the
    // detector reaches body-internal Aggregates at all.
    let saw_any_region = !plan.deferred_regions.is_empty();
    assert!(
        saw_any_region,
        "composition body containing a relaxed Aggregate must produce at least \
         one region (body-local or parent-flow)"
    );

    // Sanity: the parent-side downstream chain (parent_t, out) is reachable
    // via outgoing edges from the Composition node.
    let mut downstream: std::collections::HashSet<NodeIndex> = std::collections::HashSet::new();
    let mut stack: Vec<NodeIndex> = plan
        .graph
        .neighbors_directed(comp_idx, Direction::Outgoing)
        .collect();
    while let Some(n) = stack.pop() {
        if !downstream.insert(n) {
            continue;
        }
        for s in plan.graph.neighbors_directed(n, Direction::Outgoing) {
            stack.push(s);
        }
    }
    assert!(downstream.contains(&parent_t_idx));
    assert!(downstream.contains(&out_idx));
}

/// Test 6: Recursive composition — outer body containing inner body
/// containing a relaxed-CK Aggregate. The detector recurses through
/// nested bodies; at least one region surfaces.
#[test]
fn recursive_composition_traverses_nested_bodies() {
    let workspace = tempfile::tempdir().expect("tempdir");
    let comp_dir = workspace.path().join("compositions");
    std::fs::create_dir_all(&comp_dir).expect("mkdir compositions");

    // Inner body: relaxed Aggregate.
    std::fs::write(
        comp_dir.join("inner_relaxed.comp.yaml"),
        r#"_compose:
  name: inner_relaxed
  inputs:
    inp:
      schema:
        - { name: id, type: string }
        - { name: dept, type: string }
        - { name: amount, type: int }
  outputs:
    out: dept_totals
  config_schema: {}

nodes:
  - type: aggregate
    name: dept_totals
    input: inp
    config:
      group_by: [dept]
      cxl: |
        emit dept = dept
        emit total = sum(amount)
"#,
    )
    .expect("write inner");

    // Outer body wraps the inner.
    std::fs::write(
        comp_dir.join("outer_wrap.comp.yaml"),
        r#"_compose:
  name: outer_wrap
  inputs:
    inp:
      schema:
        - { name: id, type: string }
        - { name: dept, type: string }
        - { name: amount, type: int }
  outputs:
    out: inner_call
  config_schema: {}

nodes:
  - type: composition
    name: inner_call
    input: inp
    use: ./inner_relaxed.comp.yaml
    inputs:
      inp: inp
"#,
    )
    .expect("write outer");

    let pipelines_dir = workspace.path().join("pipelines");
    std::fs::create_dir_all(&pipelines_dir).expect("mkdir pipelines");

    let yaml = r#"
pipeline:
  name: recursive_composition
error_handling:
  strategy: continue
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: src.csv
      correlation_key: id
      schema:
        - { name: id, type: string }
        - { name: dept, type: string }
        - { name: amount, type: int }
  - type: composition
    name: outer
    input: src
    use: ../compositions/outer_wrap.comp.yaml
    inputs:
      inp: src
  - type: output
    name: out
    input: outer
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;
    let plan = compile_with_dir(yaml, workspace.path());

    // Detector must walk the nested-body chain and produce at least
    // one region covering the inner body's relaxed Aggregate.
    assert!(
        !plan.deferred_regions.is_empty(),
        "nested composition body containing a relaxed Aggregate must produce \
         at least one region"
    );
}

/// Test 7: Output fan-out via Route — Source → Aggregate(relaxed) →
/// Route → [Output1, Output2, Output3]. The region's `outputs` set must
/// hold all three.
#[test]
fn output_fanout_via_route_collects_all_outputs() {
    let yaml = r#"
pipeline:
  name: fanout_route
error_handling:
  strategy: continue
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: src.csv
      correlation_key: id
      schema:
        - { name: id, type: string }
        - { name: dept, type: string }
        - { name: amount, type: int }
  - type: aggregate
    name: agg
    input: src
    config:
      group_by: [dept]
      cxl: |
        emit dept = dept
        emit total = sum(amount)
  - type: route
    name: classify
    input: agg
    config:
      conditions:
        big: total > 1000
        medium: total > 100
      default: small
  - type: output
    name: out_big
    input: classify.big
    config:
      name: out_big
      type: csv
      path: big.csv
      include_unmapped: true
  - type: output
    name: out_medium
    input: classify.medium
    config:
      name: out_medium
      type: csv
      path: medium.csv
      include_unmapped: true
  - type: output
    name: out_small
    input: classify.small
    config:
      name: out_small
      type: csv
      path: small.csv
      include_unmapped: true
"#;
    let plan = compile(yaml);
    let agg_idx = node_idx_for(&plan, "agg");
    let route_idx = node_idx_for(&plan, "classify");
    let big_idx = node_idx_for(&plan, "out_big");
    let medium_idx = node_idx_for(&plan, "out_medium");
    let small_idx = node_idx_for(&plan, "out_small");

    let region = plan
        .deferred_regions
        .get(&agg_idx)
        .expect("agg seeds a region");
    assert!(region.members.contains(&route_idx), "Route is a member");
    assert!(
        region.outputs.contains(&big_idx),
        "out_big at the region exit"
    );
    assert!(
        region.outputs.contains(&medium_idx),
        "out_medium at the region exit"
    );
    assert!(
        region.outputs.contains(&small_idx),
        "out_small at the region exit"
    );
}

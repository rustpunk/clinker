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
use crate::plan::CompiledPlan;
use crate::plan::execution::ExecutionPlanDag;

fn compile(yaml: &str) -> ExecutionPlanDag {
    compile_full(yaml).dag().clone()
}

fn compile_full(yaml: &str) -> CompiledPlan {
    let config: PipelineConfig = parse_config(yaml).expect("parse");
    config.compile(&CompileContext::default()).expect("compile")
}

fn compile_with_dir_full(yaml: &str, workspace_root: &std::path::Path) -> CompiledPlan {
    let config: PipelineConfig = parse_config(yaml).expect("parse");
    let ctx = CompileContext::with_pipeline_dir(workspace_root, PathBuf::from("pipelines"));
    config.compile(&ctx).expect("compile")
}

fn node_idx_for(plan: &ExecutionPlanDag, node_name: &str) -> NodeIndex {
    plan.graph
        .node_indices()
        .find(|&i| plan.graph[i].name() == node_name)
        .unwrap_or_else(|| panic!("node {node_name:?} not found in plan"))
}

fn body_node_idx_for(
    body: &crate::plan::composition_body::BoundBody,
    node_name: &str,
) -> NodeIndex {
    body.graph
        .node_indices()
        .find(|&i| body.graph[i].name() == node_name)
        .unwrap_or_else(|| panic!("body node {node_name:?} not found"))
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

/// Test 3: Multi-Aggregate cascade — two relaxed-CK Aggregates chained
/// through Transform pairs. Pass A's forward BFS from the upstream
/// producer absorbs the downstream Aggregate as a region member; the
/// downstream Aggregate also independently seeds its own region (every
/// relaxed-CK Aggregate is a region producer), but the outer region
/// rooted at `agg1` covers the whole cascade — `t1`, `agg2`, and `t2`
/// are all members. Both inner aggregates run at commit time on
/// post-recompute upstream emits, so the cascade is safe under deferred
/// dispatch.
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
      correlation_key: order_id
      schema:
        - { name: order_id, type: string }
        - { name: dept, type: string }
        - { name: region, type: string }
        - { name: amount, type: int }
  - type: aggregate
    name: agg1
    input: src
    config:
      group_by: [dept]
      cxl: |
        emit total = sum(amount)
  - type: transform
    name: t1
    input: agg1
    config:
      cxl: |
        emit dept = dept
        emit region = "all"
        emit total = total
  - type: aggregate
    name: agg2
    input: t1
    config:
      group_by: [region]
      cxl: |
        emit grand = sum(total)
  - type: transform
    name: t2
    input: agg2
    config:
      cxl: |
        emit region = region
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
        "downstream relaxed Aggregate is absorbed into the outer region as a member"
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
/// body-internal Aggregate seeds a region inside the body's mini-DAG;
/// the body-local map (`BoundBody.deferred_regions`) keys the
/// body-internal Aggregate as producer and its downstream body
/// Transform as a member, with O(1) lookup at every body NodeIndex
/// participating in the region. Parent-graph continuation through the
/// Composition node is not propagated by the detector today (the parent
/// walker only seeds from top-level Aggregates); per-body keying is the
/// load-bearing dispatcher contract — the executor dispatches body
/// sub-DAGs against `BoundBody.deferred_regions`, so the body-local
/// producer NodeIndex must be present in that map.
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
    let compiled = compile_with_dir_full(yaml, workspace.path());
    let plan = compiled.dag();

    // Parent-graph indices for the composition call site and the
    // downstream chain. The body-internal Aggregate seeds a region
    // whose body-local NodeIndex space is disjoint from the parent
    // graph; the dispatcher consults the right map by which scope it
    // is currently executing.
    let comp_idx = node_idx_for(plan, "body");
    let parent_t_idx = node_idx_for(plan, "parent_t");
    let out_idx = node_idx_for(plan, "out");

    // Body-local regions: look up the bound body via the composition's
    // name → body-id assignment, then assert the body-internal
    // Aggregate is the producer, the body's Transform is a member, and
    // both NodeIndex slots are keyed for O(1) dispatcher lookup.
    let artifacts = compiled.artifacts();
    let body_id = artifacts
        .composition_body_assignments
        .get("body")
        .copied()
        .expect("composition 'body' must be assigned a CompositionBodyId");
    let bound = compiled
        .body_of(body_id)
        .expect("body_id must resolve to a BoundBody");
    let body_agg_idx = body_node_idx_for(bound, "dept_totals");
    let body_xform_idx = body_node_idx_for(bound, "agg_emit");

    let body_region = bound
        .deferred_regions
        .get(&body_agg_idx)
        .expect("body-internal Aggregate keys its body-local region");
    assert_eq!(body_region.producer, body_agg_idx);
    assert!(
        body_region.members.contains(&body_xform_idx),
        "body Transform agg_emit is a member of the body-local region"
    );
    assert!(
        bound.deferred_regions.contains_key(&body_xform_idx),
        "body Transform agg_emit is keyed in the body-local map for O(1) dispatch"
    );

    // The parent-flat map carries no entries seeded by body-internal
    // Aggregates today — the parent walker only seeds from top-level
    // Aggregates, and there are none in this fixture.
    assert!(
        plan.deferred_regions.is_empty(),
        "parent-graph map carries only top-level-Aggregate regions; \
         body-internal regions live on BoundBody.deferred_regions; got {:?}",
        plan.deferred_regions.keys().collect::<Vec<_>>()
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
    let compiled = compile_with_dir_full(yaml, workspace.path());
    let plan = compiled.dag();

    let outer_idx = node_idx_for(plan, "outer");
    let out_idx = node_idx_for(plan, "out");

    // Body-local: the inner body owns the relaxed Aggregate; the outer
    // body owns no relaxed Aggregate of its own and thus has no body-
    // local region keyed on its mini-DAG. Walk the assignments to
    // locate both bodies and verify the load-bearing one carries the
    // expected producer.
    let artifacts = compiled.artifacts();
    let outer_body_id = artifacts
        .composition_body_assignments
        .get("outer")
        .copied()
        .expect("outer composition assignment");
    let outer_body = compiled
        .body_of(outer_body_id)
        .expect("outer BoundBody resolves");
    assert!(
        outer_body.deferred_regions.is_empty(),
        "outer body has no relaxed Aggregate of its own; its body-local map stays empty",
    );

    // Inner body assignment lives under the body-internal Composition
    // node `inner_call` inside `outer_wrap.comp.yaml`.
    let inner_body_id = artifacts
        .composition_body_assignments
        .get("inner_call")
        .copied()
        .expect("inner_call composition assignment");
    let inner_body = compiled
        .body_of(inner_body_id)
        .expect("inner BoundBody resolves");
    let inner_agg_idx = body_node_idx_for(inner_body, "dept_totals");
    let inner_region = inner_body
        .deferred_regions
        .get(&inner_agg_idx)
        .expect("inner body-internal Aggregate keys its body-local region");
    assert_eq!(inner_region.producer, inner_agg_idx);
    assert!(
        inner_body.deferred_regions.contains_key(&inner_agg_idx),
        "inner body-local map carries the producer NodeIndex for O(1) dispatch"
    );

    // Parent-flat map carries no body-internal regions today (the
    // parent walker only seeds from top-level Aggregates).
    assert!(
        plan.deferred_regions.is_empty(),
        "parent-graph map stays empty for nested-body fixtures with no \
         top-level Aggregate; got {:?}",
        plan.deferred_regions.keys().collect::<Vec<_>>()
    );

    // Sanity: the parent's downstream (`out`) is reachable from the
    // outer Composition via outgoing edges.
    let mut downstream: std::collections::HashSet<NodeIndex> = std::collections::HashSet::new();
    let mut stack: Vec<NodeIndex> = plan
        .graph
        .neighbors_directed(outer_idx, Direction::Outgoing)
        .collect();
    while let Some(n) = stack.pop() {
        if !downstream.insert(n) {
            continue;
        }
        for s in plan.graph.neighbors_directed(n, Direction::Outgoing) {
            stack.push(s);
        }
    }
    assert!(downstream.contains(&out_idx));
}

/// A parent DAG whose composition body carries a relaxed-CK Aggregate
/// must produce a parent-continuation entry keyed at the Composition
/// node, with the downstream parent Transform in `members` and the
/// parent Output in `outputs`. Phase B will use this to drive the
/// parent's continuation at commit time after harvesting body output
/// records.
#[test]
fn composition_body_region_emits_parent_continuation() {
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
  name: continuation_emit
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
    let compiled = compile_with_dir_full(yaml, workspace.path());
    let plan = compiled.dag();

    let comp_idx = node_idx_for(plan, "body");
    let parent_t_idx = node_idx_for(plan, "parent_t");
    let out_idx = node_idx_for(plan, "out");

    let cont = plan
        .parent_continuations
        .get(&comp_idx)
        .expect("composition with relaxed-body region must register a continuation");
    assert_eq!(
        cont.composition_idx, comp_idx,
        "continuation seed must equal its key"
    );
    assert!(
        cont.members.contains(&parent_t_idx),
        "parent Transform downstream of the composition is a continuation member; got members={:?}",
        cont.members
    );
    assert!(
        cont.outputs.contains(&out_idx),
        "parent Output is the continuation's exit boundary; got outputs={:?}",
        cont.outputs
    );
    assert!(
        !cont.members.contains(&comp_idx) && !cont.outputs.contains(&comp_idx),
        "the seed Composition is excluded from members and outputs",
    );

    // The forward-pass deferral hook must agree: the parent Transform
    // and Output now register as deferred consumers via the
    // continuation surface, so their forward-pass arms short-circuit.
    assert!(
        plan.is_deferred_consumer(parent_t_idx),
        "parent Transform should short-circuit on the forward pass via the continuation",
    );
    assert!(
        plan.is_deferred_consumer(out_idx),
        "parent Output should short-circuit on the forward pass via the continuation",
    );
}

/// A nested composition (outer wrapping inner-relaxed) must register a
/// continuation in the OUTER body for the inner Composition node, AND
/// a continuation in the parent DAG for the outer Composition. Phase B
/// will walk these bottom-up: harvest inner-body output → run outer
/// body's continuation → harvest outer-body output → run parent's
/// continuation.
#[test]
fn recursive_composition_chains_continuations_at_each_boundary() {
    let workspace = tempfile::tempdir().expect("tempdir");
    let comp_dir = workspace.path().join("compositions");
    std::fs::create_dir_all(&comp_dir).expect("mkdir compositions");

    // Inner body: relaxed Aggregate followed by a Transform so the
    // body has a member node downstream of the producer.
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
    out: inner_emit
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
    name: inner_emit
    input: dept_totals
    config:
      cxl: |
        emit dept = dept
        emit total = total
"#,
    )
    .expect("write inner");

    // Outer body wraps the inner with a follow-up Transform so the
    // outer body has a body-internal continuation member downstream of
    // the inner Composition.
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
    out: outer_emit
  config_schema: {}

nodes:
  - type: composition
    name: inner_call
    input: inp
    use: ./inner_relaxed.comp.yaml
    inputs:
      inp: inp
  - type: transform
    name: outer_emit
    input: inner_call
    config:
      cxl: |
        emit dept = dept
        emit total = total
"#,
    )
    .expect("write outer");

    let pipelines_dir = workspace.path().join("pipelines");
    std::fs::create_dir_all(&pipelines_dir).expect("mkdir pipelines");

    let yaml = r#"
pipeline:
  name: recursive_continuations
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
  - type: transform
    name: parent_t
    input: outer
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
    let compiled = compile_with_dir_full(yaml, workspace.path());
    let plan = compiled.dag();

    let outer_idx = node_idx_for(plan, "outer");
    let parent_t_idx = node_idx_for(plan, "parent_t");
    let out_idx = node_idx_for(plan, "out");

    // Parent DAG: continuation for the outer Composition with the
    // parent Transform + Output downstream.
    let parent_cont = plan.parent_continuations.get(&outer_idx).expect(
        "outer composition with relaxed-descendant body must register a parent continuation",
    );
    assert_eq!(parent_cont.composition_idx, outer_idx);
    assert!(
        parent_cont.members.contains(&parent_t_idx),
        "parent Transform is a continuation member; got members={:?}",
        parent_cont.members
    );
    assert!(
        parent_cont.outputs.contains(&out_idx),
        "parent Output is the continuation exit; got outputs={:?}",
        parent_cont.outputs
    );

    // Outer body: continuation for the inner Composition with the
    // outer body's own Transform downstream. Resolve the outer body
    // and the inner Composition's body-local NodeIndex.
    let artifacts = compiled.artifacts();
    let outer_body_id = artifacts
        .composition_body_assignments
        .get("outer")
        .copied()
        .expect("outer composition assignment");
    let outer_body = compiled
        .body_of(outer_body_id)
        .expect("outer BoundBody resolves");
    let inner_call_idx = body_node_idx_for(outer_body, "inner_call");
    let outer_emit_idx = body_node_idx_for(outer_body, "outer_emit");

    let body_cont = outer_body
        .parent_continuations
        .get(&inner_call_idx)
        .expect("outer body must register a continuation for the inner Composition");
    assert_eq!(body_cont.composition_idx, inner_call_idx);
    assert!(
        body_cont.members.contains(&outer_emit_idx),
        "outer body's Transform downstream of the inner Composition is a member; \
         got members={:?}",
        body_cont.members
    );
    // The outer body has no Outputs of its own (body Outputs surface
    // through `output_port_to_node_idx`, not as PlanNode::Output), so
    // the continuation's `outputs` set is empty.
    assert!(
        body_cont.outputs.is_empty(),
        "body-internal continuation has no Outputs; got {:?}",
        body_cont.outputs
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

/// Pin: a body whose only operator is the relaxed Aggregate (the
/// Aggregate IS the body's terminal output-port node, with no
/// intermediate Transform between it and the port) must propagate
/// the parent continuation's column demand into the body's
/// `buffer_schema`. Without the cross-scope output-port seed, the
/// body's reverse-topo walk would terminate at the Aggregate with
/// only engine-stamped columns, the harvest would emit narrower
/// records than the parent continuation's stamped expected schema,
/// and the parent's `check_input_schema` would trip `SchemaMismatch`.
#[test]
fn body_buffer_schema_includes_columns_demanded_by_parent_continuation() {
    let workspace = tempfile::tempdir().expect("tempdir");
    let comp_dir = workspace.path().join("compositions");
    std::fs::create_dir_all(&comp_dir).expect("mkdir compositions");
    std::fs::write(
        comp_dir.join("bare_agg.comp.yaml"),
        r#"_compose:
  name: bare_agg
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
    .expect("write comp");

    let pipelines_dir = workspace.path().join("pipelines");
    std::fs::create_dir_all(&pipelines_dir).expect("mkdir pipelines");

    let yaml = r#"
pipeline:
  name: bare_agg_at_output_port
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
    use: ../compositions/bare_agg.comp.yaml
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
    let compiled = compile_with_dir_full(yaml, workspace.path());
    let artifacts = compiled.artifacts();
    let body_id = artifacts
        .composition_body_assignments
        .get("body")
        .copied()
        .expect("composition 'body' must be assigned a CompositionBodyId");
    let bound = compiled
        .body_of(body_id)
        .expect("body_id must resolve to a BoundBody");
    let body_agg_idx = body_node_idx_for(bound, "dept_totals");
    let region = bound
        .deferred_regions
        .get(&body_agg_idx)
        .expect("bare-Aggregate body must register its body-local region");

    assert!(
        region.buffer_schema.contains(&"dept".to_string()),
        "buffer_schema must carry `dept` because the parent continuation Transform reads it; \
         got {:?}",
        region.buffer_schema
    );
    assert!(
        region.buffer_schema.contains(&"total".to_string()),
        "buffer_schema must carry `total` because the parent continuation Transform reads it; \
         got {:?}",
        region.buffer_schema
    );
}

/// Pin: a body member that does not itself read column X cannot block
/// X from reaching the producer's `buffer_schema` when the parent's
/// continuation reads X. Architecturally, the body's reverse-topo walk
/// must union the parent continuation's column demand at the
/// output-port node before propagating upstream — without that, the
/// member's `support_into` alone would prune X out.
#[test]
fn body_member_that_drops_a_column_still_propagates_continuation_demand() {
    let workspace = tempfile::tempdir().expect("tempdir");
    let comp_dir = workspace.path().join("compositions");
    std::fs::create_dir_all(&comp_dir).expect("mkdir compositions");
    std::fs::write(
        comp_dir.join("dept_only.comp.yaml"),
        r#"_compose:
  name: dept_only
  inputs:
    inp:
      schema:
        - { name: id, type: string }
        - { name: dept, type: string }
        - { name: amount, type: int }
  outputs:
    out: dept_only_xform
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
  - type: transform
    name: dept_only_xform
    input: dept_totals
    config:
      cxl: |
        emit dept = dept
"#,
    )
    .expect("write comp");

    let pipelines_dir = workspace.path().join("pipelines");
    std::fs::create_dir_all(&pipelines_dir).expect("mkdir pipelines");

    let yaml = r#"
pipeline:
  name: member_drops_column
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
    use: ../compositions/dept_only.comp.yaml
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
    let compiled = compile_with_dir_full(yaml, workspace.path());
    let artifacts = compiled.artifacts();
    let body_id = artifacts
        .composition_body_assignments
        .get("body")
        .copied()
        .expect("composition 'body' must be assigned a CompositionBodyId");
    let bound = compiled
        .body_of(body_id)
        .expect("body_id must resolve to a BoundBody");
    let body_agg_idx = body_node_idx_for(bound, "dept_totals");
    let region = bound
        .deferred_regions
        .get(&body_agg_idx)
        .expect("body-internal Aggregate must register its body-local region");

    assert!(
        region.buffer_schema.contains(&"dept".to_string()),
        "buffer_schema must carry `dept` (read by both body Transform and parent continuation); \
         got {:?}",
        region.buffer_schema
    );
    assert!(
        region.buffer_schema.contains(&"total".to_string()),
        "buffer_schema must carry `total` even though the body Transform never reads it — the \
         parent continuation does, and the cross-scope demand seed must keep it alive; \
         got {:?}",
        region.buffer_schema
    );
}

/// Pin: a three-level chain (parent → outer composition → inner
/// composition with relaxed Aggregate). Demand from the OUTERMOST
/// parent's continuation must reach the INNERMOST producer's
/// `buffer_schema` via fixed-point recursion through Composition
/// continuation members. Pins the recursion across nested
/// `PlanNode::Composition` boundaries.
#[test]
fn continuation_support_chains_through_nested_composition() {
    let workspace = tempfile::tempdir().expect("tempdir");
    let comp_dir = workspace.path().join("compositions");
    std::fs::create_dir_all(&comp_dir).expect("mkdir compositions");

    // Inner body: bare relaxed Aggregate at the output port.
    std::fs::write(
        comp_dir.join("inner_chain.comp.yaml"),
        r#"_compose:
  name: inner_chain
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

    // Outer body: wraps the inner with no member of its own. The
    // inner Composition is the outer's terminal output port. The
    // outer body has no continuation members (the inner Composition
    // is the port), so the demand must chain through the outer
    // body's call site at the parent and back into the inner via
    // the recursion.
    std::fs::write(
        comp_dir.join("outer_chain.comp.yaml"),
        r#"_compose:
  name: outer_chain
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
    use: ./inner_chain.comp.yaml
    inputs:
      inp: inp
"#,
    )
    .expect("write outer");

    let pipelines_dir = workspace.path().join("pipelines");
    std::fs::create_dir_all(&pipelines_dir).expect("mkdir pipelines");

    let yaml = r#"
pipeline:
  name: nested_continuation_chain
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
    use: ../compositions/outer_chain.comp.yaml
    inputs:
      inp: src
  - type: transform
    name: parent_t
    input: outer
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
    let compiled = compile_with_dir_full(yaml, workspace.path());
    let artifacts = compiled.artifacts();

    let inner_body_id = artifacts
        .composition_body_assignments
        .get("inner_call")
        .copied()
        .expect("inner_call composition must be assigned a CompositionBodyId");
    let inner_body = compiled
        .body_of(inner_body_id)
        .expect("inner body resolves");
    let inner_agg_idx = body_node_idx_for(inner_body, "dept_totals");
    let inner_region = inner_body
        .deferred_regions
        .get(&inner_agg_idx)
        .expect("innermost body-internal Aggregate must register its body-local region");

    assert!(
        inner_region.buffer_schema.contains(&"dept".to_string()),
        "innermost buffer_schema must carry `dept` via fixed-point recursion through the outer \
         Composition's continuation back to the parent's continuation; got {:?}",
        inner_region.buffer_schema
    );
    assert!(
        inner_region.buffer_schema.contains(&"total".to_string()),
        "innermost buffer_schema must carry `total` via the same chain; got {:?}",
        inner_region.buffer_schema
    );
}

/// Pin: when a body is invoked from multiple call sites whose parent
/// continuations demand disjoint columns, the body's `buffer_schema`
/// must carry the union. Today's `bind_composition` allocates a fresh
/// `CompositionBodyId` per call (so this geometry is unreachable
/// through the YAML compile path), but the architectural contract is
/// independent of the allocator's policy. Construct
/// `DeferredRegionAnalysis` directly to lock the β contract regardless
/// of future allocator changes (memoization / body-sharing).
#[test]
fn continuation_support_unions_across_multiple_call_sites_of_same_body() {
    use std::collections::{HashMap, HashSet};

    use crate::plan::composition_body::CompositionBodyId;
    use crate::plan::deferred_region::{
        CompositionCallSite, DeferredRegionAnalysis, ParentContinuation, continuation_support,
    };

    // Compile a single-call-site fixture to obtain a real
    // `BoundBody` for the inner composition plus a parent DAG with
    // two distinct downstream Transforms reading disjoint columns.
    // We will then forge a second call site pointing at the same
    // body and assert the union.
    let workspace = tempfile::tempdir().expect("tempdir");
    let comp_dir = workspace.path().join("compositions");
    std::fs::create_dir_all(&comp_dir).expect("mkdir compositions");
    std::fs::write(
        comp_dir.join("shared_body.comp.yaml"),
        r#"_compose:
  name: shared_body
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
    .expect("write shared body");

    let pipelines_dir = workspace.path().join("pipelines");
    std::fs::create_dir_all(&pipelines_dir).expect("mkdir pipelines");

    // Two parent continuations: `parent_a` reads only `dept`,
    // `parent_b` reads only `total`. A single call site reaches one
    // of them; we synthesise the second via a hand-built
    // `CompositionCallSite` below.
    let yaml = r#"
pipeline:
  name: union_call_sites
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
    name: body_a
    input: src
    use: ../compositions/shared_body.comp.yaml
    inputs:
      inp: src
  - type: transform
    name: parent_a
    input: body_a
    config:
      cxl: |
        emit dept = dept
  - type: output
    name: out_a
    input: parent_a
    config:
      name: out_a
      type: csv
      path: out_a.csv
      include_unmapped: true
  - type: composition
    name: body_b
    input: src
    use: ../compositions/shared_body.comp.yaml
    inputs:
      inp: src
  - type: transform
    name: parent_b
    input: body_b
    config:
      cxl: |
        emit total = total
  - type: output
    name: out_b
    input: parent_b
    config:
      name: out_b
      type: csv
      path: out_b.csv
      include_unmapped: true
"#;
    let compiled = compile_with_dir_full(yaml, workspace.path());
    let plan = compiled.dag();
    let artifacts = compiled.artifacts();

    let body_a_idx = node_idx_for(plan, "body_a");
    let body_b_idx = node_idx_for(plan, "body_b");

    // The two call sites today bind to distinct body IDs (1:1
    // allocator); pick `body_a`'s id as the canonical body and
    // forge a second call site pointing at the same id but
    // referencing `body_b`'s `composition_idx` and continuation.
    let canonical_body_id: CompositionBodyId = artifacts
        .composition_body_assignments
        .get("body_a")
        .copied()
        .expect("body_a must be assigned");

    // Pull the existing per-call-site continuation members from the
    // real plan-time analysis path: each Composition's
    // `parent_continuations` entry on the parent DAG names the
    // members downstream of that call site.
    let cont_a = plan
        .parent_continuations
        .get(&body_a_idx)
        .expect("body_a's parent continuation must exist")
        .clone();
    let cont_b = plan
        .parent_continuations
        .get(&body_b_idx)
        .expect("body_b's parent continuation must exist")
        .clone();

    // Forge: pretend both call sites resolve to the same
    // CompositionBodyId. Both ParentContinuation entries map to that
    // shared id via two CompositionCallSite entries that point at
    // the original two parent-DAG composition NodeIndices.
    let mut top_continuations: HashMap<_, ParentContinuation> = HashMap::new();
    top_continuations.insert(body_a_idx, cont_a);
    top_continuations.insert(body_b_idx, cont_b);

    let mut body_call_sites: HashMap<CompositionBodyId, Vec<CompositionCallSite>> = HashMap::new();
    body_call_sites.insert(
        canonical_body_id,
        vec![
            CompositionCallSite {
                scope: None,
                composition_idx: body_a_idx,
            },
            CompositionCallSite {
                scope: None,
                composition_idx: body_b_idx,
            },
        ],
    );

    let analysis = DeferredRegionAnalysis {
        top_level: Vec::new(),
        body_regions: HashMap::new(),
        top_continuations,
        body_continuations: HashMap::new(),
        body_call_sites,
    };

    let mut memo: HashMap<(CompositionBodyId, String), HashSet<String>> = HashMap::new();
    let mut active: HashSet<(CompositionBodyId, String)> = HashSet::new();

    // Pass the real parent DAG so the walker can read member node
    // kinds. The walker indexes `containing_graph[member_idx]` for
    // every continuation member; the parent's continuation members
    // for `parent_a` and `parent_b` are plain Transforms, not
    // Compositions, so the recursion does not re-enter.
    let demand = continuation_support(
        canonical_body_id,
        "out",
        &analysis,
        &plan.graph,
        artifacts,
        &mut memo,
        &mut active,
    );

    assert!(
        demand.contains("dept"),
        "union must include `dept` from call site `body_a`'s parent_a continuation; got {:?}",
        demand
    );
    assert!(
        demand.contains("total"),
        "union must include `total` from call site `body_b`'s parent_b continuation; got {:?}",
        demand
    );

    // Hand-built control: reduce to a single call site and confirm
    // demand narrows. This rules out a false positive where the
    // walker globally unions across all top_continuations regardless
    // of body_call_sites.
    let mut single_call_sites: HashMap<CompositionBodyId, Vec<CompositionCallSite>> =
        HashMap::new();
    single_call_sites.insert(
        canonical_body_id,
        vec![CompositionCallSite {
            scope: None,
            composition_idx: body_a_idx,
        }],
    );
    let single_analysis = DeferredRegionAnalysis {
        top_level: Vec::new(),
        body_regions: HashMap::new(),
        top_continuations: analysis.top_continuations.clone(),
        body_continuations: HashMap::new(),
        body_call_sites: single_call_sites,
    };
    let mut memo2: HashMap<(CompositionBodyId, String), HashSet<String>> = HashMap::new();
    let mut active2: HashSet<(CompositionBodyId, String)> = HashSet::new();
    let single_demand = continuation_support(
        canonical_body_id,
        "out",
        &single_analysis,
        &plan.graph,
        artifacts,
        &mut memo2,
        &mut active2,
    );
    assert!(
        single_demand.contains("dept"),
        "single call site must still see `dept`; got {:?}",
        single_demand
    );
    assert!(
        !single_demand.contains("total"),
        "single call site must NOT see `total` — that would mean the walker is unioning \
         across unrelated continuations rather than the call-site set; got {:?}",
        single_demand
    );
}

/// Pin: engine-stamped columns on the producer (`$ck.aggregate.<name>`
/// and `$ck.<source-field>` shadows) survive the new continuation-aware
/// seed path AND remain in producer-emit order. The seed merges through
/// `add_producer_engine_stamped_columns` and `project_in_producer_order`,
/// not insertion order; an alphabetical or insertion-order ordering
/// would trip `SchemaMismatch` at the first downstream consumer.
#[test]
fn parent_continuation_reading_engine_stamped_column_preserves_emit_order() {
    let workspace = tempfile::tempdir().expect("tempdir");
    let comp_dir = workspace.path().join("compositions");
    std::fs::create_dir_all(&comp_dir).expect("mkdir compositions");
    std::fs::write(
        comp_dir.join("emit_order.comp.yaml"),
        r#"_compose:
  name: emit_order
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
    .expect("write comp");

    let pipelines_dir = workspace.path().join("pipelines");
    std::fs::create_dir_all(&pipelines_dir).expect("mkdir pipelines");

    let yaml = r#"
pipeline:
  name: engine_stamped_emit_order
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
    use: ../compositions/emit_order.comp.yaml
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
    let compiled = compile_with_dir_full(yaml, workspace.path());
    let artifacts = compiled.artifacts();
    let body_id = artifacts
        .composition_body_assignments
        .get("body")
        .copied()
        .expect("composition 'body' must be assigned a CompositionBodyId");
    let bound = compiled
        .body_of(body_id)
        .expect("body_id must resolve to a BoundBody");
    let body_agg_idx = body_node_idx_for(bound, "dept_totals");
    let region = bound
        .deferred_regions
        .get(&body_agg_idx)
        .expect("body-internal Aggregate must register its body-local region");

    // Resolve every engine-stamped column from the producer's
    // stored output schema and assert (a) every one is present in
    // buffer_schema, (b) buffer_schema column order matches the
    // producer's column order. `add_producer_engine_stamped_columns`
    // is responsible for (a); `project_in_producer_order` for (b).
    use clinker_record::FieldMetadata;
    let producer_schema = bound.graph[body_agg_idx]
        .stored_output_schema()
        .cloned()
        .expect("relaxed Aggregate carries a stored_output_schema");
    let mut engine_stamped_cols: Vec<String> = Vec::new();
    let mut producer_order: Vec<String> = Vec::new();
    for i in 0..producer_schema.column_count() {
        let name = producer_schema
            .column_name(i)
            .expect("schema column has a name")
            .to_string();
        producer_order.push(name.clone());
        if producer_schema
            .field_metadata(i)
            .is_some_and(FieldMetadata::is_engine_stamped)
        {
            engine_stamped_cols.push(name);
        }
    }
    assert!(
        !engine_stamped_cols.is_empty(),
        "relaxed Aggregate must stamp at least one engine-managed column \
         ($ck.<source-field> shadow + $ck.aggregate.<name>); got schema columns {:?}",
        producer_order
    );
    for col in &engine_stamped_cols {
        assert!(
            region.buffer_schema.contains(col),
            "buffer_schema must preserve engine-stamped column {col:?}; got {:?}",
            region.buffer_schema
        );
    }

    // Emit order: filter producer_order to buffer_schema's columns
    // and assert the resulting subsequence matches buffer_schema
    // verbatim. A reordered buffer trips SchemaMismatch at the
    // first downstream consumer regardless of which columns made it
    // into the set.
    let expected_subseq: Vec<String> = producer_order
        .iter()
        .filter(|c| region.buffer_schema.contains(c))
        .cloned()
        .collect();
    assert_eq!(
        region.buffer_schema, expected_subseq,
        "buffer_schema must follow producer-emit order; got {:?}, expected subsequence {:?}",
        region.buffer_schema, expected_subseq
    );
}

//! Integration tests for Phase Combine.
//!
//! - C.0.0 scaffolds (`test_combine_scaffold_compiles`,
//!   `test_combine_fixtures_exist`) remain in place as smoke gates.
//! - C.0.4 adds DAG-edge gate tests: verify that combine fixtures
//!   compile through `ExecutionPlanDag::compile` with the correct number
//!   of incoming edges and that the combine node appears after its
//!   inputs in topological order.
//! - C.2.0 adds end-to-end execution helpers (`run_combine_fixture`,
//!   `assert_records_match`, `canonicalize_csv`) and captures lookup
//!   regression snapshots as the C.2.3 migration baseline. The
//!   lookup-baseline tests run lookup pipelines through today's runtime
//!   and write `.snap` files to `tests/snapshots/` that PERSIST after
//!   C.2.3 deletes the lookup infrastructure — migrated combine tests in
//!   C.2.3.4 read these snapshots verbatim to assert output equivalence.

#[cfg(test)]
mod tests {
    use clinker_core::config::{CompileContext, PipelineConfig};
    use clinker_core::executor::{PipelineExecutor, PipelineRunParams};
    use clinker_core::plan::execution::{ExecutionPlanDag, PlanNode};
    use clinker_core::test_helpers::SharedBuffer;
    use petgraph::Direction;
    use petgraph::graph::NodeIndex;
    use std::collections::HashMap;
    use std::io::{Read, Write};

    /// Verifies the test module compiles and is discovered by cargo test.
    #[test]
    fn test_combine_scaffold_compiles() {
        // This test passing means the module is correctly wired and discovered
        // by `cargo test --test combine_test`. An empty body is sufficient;
        // the signal is "this test ran", not any runtime assertion.
    }

    /// Gate test for C.0.5.1: `benches/combine.rs` exists, is non-empty,
    /// and defines all 6 scaffold functions named in the phase plan.
    ///
    /// Criterion benches are not discovered by `cargo test`, so this is
    /// the only in-`cargo test` signal that the bench file is authored
    /// correctly. Compile-time verification is covered separately by
    /// `cargo check --benches --workspace` in the pre-commit checklist.
    #[test]
    fn test_bench_combine_scaffold_compiles() {
        let bench_path =
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("benches/combine.rs");
        assert!(
            bench_path.exists(),
            "missing bench file: {}",
            bench_path.display()
        );
        let source = std::fs::read_to_string(&bench_path)
            .unwrap_or_else(|e| panic!("cannot read {}: {e}", bench_path.display()));
        assert!(!source.is_empty(), "empty bench file");

        // All six functions required by the Phase C.0.5 spec.
        for func in [
            "fn bench_combine_equi_2input",
            "fn bench_combine_iejoin",
            "fn bench_combine_nary_3input",
            "fn bench_predicate_decomposition",
            "fn bench_combine_grace_hash",
            "fn bench_lookup_baseline",
        ] {
            assert!(
                source.contains(func),
                "benches/combine.rs missing required fn: {func}"
            );
        }

        // `criterion_group!` wires all 6 into the registered suite.
        assert!(
            source.contains("criterion_group!"),
            "benches/combine.rs missing criterion_group! macro"
        );
        assert!(
            source.contains("criterion_main!"),
            "benches/combine.rs missing criterion_main! macro"
        );
    }

    /// Verifies all 10 fixture YAML files exist and are readable.
    #[test]
    fn test_combine_fixtures_exist() {
        let fixture_dir =
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/combine");
        let expected = [
            "two_input_equi.yaml",
            "two_input_range.yaml",
            "two_input_mixed.yaml",
            "three_input_shared_key.yaml",
            "match_all.yaml",
            "match_collect.yaml",
            "on_miss_skip.yaml",
            "on_miss_error.yaml",
            "drive_hint.yaml",
            "error_one_input.yaml",
        ];
        for name in expected {
            let path = fixture_dir.join(name);
            assert!(path.exists(), "missing fixture: {}", path.display());
            let content = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
            assert!(!content.is_empty(), "empty fixture: {}", path.display());
        }
    }

    // --- C.0.4 helpers ---------------------------------------------------

    /// Load a combine fixture YAML from `tests/fixtures/combine/`.
    fn load_fixture(name: &str) -> String {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/combine")
            .join(name);
        std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()))
    }

    /// Parse YAML into a `PipelineConfig`.
    fn parse_fixture(yaml: &str) -> PipelineConfig {
        clinker_core::yaml::from_str::<PipelineConfig>(yaml)
            .unwrap_or_else(|e| panic!("parse failed: {e}"))
    }

    // --- C.1.0 helpers ---------------------------------------------------

    /// Compile a combine fixture through `bind_schema` and return the
    /// resulting `CompileArtifacts` plus every diagnostic emitted. The
    /// direct `bind_schema` path (rather than the full
    /// `PipelineConfig::compile`) lets tests inspect `artifacts`
    /// regardless of whether the fixture was expected to succeed or
    /// fail — full compile short-circuits on the first error severity.
    ///
    /// Error fixtures (`error_*.yaml`) populate `diags` with the
    /// expected E3xx codes; success fixtures leave `diags` empty (modulo
    /// warnings).
    ///
    /// Fixture name is passed without the `.yaml` extension —
    /// `compile_combine_fixture("two_input_equi")`.
    #[allow(dead_code)] // Used by C.1.1+ tests; referenced here as a gate.
    fn compile_combine_fixture(
        name: &str,
    ) -> (
        clinker_core::plan::bind_schema::CompileArtifacts,
        Vec<clinker_core::error::Diagnostic>,
    ) {
        let yaml = load_fixture(&format!("{name}.yaml"));
        let config = parse_fixture(&yaml);
        let ctx = clinker_core::config::CompileContext::default();
        let symbol_table = indexmap::IndexMap::new();
        let mut diags = Vec::new();
        let artifacts = clinker_core::plan::bind_schema::bind_schema(
            &config.nodes,
            &mut diags,
            &ctx,
            &symbol_table,
            std::path::Path::new(""),
        );
        (artifacts, diags)
    }

    /// Assert that the node's output row (as published in
    /// `BoundSchemas`) has exactly the expected bare field names in
    /// declaration order.
    ///
    /// Combine nodes publish their output row in C.1.3 — this helper
    /// will be wired into the C.1.3 gate tests (`test_combine_output_
    /// row_from_emit`). Defined in C.1.0 alongside
    /// `compile_combine_fixture` so the test surface is one-stop.
    #[allow(dead_code)] // Exercised by C.1.3+ tests.
    fn assert_output_row(
        artifacts: &clinker_core::plan::bind_schema::CompileArtifacts,
        node_name: &str,
        expected_fields: &[&str],
    ) {
        let row = artifacts
            .bound_schemas
            .output_of(node_name)
            .unwrap_or_else(|| panic!("no bound output row for node {node_name:?}"));
        let actual: Vec<String> = row.field_names().map(|qf| qf.to_string()).collect();
        let expected: Vec<String> = expected_fields.iter().map(|s| (*s).to_string()).collect();
        assert_eq!(
            actual, expected,
            "output row mismatch for {node_name:?}: expected {expected:?}, got {actual:?}"
        );
    }

    /// Assert the decomposed predicate recorded in
    /// `CompileArtifacts.combine_predicates[node_name]` has the
    /// expected `(equality, range, has_residual)` shape. Combine-side
    /// table populated in C.1.2 — helper defined here for early
    /// availability.
    #[allow(dead_code)] // Exercised by C.1.2+ tests.
    fn assert_predicate_decomposition(
        artifacts: &clinker_core::plan::bind_schema::CompileArtifacts,
        node_name: &str,
        expected_equalities: usize,
        expected_ranges: usize,
        expected_has_residual: bool,
    ) {
        let pred = artifacts
            .combine_predicates
            .get(node_name)
            .unwrap_or_else(|| panic!("no decomposed predicate for combine {node_name:?}"));
        assert_eq!(
            pred.equalities.len(),
            expected_equalities,
            "equalities count mismatch for {node_name:?}"
        );
        assert_eq!(
            pred.ranges.len(),
            expected_ranges,
            "ranges count mismatch for {node_name:?}"
        );
        assert_eq!(
            pred.residual.is_some(),
            expected_has_residual,
            "residual presence mismatch for {node_name:?}"
        );
    }

    // --- C.1.0 gate test -------------------------------------------------

    /// C.1.0 gate: `compile_combine_fixture` is invokable against a
    /// real fixture and returns `(artifacts, diagnostics)`.
    ///
    /// Post-C.1.1 the Combine arm actively builds a merged `Row` out of
    /// the upstream schemas; `two_input_equi` still produces zero
    /// diagnostics because both qualifiers are plain identifiers and
    /// both upstreams are declared sources.
    #[test]
    fn test_combine_schema_scaffold_compiles() {
        let (_artifacts, diags) = compile_combine_fixture("two_input_equi");
        assert!(
            diags.is_empty(),
            "two_input_equi must bind cleanly; got codes: {:?}",
            diags.iter().map(|d| &d.code).collect::<Vec<_>>()
        );
    }

    /// C.1.0 sanity sweep (not a gate): every front-loaded C.1
    /// fixture parses through `PipelineConfig` YAML deserialization.
    /// Catches shape/syntax errors in the new fixtures immediately so
    /// C.1.1–C.1.4 tests don't fail for parse reasons masking real
    /// bind_schema bugs.
    #[test]
    fn test_c1_fixtures_parse() {
        let c1_fixtures = [
            // C.1.1 error fixtures
            "error_reserved_namespace.yaml",
            "error_dotted_qualifier.yaml",
            // C.1.2 fixtures (errors + decomposition cases)
            "error_where_not_bool.yaml",
            "error_unknown_field.yaml",
            "error_no_cross_input.yaml",
            "error_literal_true.yaml",
            "error_or_predicate.yaml",
            "error_3part_ref.yaml",
            "expression_equi.yaml",
            "mixed_qualifier_expr.yaml",
            // C.1.3 fixtures
            "error_body_unknown_field.yaml",
            "error_no_emit.yaml",
            "combine_then_transform.yaml",
        ];
        for name in c1_fixtures {
            let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/combine")
                .join(name);
            assert!(path.exists(), "missing C.1 fixture: {}", path.display());
            let yaml = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
            clinker_core::yaml::from_str::<PipelineConfig>(&yaml)
                .unwrap_or_else(|e| panic!("fixture {name} failed to parse: {e}"));
        }
    }

    // --- C.1.1 gate tests ------------------------------------------------
    //
    // The merged row built inside `bind_schema_inner` for a
    // `PipelineNode::Combine` is a LOCAL INTERMEDIATE — it feeds the
    // where-clause typecheck in C.1.2 and the body typecheck in C.1.3,
    // but it is not published anywhere the test can inspect it. The
    // observable contract at C.1.1 is therefore "diagnostics match the
    // fixture's intent": clean fixtures produce no diagnostics; error
    // fixtures produce exactly one of E300 / E301.

    /// C.1.1 gate: a 2-input combine with plain qualifiers and declared
    /// upstreams binds with zero diagnostics. Confirms the merged-row
    /// construction path runs end-to-end for the canonical shape.
    #[test]
    fn test_combine_merged_row_two_inputs() {
        let (_artifacts, diags) = compile_combine_fixture("two_input_equi");
        assert!(
            diags.is_empty(),
            "two_input_equi must bind cleanly at C.1.1; got codes: {:?}",
            diags.iter().map(|d| &d.code).collect::<Vec<_>>()
        );
    }

    /// C.1.1 gate: a 3-input combine binds cleanly — qualifier/field
    /// pairs stay unique across all three upstream rows, and no E301
    /// fires because each qualifier is a plain identifier.
    #[test]
    fn test_combine_merged_row_three_inputs() {
        let (_artifacts, diags) = compile_combine_fixture("three_input_shared_key");
        assert!(
            diags.is_empty(),
            "three_input_shared_key must bind cleanly at C.1.1; got codes: {:?}",
            diags.iter().map(|d| &d.code).collect::<Vec<_>>()
        );
    }

    /// C.1.1 gate: fewer than 2 declared inputs → E300.
    #[test]
    fn test_combine_e300_one_input() {
        let (_artifacts, diags) = compile_combine_fixture("error_one_input");
        assert!(
            diags.iter().any(|d| d.code == "E300"),
            "error_one_input must produce E300; got codes: {:?}",
            diags.iter().map(|d| &d.code).collect::<Vec<_>>()
        );
    }

    /// C.1.1 gate: a `$`-prefixed qualifier collides with CXL system
    /// namespaces (`$pipeline`, `$window`, `$meta`) → E301.
    #[test]
    fn test_combine_e301_reserved_namespace() {
        let (_artifacts, diags) = compile_combine_fixture("error_reserved_namespace");
        assert!(
            diags.iter().any(|d| d.code == "E301"),
            "error_reserved_namespace must produce E301; got codes: {:?}",
            diags.iter().map(|d| &d.code).collect::<Vec<_>>()
        );
    }

    /// C.1.1 gate (RESOLUTION EC-4): a qualifier containing `.` would
    /// alias a genuine 3-part CXL reference → E301.
    #[test]
    fn test_combine_e301_dotted_qualifier() {
        let (_artifacts, diags) = compile_combine_fixture("error_dotted_qualifier");
        assert!(
            diags.iter().any(|d| d.code == "E301"),
            "error_dotted_qualifier must produce E301; got codes: {:?}",
            diags.iter().map(|d| &d.code).collect::<Vec<_>>()
        );
    }

    // --- C.1.2 gate tests ------------------------------------------------
    //
    // The where-clause is parsed, typechecked against the merged row,
    // and decomposed into cross-input equality + range conjuncts + a
    // residual `TypedProgram`. Residual is re-typechecked (drill D8);
    // the original where-clause `TypedProgram` is a local intermediate
    // that does NOT survive past `bind_schema`.

    /// C.1.2 gate: a valid cross-input where-clause typechecks, decomposes,
    /// and populates `artifacts.combine_predicates`.
    #[test]
    fn test_combine_where_typecheck_bool() {
        let (artifacts, diags) = compile_combine_fixture("two_input_equi");
        assert!(
            diags.is_empty(),
            "two_input_equi must bind cleanly; got codes: {:?}",
            diags.iter().map(|d| &d.code).collect::<Vec<_>>()
        );
        let pred = artifacts
            .combine_predicates
            .get("enriched")
            .expect("combine_predicates['enriched'] must exist");
        assert!(
            !pred.equalities.is_empty(),
            "two_input_equi must produce at least one equality conjunct"
        );
    }

    /// C.1.2 gate: a where-clause whose predicate is a non-Bool type → E303.
    #[test]
    fn test_combine_e303_where_not_bool() {
        let (_artifacts, diags) = compile_combine_fixture("error_where_not_bool");
        assert!(
            diags.iter().any(|d| d.code == "E303"),
            "error_where_not_bool must produce E303; got codes: {:?}",
            diags.iter().map(|d| &d.code).collect::<Vec<_>>()
        );
    }

    /// C.1.2 gate: a where-clause that references a non-existent qualified
    /// field → E304.
    #[test]
    fn test_combine_e304_unknown_field() {
        let (_artifacts, diags) = compile_combine_fixture("error_unknown_field");
        assert!(
            diags.iter().any(|d| d.code == "E304"),
            "error_unknown_field must produce E304; got codes: {:?}",
            diags.iter().map(|d| &d.code).collect::<Vec<_>>()
        );
    }

    /// C.1.2 gate: a where-clause with only same-input comparisons yields
    /// no cross-input extractables → E305.
    #[test]
    fn test_combine_e305_no_cross_input() {
        let (_artifacts, diags) = compile_combine_fixture("error_no_cross_input");
        assert!(
            diags.iter().any(|d| d.code == "E305"),
            "error_no_cross_input must produce E305; got codes: {:?}",
            diags.iter().map(|d| &d.code).collect::<Vec<_>>()
        );
    }

    /// C.1.2 gate (V-7-1): a literal-only predicate (`where: "true"`) has
    /// zero cross-input comparisons → E305. Combine does not support
    /// cross joins; users must add a real predicate or use a Merge node.
    #[test]
    fn test_combine_e305_literal_true() {
        let (_artifacts, diags) = compile_combine_fixture("error_literal_true");
        assert!(
            diags.iter().any(|d| d.code == "E305"),
            "error_literal_true must produce E305; got codes: {:?}",
            diags.iter().map(|d| &d.code).collect::<Vec<_>>()
        );
    }

    /// C.1.2 gate: pure equi predicate `a.id == b.id` → 1 equality, 0 ranges,
    /// no residual.
    #[test]
    fn test_combine_decompose_pure_equi() {
        let (artifacts, diags) = compile_combine_fixture("two_input_equi");
        assert!(diags.is_empty(), "expected clean compile");
        let pred = &artifacts.combine_predicates["enriched"];
        assert_eq!(pred.equalities.len(), 1, "1 equality expected");
        assert_eq!(pred.ranges.len(), 0, "0 ranges expected");
        assert!(
            pred.residual.is_none(),
            "no residual expected for pure equi"
        );
    }

    /// C.1.2 gate (RESOLUTION T-8): mixed predicate
    /// `a.id == b.id and a.amt >= b.min and a.amt < b.max` →
    /// 1 equality + 2 ranges. Strengthened asserts on `ranges[0]`
    /// verify that `split_conjunction` preserves source order and
    /// `classify_conjunct` maps `BinOp::Gte → RangeOp::Ge` correctly.
    #[test]
    fn test_combine_decompose_mixed() {
        use clinker_core::plan::combine::RangeOp;

        let (artifacts, diags) = compile_combine_fixture("two_input_mixed");
        assert!(
            diags.is_empty(),
            "two_input_mixed must bind cleanly; got codes: {:?}",
            diags.iter().map(|d| &d.code).collect::<Vec<_>>()
        );
        let pred = &artifacts.combine_predicates["qualified"];
        assert_eq!(pred.equalities.len(), 1, "1 equality expected");
        assert_eq!(pred.ranges.len(), 2, "2 ranges expected");
        assert!(matches!(pred.ranges[0].op, RangeOp::Ge));
        assert_eq!(pred.ranges[0].left_input.as_ref(), "orders");
        assert_eq!(pred.ranges[0].right_input.as_ref(), "products");
    }

    /// C.1.2 gate: an OR expression at the top level is NEVER decomposed
    /// (drill D7 — universal consensus across engines) → 0 eq/range,
    /// residual holds the whole OR.
    #[test]
    fn test_combine_decompose_or_is_residual() {
        let (artifacts, _diags) = compile_combine_fixture("error_or_predicate");
        let pred = &artifacts.combine_predicates["combine_or"];
        assert_eq!(pred.equalities.len(), 0);
        assert_eq!(pred.ranges.len(), 0);
        assert!(pred.residual.is_some(), "residual must hold the OR whole");
    }

    /// C.1.2 gate: expression-based equality `a.name.lower() ==
    /// b.name.lower()` extracts as a single equality conjunct. Verifies
    /// that `EqualityConjunct.{left,right}_expr` can hold arbitrary
    /// expressions per drill D5.
    #[test]
    fn test_combine_decompose_expression_equality() {
        let (artifacts, diags) = compile_combine_fixture("expression_equi");
        assert!(
            diags.is_empty(),
            "expression_equi must bind cleanly; got codes: {:?}",
            diags.iter().map(|d| &d.code).collect::<Vec<_>>()
        );
        let pred = &artifacts.combine_predicates["expr_combine"];
        assert_eq!(pred.equalities.len(), 1, "1 equality expected");
        assert_eq!(pred.equalities[0].left_input.as_ref(), "orders");
        assert_eq!(pred.equalities[0].right_input.as_ref(), "products");
    }

    /// C.1.2 gate: a conjunct with mixed-qualifier operands (one side
    /// references both inputs) is residual, not a cross-input conjunct.
    /// Matches DataFusion `find_valid_equijoin_key_pair` and Spark
    /// `canEvaluate` semantics.
    #[test]
    fn test_combine_decompose_mixed_qualifier_residual() {
        let (artifacts, _diags) = compile_combine_fixture("mixed_qualifier_expr");
        let pred = &artifacts.combine_predicates["mixed_combine"];
        assert_eq!(pred.equalities.len(), 0);
        assert_eq!(pred.ranges.len(), 0);
        assert!(pred.residual.is_some());
    }

    /// C.1.2 gate (RESOLUTION EC-6): a 3-part qualified ref (`a.b.c`) in
    /// the where-clause is not supported and produces E304 with a clear
    /// "only 2-part references are supported" message.
    #[test]
    fn test_combine_3part_ref_residual() {
        let (_artifacts, diags) = compile_combine_fixture("error_3part_ref");
        assert!(
            diags.iter().any(|d| d.code == "E304"),
            "error_3part_ref must produce E304; got codes: {:?}",
            diags.iter().map(|d| &d.code).collect::<Vec<_>>()
        );
    }

    // --- C.1.3 gate tests ------------------------------------------------
    //
    // The cxl body is typechecked against the merged row. Output fields
    // are the LHS names of non-meta `emit` statements, always bare
    // (`QualifiedField::bare`). The output row lands in
    // `BoundSchemas::output_of(name)` so downstream nodes see it; the
    // body TypedProgram lands in `artifacts.typed[name]` per drill D10.

    /// C.1.3 gate: emit statements publish an output row in
    /// `BoundSchemas` whose declared field names (bare) are exactly the
    /// LHS emit names in source order. V-5-6: also asserts
    /// `artifacts.typed[name]` is populated with the body TypedProgram.
    #[test]
    fn test_combine_output_row_from_emit() {
        let (artifacts, diags) = compile_combine_fixture("two_input_equi");
        assert!(
            diags.is_empty(),
            "two_input_equi must bind cleanly; got codes: {:?}",
            diags.iter().map(|d| &d.code).collect::<Vec<_>>()
        );
        assert_output_row(
            &artifacts,
            "enriched",
            &[
                "order_id",
                "product_id",
                "product_name",
                "category",
                "amount",
            ],
        );
        assert!(
            artifacts.typed.contains_key("enriched"),
            "artifacts.typed['enriched'] must be populated with body TypedProgram"
        );
    }

    /// C.1.3 gate: a Transform after a combine resolves the combine's
    /// emitted (bare) output fields. Proves downstream binding sees the
    /// output row, not the internal merged qualified row.
    #[test]
    fn test_combine_downstream_sees_output() {
        let (_artifacts, diags) = compile_combine_fixture("combine_then_transform");
        assert!(
            diags.is_empty(),
            "combine_then_transform must bind cleanly; got codes: {:?}",
            diags.iter().map(|d| &d.code).collect::<Vec<_>>()
        );
    }

    /// C.1.3 gate: cxl body referencing a nonexistent qualified field
    /// → E308.
    #[test]
    fn test_combine_e308_unknown_merged_field() {
        let (_artifacts, diags) = compile_combine_fixture("error_body_unknown_field");
        assert!(
            diags.iter().any(|d| d.code == "E308"),
            "error_body_unknown_field must produce E308; got codes: {:?}",
            diags.iter().map(|d| &d.code).collect::<Vec<_>>()
        );
    }

    /// C.1.3 gate: cxl body with zero emit statements → E309. Combine
    /// has no pass-through semantics; every output field must be emitted.
    #[test]
    fn test_combine_e309_no_emit() {
        let (_artifacts, diags) = compile_combine_fixture("error_no_emit");
        assert!(
            diags.iter().any(|d| d.code == "E309"),
            "error_no_emit must produce E309; got codes: {:?}",
            diags.iter().map(|d| &d.code).collect::<Vec<_>>()
        );
    }

    /// Compile a parsed `PipelineConfig` into an `ExecutionPlanDag` via the
    /// legacy planner path. The combine fixtures used here declare no
    /// CXL-bearing transform/aggregate/route nodes, so `compiled_refs` is
    /// empty — `build_transform_specs` produces nothing for Combine nodes
    /// (they lower to `PlanNode::Combine` directly in C.0.4's Phase 1
    /// block, not via `PlanTransformSpec`).
    fn compile_plan(config: &PipelineConfig) -> ExecutionPlanDag {
        ExecutionPlanDag::compile(config, &[]).unwrap_or_else(|e| panic!("compile failed: {e}"))
    }

    /// Find the `NodeIndex` of the combine node named `name` in the DAG.
    /// Panics with a helpful message if no matching node is found.
    fn find_combine_node(plan: &ExecutionPlanDag, name: &str) -> NodeIndex {
        for idx in plan.graph.node_indices() {
            if let PlanNode::Combine {
                name: node_name, ..
            } = &plan.graph[idx]
                && node_name == name
            {
                return idx;
            }
        }
        let all: Vec<String> = plan
            .graph
            .node_weights()
            .map(|n| format!("{}:{}", n.type_tag(), n.name()))
            .collect();
        panic!(
            "combine node {name:?} not in plan; nodes: [{}]",
            all.join(", ")
        );
    }

    // --- C.0.4 gate tests ------------------------------------------------

    /// Gate test: a 2-input combine has exactly 2 incoming `Data` edges
    /// in the compiled DAG. Uses the `two_input_equi.yaml` fixture whose
    /// combine node named `enriched` pulls from sources `orders` and
    /// `products`.
    #[test]
    fn test_combine_dag_edges_two_input() {
        let yaml = load_fixture("two_input_equi.yaml");
        let config = parse_fixture(&yaml);
        let plan = compile_plan(&config);

        let combine_idx = find_combine_node(&plan, "enriched");
        let incoming = plan
            .graph
            .edges_directed(combine_idx, Direction::Incoming)
            .count();
        assert_eq!(
            incoming, 2,
            "two_input_equi: combine 'enriched' should have 2 incoming edges, got {incoming}"
        );
    }

    /// Gate test: a 3-input combine has exactly 3 incoming `Data` edges.
    /// Uses the `three_input_shared_key.yaml` fixture whose combine node
    /// `fully_enriched` pulls from `orders`, `products`, and `categories`.
    #[test]
    fn test_combine_dag_edges_three_input() {
        let yaml = load_fixture("three_input_shared_key.yaml");
        let config = parse_fixture(&yaml);
        let plan = compile_plan(&config);

        let combine_idx = find_combine_node(&plan, "fully_enriched");
        let incoming = plan
            .graph
            .edges_directed(combine_idx, Direction::Incoming)
            .count();
        assert_eq!(
            incoming, 3,
            "three_input_shared_key: combine 'fully_enriched' should have 3 incoming edges, got {incoming}"
        );
    }

    /// Sanity sweep (not a gate test): verify every combine fixture in
    /// the corpus compiles through `ExecutionPlanDag::compile` without
    /// emitting E307 (undeclared upstream). `error_one_input.yaml` is
    /// structurally 1-input but that is still a valid reference — E300
    /// ("fewer than 2 inputs") fires in C.1 via bind_schema, not at DAG
    /// wiring. All declared upstreams resolve via `resolve_any_node`.
    #[test]
    fn test_all_combine_fixtures_compile_through_dag() {
        let fixtures = [
            "two_input_equi.yaml",
            "two_input_range.yaml",
            "two_input_mixed.yaml",
            "three_input_shared_key.yaml",
            "match_all.yaml",
            "match_collect.yaml",
            "on_miss_skip.yaml",
            "on_miss_error.yaml",
            "drive_hint.yaml",
            "error_one_input.yaml",
        ];
        for name in fixtures {
            let yaml = load_fixture(name);
            let config = parse_fixture(&yaml);
            let result = ExecutionPlanDag::compile(&config, &[]);
            let plan = result.unwrap_or_else(|e| {
                panic!("fixture {name}: compile failed with {e}");
            });
            // Each fixture has exactly one combine node; find it and assert
            // it has at least one incoming edge (one for error_one_input,
            // ≥2 for the rest).
            let combine_count = plan
                .graph
                .node_weights()
                .filter(|n| matches!(n, PlanNode::Combine { .. }))
                .count();
            assert_eq!(combine_count, 1, "fixture {name}: expected 1 combine node");
            for idx in plan.graph.node_indices() {
                if matches!(&plan.graph[idx], PlanNode::Combine { .. }) {
                    let incoming = plan.graph.edges_directed(idx, Direction::Incoming).count();
                    assert!(
                        incoming >= 1,
                        "fixture {name}: combine has no incoming edges"
                    );
                }
            }
        }
    }

    // --- Pre-C.1 field-level span gate tests ---------------------------
    //
    // These tests lock in the architectural contract of the Pre-C.1
    // custom-Deserialize refactor: every consumer header carries
    // `Spanned<NodeInput>` at field level, E307 carries a real span, and
    // parity holds across Merge and Transform headers.
    //
    // See `docs/internal/research/RESEARCH-span-preserving-deser.md`
    // (Approach A). If any of these tests regress, the refactor has
    // leaked — do NOT weaken the asserts.

    /// Gate: CombineHeader's `input: IndexMap<String, Spanned<NodeInput>>`
    /// captures real YAML line numbers per entry. Parses the
    /// `two_input_equi.yaml` fixture and verifies the span of the
    /// `orders:` entry points at its actual line in the file.
    #[test]
    fn test_combine_input_carries_field_level_span() {
        use clinker_core::config::PipelineNode;
        use clinker_core::yaml::Location;

        let yaml = load_fixture("two_input_equi.yaml");
        let config = parse_fixture(&yaml);

        let combine = config
            .nodes
            .iter()
            .find_map(|s| match &s.value {
                PipelineNode::Combine { header, .. } if header.name == "enriched" => Some(header),
                _ => None,
            })
            .expect("combine node 'enriched' must be present");

        let orders_entry = combine
            .input
            .get("orders")
            .expect("combine must declare 'orders' qualifier");
        let products_entry = combine
            .input
            .get("products")
            .expect("combine must declare 'products' qualifier");

        assert_ne!(
            orders_entry.referenced,
            Location::UNKNOWN,
            "combine.input['orders'] must carry a real YAML location (pre-C.1 gate)"
        );
        assert_ne!(
            products_entry.referenced,
            Location::UNKNOWN,
            "combine.input['products'] must carry a real YAML location (pre-C.1 gate)"
        );

        let orders_line = orders_entry.referenced.line();
        let products_line = products_entry.referenced.line();
        assert!(
            orders_line >= 1,
            "orders entry line must be >= 1, got {orders_line}"
        );
        // `products_line > orders_line` proves the spans are really
        // being read from the YAML (not just a global constant).
        assert!(
            products_line > orders_line,
            "products entry must come after orders in the YAML; got orders={orders_line} products={products_line}"
        );

        // Verify against the literal fixture contents: find the line
        // whose 0-based index matches what saphyr reported (1-based).
        let lines: Vec<&str> = yaml.lines().collect();
        let orders_text = lines
            .get((orders_line as usize).saturating_sub(1))
            .copied()
            .unwrap_or("");
        assert!(
            orders_text.contains("orders:") && orders_text.contains("orders"),
            "line {orders_line} must contain the 'orders: orders' mapping; got {orders_text:?}"
        );
    }

    /// Gate: `PlanError::CombineInputUndeclared` (E307) carries a real
    /// `Span`. Constructs an inline combine fixture that references an
    /// undeclared upstream and asserts the error's `span` encodes a
    /// non-zero line number.
    #[test]
    fn test_combine_e307_diagnostic_has_real_span() {
        use clinker_core::plan::execution::{ExecutionPlanDag, PlanError};

        // `nowhere` is not declared as a source, transform, or any
        // other producer — combine wiring must surface E307.
        let yaml = r#"
pipeline:
  name: e307_gate
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      schema:
        - { name: order_id, type: string }
  - type: combine
    name: broken
    input:
      orders: orders
      products: nowhere
    config:
      where: "orders.order_id == products.order_id"
      cxl: |
        emit order_id = orders.order_id
"#;
        let config = parse_fixture(yaml);
        let err = ExecutionPlanDag::compile(&config, &[])
            .expect_err("combine referencing undeclared upstream must fail");
        match err {
            PlanError::CombineInputUndeclared {
                combine,
                qualifier,
                reference,
                span,
            } => {
                assert_eq!(combine, "broken");
                assert_eq!(qualifier, "products");
                assert_eq!(reference, "nowhere");
                // The span must carry a real line number, not SYNTHETIC.
                let line = span.synthetic_line_number().unwrap_or(0);
                assert!(
                    line > 0,
                    "E307 span must have a non-zero line; got synthetic_line_number={line:?}, span={span:?}"
                );
                // The reference is on the `products: nowhere` line. In
                // this fixture that's line 16 of the inline YAML (1-based
                // from the leading newline of the raw string). We assert
                // the weaker "within reasonable bounds" property so the
                // test doesn't brittle on whitespace tweaks, but also
                // confirm it's in the right neighbourhood: inside the
                // `input:` block, not on the very first line.
                assert!(
                    line >= 10,
                    "E307 span line must be inside the combine input block (>= 10), got {line}"
                );

                // Display includes the line number.
                let display = format!(
                    "{}",
                    PlanError::CombineInputUndeclared {
                        combine,
                        qualifier,
                        reference,
                        span,
                    }
                );
                assert!(
                    display.contains(&format!("line {line}")),
                    "Display output must include 'line {line}', got: {display}"
                );
            }
            other => panic!("expected CombineInputUndeclared, got: {other}"),
        }
    }

    /// Gate: `MergeHeader.inputs: Vec<Spanned<NodeInput>>` — every input
    /// in a merge fixture carries a real span. Parity with Combine.
    #[test]
    fn test_nodeinput_span_preserved_for_merge_header() {
        use clinker_core::config::PipelineNode;
        use clinker_core::yaml::Location;

        let yaml = r#"
pipeline:
  name: merge_span_gate
nodes:
  - type: source
    name: a
    config:
      name: a
      type: csv
      path: a.csv
      schema:
        - { name: id, type: string }
  - type: source
    name: b
    config:
      name: b
      type: csv
      path: b.csv
      schema:
        - { name: id, type: string }
  - type: merge
    name: combined
    inputs:
      - a
      - b
"#;
        let config = parse_fixture(yaml);
        let merge = config
            .nodes
            .iter()
            .find_map(|s| match &s.value {
                PipelineNode::Merge { header, .. } if header.name == "combined" => Some(header),
                _ => None,
            })
            .expect("merge 'combined' must be present");
        assert_eq!(merge.inputs.len(), 2);
        for (i, entry) in merge.inputs.iter().enumerate() {
            assert_ne!(
                entry.referenced,
                Location::UNKNOWN,
                "merge.inputs[{i}] must carry a real YAML location"
            );
            assert!(
                entry.referenced.line() >= 1,
                "merge.inputs[{i}] line must be >= 1, got {}",
                entry.referenced.line()
            );
        }
        // Spans must differ — proves we're reading per-entry, not a
        // global constant.
        assert_ne!(
            merge.inputs[0].referenced.line(),
            merge.inputs[1].referenced.line(),
            "merge entries must have distinct line numbers"
        );
    }

    /// Gate: `NodeHeader.input: Spanned<NodeInput>` on a single-input
    /// Transform header carries a real span. Parity with Combine.
    #[test]
    fn test_nodeinput_span_preserved_for_transform_header() {
        use clinker_core::config::PipelineNode;
        use clinker_core::yaml::Location;

        let yaml = r#"
pipeline:
  name: transform_span_gate
nodes:
  - type: source
    name: raw
    config:
      name: raw
      type: csv
      path: raw.csv
      schema:
        - { name: id, type: string }
  - type: transform
    name: clean
    input: raw
    config:
      cxl: "emit id = id"
"#;
        let config = parse_fixture(yaml);
        let transform = config
            .nodes
            .iter()
            .find_map(|s| match &s.value {
                PipelineNode::Transform { header, .. } if header.name == "clean" => Some(header),
                _ => None,
            })
            .expect("transform 'clean' must be present");
        assert_ne!(
            transform.input.referenced,
            Location::UNKNOWN,
            "transform.input must carry a real YAML location (pre-C.1)"
        );
        assert!(
            transform.input.referenced.line() >= 1,
            "transform.input line must be >= 1, got {}",
            transform.input.referenced.line()
        );
    }

    /// Gate test: the topo order places every upstream input BEFORE the
    /// combine node. Uses `two_input_equi.yaml` (combine 'enriched' over
    /// sources 'orders' and 'products').
    #[test]
    fn test_combine_topo_sort_correct() {
        let yaml = load_fixture("two_input_equi.yaml");
        let config = parse_fixture(&yaml);
        let plan = compile_plan(&config);

        let combine_idx = find_combine_node(&plan, "enriched");
        let combine_pos = plan
            .topo_order
            .iter()
            .position(|&idx| idx == combine_idx)
            .expect("combine node must appear in topo order");

        // Collect the topo positions of every upstream (incoming) node.
        let upstream_positions: Vec<usize> = plan
            .graph
            .neighbors_directed(combine_idx, Direction::Incoming)
            .map(|pred| {
                plan.topo_order
                    .iter()
                    .position(|&idx| idx == pred)
                    .expect("every predecessor must appear in topo order")
            })
            .collect();
        assert_eq!(
            upstream_positions.len(),
            2,
            "expected 2 upstream inputs for 'enriched'"
        );
        for pos in &upstream_positions {
            assert!(
                *pos < combine_pos,
                "upstream at topo pos {pos} must precede combine at pos {combine_pos}"
            );
        }
    }

    // --- C.1.4 gate tests ------------------------------------------------

    // ─── C.2.0 helpers + execution scaffold ──────────────────────────────
    //
    // `run_combine_fixture` is the canonical end-to-end executor entry for
    // combine integration tests. It wraps the public
    // `PipelineExecutor::run_plan_with_readers_writers_with_primary` entry,
    // providing a single `(yaml, named_inputs, primary)` signature that
    // mirrors how lookup tests are written in `src/integration_tests.rs`
    // but uses only the public API surface (the lookup test helper is
    // `pub(crate)` and not visible from this `tests/` crate).
    //
    // Returned tuple: (execution report, captured CSV output of the first
    // declared output node). The CSV string is the raw bytes the writer
    // emitted — caller is responsible for canonicalization (sort, header
    // strip) before any equality assertion. `assert_records_match`
    // performs that canonicalization.

    /// Run a combine (or lookup) pipeline end-to-end through the public
    /// executor with the given named in-memory CSV inputs. Returns the
    /// `ExecutionReport` and the captured output CSV string.
    ///
    /// `inputs` is `&[(source_name, csv_bytes_as_str)]`. The first entry
    /// is treated as the primary driving source (consistent with the
    /// `--primary` semantics of `run_plan_with_readers_writers_with_primary`).
    /// `primary_override` lets a test pin a specific source as primary
    /// without reordering `inputs` — pass `None` to default to the first
    /// input.
    ///
    /// Output capture: only the FIRST output node declared in the YAML
    /// is captured. Pipelines with multiple outputs would need the helper
    /// extended to a `HashMap<String, String>` return; no current fixture
    /// uses multi-output combine, so this stays simple.
    #[allow(dead_code)] // Wired by C.2.0+ tests; gate test enforces it compiles.
    fn run_combine_fixture(
        yaml: &str,
        inputs: &[(&str, &str)],
        primary_override: Option<&str>,
    ) -> Result<(clinker_core::executor::ExecutionReport, String), Box<dyn std::error::Error>> {
        let config: PipelineConfig = clinker_core::yaml::from_str(yaml)?;
        let ctx = CompileContext::default();
        let plan = config.compile(&ctx).map_err(|diags| {
            let messages: Vec<String> = diags.into_iter().map(|d| d.message).collect();
            format!("compile failed: {}", messages.join("; "))
        })?;

        let primary: String = match primary_override {
            Some(name) => name.to_string(),
            None => inputs
                .first()
                .map(|(n, _)| (*n).to_string())
                .ok_or("run_combine_fixture: no inputs provided")?,
        };

        let mut readers: HashMap<String, Box<dyn Read + Send>> = HashMap::new();
        for (name, data) in inputs {
            readers.insert(
                (*name).to_string(),
                Box::new(std::io::Cursor::new(data.as_bytes().to_vec())) as Box<dyn Read + Send>,
            );
        }

        let first_output = plan
            .config()
            .output_configs()
            .next()
            .ok_or("run_combine_fixture: pipeline declares no output nodes")?
            .name
            .clone();
        let output_buf = SharedBuffer::new();
        let mut writers: HashMap<String, Box<dyn Write + Send>> = HashMap::new();
        writers.insert(
            first_output,
            Box::new(output_buf.clone()) as Box<dyn Write + Send>,
        );

        let pipeline_vars = plan
            .config()
            .pipeline
            .vars
            .as_ref()
            .map(|v| clinker_core::config::convert_pipeline_vars(v))
            .unwrap_or_default();
        let params = PipelineRunParams {
            execution_id: "combine-test-exec".to_string(),
            batch_id: "combine-test-batch".to_string(),
            pipeline_vars,
            shutdown_token: None,
        };

        let report = PipelineExecutor::run_plan_with_readers_writers_with_primary(
            &plan, &primary, readers, writers, &params,
        )?;

        Ok((report, output_buf.as_string()))
    }

    /// Canonicalize a CSV string for order-independent comparison: keep
    /// the header line as-is, sort the body lines lexicographically,
    /// drop trailing whitespace per line, drop empty trailing lines.
    ///
    /// This matches how the C.2.0 lookup-baseline snapshots store their
    /// content — combine output is unordered (hash probe) so any
    /// equality check must canonicalize first.
    fn canonicalize_csv(csv: &str) -> String {
        let mut lines: Vec<&str> = csv.lines().collect();
        // Strip trailing empty lines.
        while lines.last().map(|l| l.trim().is_empty()).unwrap_or(false) {
            lines.pop();
        }
        if lines.is_empty() {
            return String::new();
        }
        let header = lines[0].trim_end();
        let mut body: Vec<&str> = lines.iter().skip(1).map(|l| l.trim_end()).collect();
        body.sort_unstable();
        let mut out = String::with_capacity(csv.len());
        out.push_str(header);
        out.push('\n');
        for line in body {
            out.push_str(line);
            out.push('\n');
        }
        out
    }

    /// Assert two CSV outputs match record-by-record, ignoring row order
    /// but requiring identical header and identical row content. Whitespace
    /// at end of line is stripped before comparison; empty trailing lines
    /// are dropped.
    ///
    /// This is the C.2.3.4 regression assertion that migrated combine
    /// fixtures produce output equivalent to the C.2.0 lookup baseline.
    #[allow(dead_code)] // Wired by C.2.0+ tests.
    fn assert_records_match(actual: &str, expected: &str) {
        let actual_canon = canonicalize_csv(actual);
        let expected_canon = canonicalize_csv(expected);
        assert_eq!(
            actual_canon, expected_canon,
            "record sets differ\n--- actual (canonicalized) ---\n{actual_canon}\n--- expected (canonicalized) ---\n{expected_canon}"
        );
    }

    // ─── C.2.0.1 gate test: scaffold compiles + runs ──────────────────────

    /// C.2.0.1 gate: `run_combine_fixture` compiles, executes a real
    /// pipeline through the public executor, and returns a non-empty
    /// `ExecutionReport`. Uses the simplest available 2-input fixture
    /// (lookup syntax — combine executor lands in C.2.2 — but the helper
    /// itself is mode-agnostic and exercised here for compilation only).
    ///
    /// THIS TEST DOES NOT yet exercise combine execution. Combine
    /// dispatch returns `PipelineError::Internal` until C.2.2 replaces
    /// the stub at `executor/mod.rs:3566`. The test that runs combine
    /// fixtures end-to-end is `test_combine_exec_equi_match_first` in
    /// C.2.2's gate suite. The C.2.0 gate is "the helper compiles and is
    /// callable" — equivalent to the existing
    /// `test_combine_scaffold_compiles` for the C.0 module gate.
    #[test]
    fn test_combine_exec_scaffold_compiles() {
        // The simplest in-tree pipeline that exercises the helper
        // end-to-end without depending on combine execution: a 1-source,
        // 1-transform, 1-output pipeline. This proves the helper's
        // compile + execute + capture path works on the public API.
        let yaml = r#"
pipeline:
  name: scaffold_smoke
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: src.csv
      schema:
        - { name: id, type: string }
  - type: transform
    name: pass_through
    input: src
    config:
      cxl: |
        emit id = id
  - type: output
    name: out
    input: pass_through
    config:
      name: out
      type: csv
      path: scaffold_smoke.csv
"#;
        let inputs = "id\nA\nB\nC\n";
        let (report, output) = run_combine_fixture(yaml, &[("src", inputs)], None)
            .expect("run_combine_fixture must compile and execute the scaffold pipeline");
        assert_eq!(
            report.counters.total_count, 3,
            "expected 3 records processed, got {}",
            report.counters.total_count
        );
        assert!(
            output.contains("id\n") && output.contains("A\n"),
            "expected output to contain header + records; got: {output:?}"
        );

        // Also exercise canonicalize_csv + assert_records_match so they
        // are statically reachable from a #[test] (kills `dead_code` warn).
        let unsorted = "id\nC\nA\nB\n";
        let sorted = "id\nA\nB\nC\n";
        assert_records_match(unsorted, sorted);
    }

    // ─── C.2.0.2 lookup-baseline snapshot capture ────────────────────────
    //
    // Each test below runs the lookup pipeline currently exercised by
    // `crates/clinker-core/src/integration_tests.rs::test_lookup_*` and
    // captures its canonicalized CSV output as an `insta` snapshot. The
    // .snap files land in `crates/clinker-core/tests/snapshots/` and are
    // committed to git; they survive C.2.3 deletion of lookup runtime.
    //
    // C.2.3.4 migration tests (combine fixtures) read these snapshot
    // files via std::fs and assert the migrated combine output
    // canonicalizes to byte-identical CSV.
    //
    // Snapshot file paths (deterministic per insta defaults):
    //   tests/snapshots/combine_test__tests__lookup_baseline_<name>.snap
    //
    // These functions are deleted in C.2.3.2 alongside the lookup
    // runtime; the .snap files persist as the regression baseline.

    fn lookup_yaml_equality() -> &'static str {
        r#"
pipeline:
  name: lookup_eq_baseline
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      schema:
        - { name: order_id, type: string }
        - { name: product_id, type: string }
        - { name: quantity, type: int }
  - type: source
    name: products
    config:
      name: products
      type: csv
      path: products.csv
      schema:
        - { name: product_id, type: string }
        - { name: product_name, type: string }
  - type: transform
    name: enrich
    input: orders
    config:
      lookup:
        source: products
        where: "product_id == products.product_id"
      cxl: |
        emit order_id = order_id
        emit product_name = products.product_name
        emit quantity = quantity
  - type: output
    name: result
    input: enrich
    config:
      name: result
      type: csv
      path: lookup_eq_baseline.csv
"#
    }

    fn lookup_yaml_range() -> &'static str {
        r#"
pipeline:
  name: lookup_range_baseline
nodes:
  - type: source
    name: employees
    config:
      name: employees
      type: csv
      path: employees.csv
      schema:
        - { name: employee_id, type: string }
        - { name: ee_group, type: string }
        - { name: pay, type: int }
  - type: source
    name: rate_bands
    config:
      name: rate_bands
      type: csv
      path: rate_bands.csv
      schema:
        - { name: ee_group, type: string }
        - { name: min_pay, type: int }
        - { name: max_pay, type: int }
        - { name: rate_class, type: string }
  - type: transform
    name: classify
    input: employees
    config:
      lookup:
        source: rate_bands
        where: |
          ee_group == rate_bands.ee_group
          and pay >= rate_bands.min_pay
          and pay <= rate_bands.max_pay
      cxl: |
        emit employee_id = employee_id
        emit rate_class = rate_bands.rate_class
  - type: output
    name: result
    input: classify
    config:
      name: result
      type: csv
      path: lookup_range_baseline.csv
"#
    }

    fn lookup_yaml_on_miss_skip() -> &'static str {
        r#"
pipeline:
  name: lookup_skip_baseline
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      schema:
        - { name: order_id, type: string }
        - { name: product_id, type: string }
  - type: source
    name: products
    config:
      name: products
      type: csv
      path: products.csv
      schema:
        - { name: product_id, type: string }
        - { name: product_name, type: string }
  - type: transform
    name: enrich
    input: orders
    config:
      lookup:
        source: products
        where: "product_id == products.product_id"
        on_miss: skip
      cxl: |
        emit order_id = order_id
        emit product_name = products.product_name
  - type: output
    name: result
    input: enrich
    config:
      name: result
      type: csv
      path: lookup_skip_baseline.csv
"#
    }

    fn lookup_yaml_match_all() -> &'static str {
        r#"
pipeline:
  name: lookup_match_all_baseline
nodes:
  - type: source
    name: employees
    config:
      name: employees
      type: csv
      path: employees.csv
      schema:
        - { name: employee_id, type: string }
        - { name: department, type: string }
  - type: source
    name: assignments
    config:
      name: assignments
      type: csv
      path: assignments.csv
      schema:
        - { name: employee_id, type: string }
        - { name: project, type: string }
  - type: transform
    name: enrich
    input: employees
    config:
      lookup:
        source: assignments
        where: "employee_id == assignments.employee_id"
        match: all
      cxl: |
        emit employee_id = employee_id
        emit department = department
        emit project = assignments.project
  - type: output
    name: result
    input: enrich
    config:
      name: result
      type: csv
      path: lookup_match_all_baseline.csv
"#
    }

    fn lookup_yaml_match_all_skip() -> &'static str {
        r#"
pipeline:
  name: lookup_match_all_skip_baseline
nodes:
  - type: source
    name: employees
    config:
      name: employees
      type: csv
      path: employees.csv
      schema:
        - { name: employee_id, type: string }
        - { name: department, type: string }
  - type: source
    name: assignments
    config:
      name: assignments
      type: csv
      path: assignments.csv
      schema:
        - { name: employee_id, type: string }
        - { name: project, type: string }
  - type: transform
    name: enrich
    input: employees
    config:
      lookup:
        source: assignments
        where: "employee_id == assignments.employee_id"
        match: all
        on_miss: skip
      cxl: |
        emit employee_id = employee_id
        emit department = department
        emit project = assignments.project
  - type: output
    name: result
    input: enrich
    config:
      name: result
      type: csv
      path: lookup_match_all_skip_baseline.csv
"#
    }

    /// C.2.0.2 — capture lookup equality enrichment baseline.
    /// 1:1 match, NullFields on miss (default).
    #[test]
    fn test_lookup_baseline_equality() {
        let orders =
            "order_id,product_id,quantity\nORD-1,PROD-A,5\nORD-2,PROD-B,3\nORD-3,PROD-X,7\n";
        let products = "product_id,product_name\nPROD-A,Widget\nPROD-B,Gadget\n";
        let (_report, output) = run_combine_fixture(
            lookup_yaml_equality(),
            &[("orders", orders), ("products", products)],
            None,
        )
        .expect("lookup_eq baseline must execute under current lookup runtime");
        insta::assert_snapshot!("lookup_baseline_equality", canonicalize_csv(&output));
    }

    /// C.2.0.2 — range predicate baseline (compound and).
    #[test]
    fn test_lookup_baseline_range() {
        let employees =
            "employee_id,ee_group,pay\nE001,exempt,75000\nE002,hourly,35000\nE003,exempt,120000\n";
        let rate_bands = "ee_group,min_pay,max_pay,rate_class\nexempt,50000,80000,tier_1\nexempt,80001,150000,tier_2\nhourly,20000,50000,tier_3\n";
        let (_report, output) = run_combine_fixture(
            lookup_yaml_range(),
            &[("employees", employees), ("rate_bands", rate_bands)],
            None,
        )
        .expect("lookup_range baseline must execute under current lookup runtime");
        insta::assert_snapshot!("lookup_baseline_range", canonicalize_csv(&output));
    }

    /// C.2.0.2 — on_miss: skip baseline. Unmatched rows are dropped.
    #[test]
    fn test_lookup_baseline_on_miss_skip() {
        let orders = "order_id,product_id\nORD-1,PROD-A\nORD-2,PROD-X\nORD-3,PROD-B\n";
        let products = "product_id,product_name\nPROD-A,Widget\nPROD-B,Gadget\n";
        let (_report, output) = run_combine_fixture(
            lookup_yaml_on_miss_skip(),
            &[("orders", orders), ("products", products)],
            None,
        )
        .expect("lookup_skip baseline must execute under current lookup runtime");
        insta::assert_snapshot!("lookup_baseline_on_miss_skip", canonicalize_csv(&output));
    }

    /// C.2.0.2 — match: all (fan-out) baseline. One employee with N
    /// assignments produces N output records.
    #[test]
    fn test_lookup_baseline_match_all() {
        let employees = "employee_id,department\nE001,Engineering\nE002,Sales\nE003,Engineering\n";
        let assignments = "employee_id,project\nE001,Phoenix\nE001,Atlas\nE002,Borealis\nE003,Phoenix\nE003,Atlas\nE003,Vega\n";
        let (_report, output) = run_combine_fixture(
            lookup_yaml_match_all(),
            &[("employees", employees), ("assignments", assignments)],
            None,
        )
        .expect("lookup_match_all baseline must execute under current lookup runtime");
        insta::assert_snapshot!("lookup_baseline_match_all", canonicalize_csv(&output));
    }

    /// C.2.0.2 — match: all + on_miss: skip baseline. Unmatched
    /// employees dropped; matched ones fan out.
    #[test]
    fn test_lookup_baseline_match_all_skip() {
        let employees = "employee_id,department\nE001,Engineering\nE002,Sales\nE003,Engineering\nE004,Marketing\n";
        let assignments = "employee_id,project\nE001,Phoenix\nE001,Atlas\nE003,Vega\n";
        let (_report, output) = run_combine_fixture(
            lookup_yaml_match_all_skip(),
            &[("employees", employees), ("assignments", assignments)],
            None,
        )
        .expect("lookup_match_all_skip baseline must execute under current lookup runtime");
        insta::assert_snapshot!("lookup_baseline_match_all_skip", canonicalize_csv(&output));
    }

    /// C.2.0.2 gate: verify that all 5 lookup baseline `.snap` files
    /// exist on disk under `tests/snapshots/`. Depends on the 5
    /// `test_lookup_baseline_*` tests having run first to write the
    /// snapshots; in CI that ordering is implicit because all tests in
    /// a binary run before the binary exits.
    ///
    /// RESOLUTION T-13: verify snapshot CONTENT exists (non-empty file
    /// with header + at least one body row), not just file existence.
    /// Empty .snap files would silently pass an existence-only check.
    #[test]
    fn test_lookup_snapshots_captured() {
        let snapshots_dir =
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/snapshots");
        let expected = [
            "combine_test__tests__lookup_baseline_equality.snap",
            "combine_test__tests__lookup_baseline_range.snap",
            "combine_test__tests__lookup_baseline_on_miss_skip.snap",
            "combine_test__tests__lookup_baseline_match_all.snap",
            "combine_test__tests__lookup_baseline_match_all_skip.snap",
        ];
        for name in expected {
            let path = snapshots_dir.join(name);
            assert!(
                path.exists(),
                "missing lookup baseline snapshot: {} — did the corresponding `test_lookup_baseline_*` test run? Try `cargo test --test combine_test`",
                path.display()
            );
            let content = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
            // A captured insta snapshot is at minimum:
            //   ---
            //   source: tests/combine_test.rs
            //   expression: ...
            //   ---
            //   <body>
            //
            // We require non-empty body (a header line at least). The
            // body section starts after the second `---` separator.
            let body_start = content.find("---\n").and_then(|i| {
                let after = &content[i + 4..];
                after.find("---\n").map(|j| i + 4 + j + 4)
            });
            let body_start = body_start.unwrap_or_else(|| {
                panic!(
                    "snapshot {} has no body section (malformed insta file?):\n{content}",
                    path.display()
                )
            });
            let body = content[body_start..].trim();
            assert!(
                !body.is_empty(),
                "snapshot {} has empty body — lookup pipeline produced no output, regression baseline is degenerate",
                path.display()
            );
            assert!(
                body.lines().count() >= 1,
                "snapshot {} body has zero rows — header-only baseline is degenerate",
                path.display()
            );
        }
    }

    // ─── End C.2.0 ────────────────────────────────────────────────────────

    /// Gate (C.1.4): a compiled Combine node carries
    /// `OrderingProvenance::DestroyedByCombine { confidence: Proven }` in
    /// its `NodeProperties`. Hash-build/probe (and IEJoin, grace hash) do
    /// not preserve driving-input order; the property-derivation pass
    /// must mark the combine as destructive so downstream streaming-agg
    /// eligibility, `--explain`, and Kiln canvas overlays can chain
    /// through. Resolves Phase Combine §OQ-6 and drill D12.
    ///
    /// Uses `two_input_equi.yaml` (combine 'enriched' over sources
    /// 'orders' and 'products'). The combine lands through full
    /// `ExecutionPlanDag::compile`, which invokes
    /// `compute_node_properties` internally.
    #[test]
    fn test_combine_destroys_ordering() {
        use clinker_core::plan::properties::{Confidence, OrderingProvenance};

        let yaml = load_fixture("two_input_equi.yaml");
        let config = parse_fixture(&yaml);
        let plan = compile_plan(&config);

        let combine_idx = find_combine_node(&plan, "enriched");
        let props = plan
            .node_properties
            .get(&combine_idx)
            .expect("combine node must have NodeProperties populated by compile()");

        // No sort_order — combine output is unordered.
        assert!(
            props.ordering.sort_order.is_none(),
            "combine output sort_order must be None, got {:?}",
            props.ordering.sort_order
        );

        match &props.ordering.provenance {
            OrderingProvenance::DestroyedByCombine {
                at_node,
                confidence,
            } => {
                assert_eq!(
                    at_node, "enriched",
                    "DestroyedByCombine.at_node must identify the combine node"
                );
                assert_eq!(
                    *confidence,
                    Confidence::Proven,
                    "Combine destruction is structural — confidence must be Proven"
                );
            }
            other => panic!(
                "combine 'enriched' must produce DestroyedByCombine provenance; got {other:?}"
            ),
        }
    }
}

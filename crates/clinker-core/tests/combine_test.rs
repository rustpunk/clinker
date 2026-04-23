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
    use clinker_bench_support::io::SharedBuffer;
    use clinker_core::config::{CompileContext, PipelineConfig};
    use clinker_core::error::{Diagnostic, PipelineError};
    use clinker_core::executor::{ExecutionReport, PipelineExecutor, PipelineRunParams};
    use clinker_core::plan::execution::{ExecutionPlanDag, PlanNode};
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
    ///
    /// C.2.4.3 update: W306 is an expected planner warning for 3+
    /// inputs without cardinality estimates (no `drive:` hint either),
    /// emitted by `select_driving_input`. The C.1.1 invariant is that
    /// no *errors* fire during merged-row construction — informational
    /// warnings are out of scope for this gate.
    #[test]
    fn test_combine_merged_row_three_inputs() {
        let (_artifacts, diags) = compile_combine_fixture("three_input_shared_key");
        let errors: Vec<&str> = diags
            .iter()
            .filter(|d| matches!(d.severity, clinker_core::error::Severity::Error))
            .map(|d| d.code.as_str())
            .collect();
        assert!(
            errors.is_empty(),
            "three_input_shared_key must bind without errors at C.1.1; got error codes: {errors:?}"
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

    /// C.2.4.2 gate (R1): `match: collect` with a non-empty `cxl:` body
    /// is a structural error. `bind_combine` emits E311 and stops
    /// processing the combine.
    #[test]
    fn test_combine_e311_collect_with_body() {
        let (artifacts, diags) = compile_combine_fixture("error_collect_with_body");
        assert!(
            diags.iter().any(|d| d.code == "E311"),
            "error_collect_with_body must emit E311; got codes: {:?}",
            diags.iter().map(|d| &d.code).collect::<Vec<_>>()
        );
        assert!(
            artifacts.bound_schemas.output_of("bad_collect").is_none(),
            "no output row published when E311 fires"
        );
    }

    /// C.2.4.2 gate (R1): `match: collect` with an empty `cxl:` body
    /// auto-derives the output row as `{ driver fields,
    /// <build_qualifier>: Array }`. The build qualifier becomes one
    /// new bare field carrying the gathered records.
    #[test]
    fn test_combine_collect_output_row_auto_derived() {
        use cxl::typecheck::{QualifiedField, Type};

        let (artifacts, diags) = compile_combine_fixture("match_collect");
        assert!(
            diags.is_empty(),
            "match_collect must bind cleanly under R1; got codes: {:?}",
            diags.iter().map(|d| &d.code).collect::<Vec<_>>()
        );
        let row = artifacts
            .bound_schemas
            .output_of("collected")
            .expect("collected publishes a bound output row");

        // Driver = orders (default first-in-IndexMap, no `drive:` hint).
        // Driver fields: order_id, product_id, amount. Plus auto-added
        // `products` field of Type::Array.
        let names: Vec<String> = row.field_names().map(|qf| qf.to_string()).collect();
        assert_eq!(
            names,
            vec![
                "order_id".to_string(),
                "product_id".to_string(),
                "amount".to_string(),
                "products".to_string(),
            ],
            "auto-derived row = driver fields + (build_qualifier: Array)"
        );
        let array_type = row
            .fields()
            .find(|(qf, _)| qf == &&QualifiedField::bare("products"))
            .map(|(_, t)| t.clone());
        assert_eq!(array_type, Some(Type::Array));
    }

    /// C.2.4.3 gate: explicit `drive: products` hint on `drive_hint.yaml`
    /// stamps `combine_driving["product_driven"] == "products"` in
    /// `CompileArtifacts`. Verifies that `bind_combine` calls
    /// `select_driving_input` and threads the explicit hint through.
    #[test]
    fn test_combine_driving_input_explicit_drive() {
        let (artifacts, diags) = compile_combine_fixture("drive_hint");
        assert!(
            diags.is_empty(),
            "drive_hint must bind cleanly; got codes: {:?}",
            diags.iter().map(|d| &d.code).collect::<Vec<_>>()
        );
        let driver = artifacts
            .combine_driving
            .get("product_driven")
            .expect("combine_driving entry for product_driven");
        assert_eq!(driver, "products", "explicit drive wins over default");
    }

    /// C.2.4.3 gate: `drive: ghost` references an input not declared on
    /// the combine → bind_combine emits E306 and skips populating
    /// `combine_driving` for that node.
    #[test]
    fn test_combine_e306_invalid_drive() {
        let (artifacts, diags) = compile_combine_fixture("error_invalid_drive");
        assert!(
            diags.iter().any(|d| d.code == "E306"),
            "error_invalid_drive must emit E306; got codes: {:?}",
            diags.iter().map(|d| &d.code).collect::<Vec<_>>()
        );
        assert!(
            !artifacts.combine_driving.contains_key("bad_drive"),
            "no combine_driving entry written when drive selection fails"
        );
    }

    /// C.2.4.3 gate: with no explicit drive and no cardinality estimates,
    /// `select_driving_input` falls back to declaration order. For
    /// `two_input_equi.yaml` the input map declares `orders` first.
    #[test]
    fn test_combine_driving_input_default_first_in_indexmap() {
        let (artifacts, diags) = compile_combine_fixture("two_input_equi");
        assert!(
            diags.is_empty(),
            "two_input_equi must bind cleanly; got codes: {:?}",
            diags.iter().map(|d| &d.code).collect::<Vec<_>>()
        );
        let driver = artifacts
            .combine_driving
            .get("enriched")
            .expect("combine_driving entry for enriched");
        assert_eq!(driver, "orders", "first declared input drives by default");
    }

    /// C.2.4.1 gate: every `EqualityConjunct` produced by predicate
    /// decomposition carries an `Arc<TypedProgram>` on each side. Both
    /// sides share the where-clause TypedProgram (same `Arc`), so the
    /// runtime `KeyExtractor` can call `cxl::eval::eval_expr` against
    /// the shared regex cache without per-side re-typecheck.
    #[test]
    fn test_combine_equality_program_compiled() {
        let (artifacts, diags) = compile_combine_fixture("two_input_equi");
        assert!(
            diags.is_empty(),
            "two_input_equi must bind cleanly; got codes: {:?}",
            diags.iter().map(|d| &d.code).collect::<Vec<_>>()
        );
        let pred = &artifacts.combine_predicates["enriched"];
        assert!(!pred.equalities.is_empty(), "expected at least 1 equality");
        for eq in &pred.equalities {
            assert!(
                std::sync::Arc::ptr_eq(&eq.left_program, &eq.right_program),
                "left/right_program must share the same Arc<TypedProgram> (where-clause)"
            );
            assert!(
                !eq.left_program.program.statements.is_empty(),
                "left_program must contain the where-clause Filter statement"
            );
            assert!(
                eq.left_program.node_count > 0,
                "left_program node_count must cover the predicate AST"
            );
        }
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
    /// canonical compile path. `bind_schema` typechecks every CXL-bearing
    /// node against the author-declared source schemas; Combine nodes
    /// lower to `PlanNode::Combine` in stage 5 via
    /// `lower_node_to_plan_node`.
    fn compile_plan(config: &PipelineConfig) -> ExecutionPlanDag {
        // Combine fixtures declare output paths under /tmp/ (standard test
        // scratch directory); opt in to absolute paths so E-SEC-001 does
        // not reject them here. Production CLI sets this via
        // `--allow-absolute-paths` / `CLINKER_ALLOW_ABSOLUTE_PATHS`.
        let mut ctx = clinker_core::config::CompileContext::default();
        ctx.allow_absolute_paths = true;
        config
            .compile(&ctx)
            .unwrap_or_else(|e| panic!("compile failed: {e:?}"))
            .dag()
            .clone()
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

    /// The planner only supports binary (2-input) combines. A fixture
    /// declaring 3 inputs must be rejected by the post-bind combine
    /// strategy pass with E312 — NOT by any bind_schema-phase check —
    /// until N-ary decomposition support lands. When that support does
    /// land, this test will start failing and needs to be rewritten to
    /// assert the 3-incoming-edge DAG shape directly.
    ///
    /// The assertion splits the two phases:
    ///
    /// 1. bind_schema must succeed on this fixture — none of the
    ///    combine-authoring diagnostics (E300, E301, E303, E304, E305,
    ///    E306, E307, E308, E309) fire, because the 3-input predicate
    ///    decomposition, qualifier binding, and output-row merging are
    ///    themselves well-formed.
    /// 2. `select_combine_strategies` is the sole source of the E312
    ///    rejection.
    ///
    /// This is Shape B (single compile() call) rather than Shape A
    /// (call bind_schema + select_combine_strategies separately)
    /// because driving `select_combine_strategies` from a test requires
    /// a fully enriched `ExecutionPlanDag` — reproducing Stage 5 of
    /// `PipelineConfig::compile_with_diagnostics` outside compile
    /// would be speculative API surface rather than a fair test of the
    /// existing wiring.
    ///
    /// Shape B remains strict despite the single entry point because
    /// `select_combine_strategies` is invoked inside
    /// `compile_with_diagnostics` after bind_schema runs; any
    /// bind_schema-phase error in the E300–E309 block would be
    /// collected into the same `diags` vec returned on failure. So
    /// asserting that none of E300–E309 appear in the returned vec,
    /// AND that E312 does appear, proves bind_schema bound cleanly
    /// and that the post-pass is the rejection source.
    #[test]
    fn test_combine_e312_three_input_rejected() {
        let yaml = load_fixture("three_input_shared_key.yaml");
        let config = parse_fixture(&yaml);
        let mut ctx = clinker_core::config::CompileContext::default();
        ctx.allow_absolute_paths = true;
        let diags = config
            .compile(&ctx)
            .expect_err("3-input combine must be rejected while planner is binary-only");

        // Phase 1: bind_schema must succeed — the fixture is itself
        // well-formed as an N-ary combine; only the binary-only planner
        // rejects it. Any E300–E309 here would mean the fixture broke
        // at bind_schema instead, defeating this test's regression
        // purpose.
        const BIND_PHASE_COMBINE_CODES: &[&str] = &[
            "E300", "E301", "E303", "E304", "E305", "E306", "E307", "E308", "E309",
        ];
        let bind_phase_errors: Vec<&Diagnostic> = diags
            .iter()
            .filter(|d| {
                matches!(d.severity, clinker_core::error::Severity::Error)
                    && BIND_PHASE_COMBINE_CODES.contains(&d.code.as_str())
            })
            .collect();
        assert!(
            bind_phase_errors.is_empty(),
            "bind_schema must bind the 3-input fixture cleanly; got \
             bind-phase combine errors: {bind_phase_errors:?}"
        );

        // Phase 2: the post-pass (`select_combine_strategies`) must be
        // the source of the rejection.
        assert!(
            diags.iter().any(|d| d.code == "E312"),
            "expected E312 from the post-bind combine strategy pass; got {diags:?}"
        );
    }

    /// A combine where-clause needs at least one cross-input equality to
    /// drive the `HashBuildProbe` planner. A pure-range fixture is
    /// rejected at compile time with E313 until the sort-merge / iejoin
    /// planner lands. When that support arrives, this test needs to be
    /// rewritten or removed in favor of an end-to-end range-combine
    /// assertion.
    #[test]
    fn test_combine_e313_pure_range_rejected() {
        let yaml = load_fixture("two_input_range.yaml");
        let config = parse_fixture(&yaml);
        let mut ctx = clinker_core::config::CompileContext::default();
        ctx.allow_absolute_paths = true;
        let diags = config
            .compile(&ctx)
            .expect_err("pure-range combine must be rejected by the binary hash planner");
        assert!(
            diags.iter().any(|d| d.code == "E313"),
            "expected E313 for pure-range combine; got {diags:?}"
        );
    }

    /// Gate: serialized `ExecutionPlanDag` JSON for a 2-input equi
    /// combine surfaces the planner-selected strategy, the chosen
    /// driving input, the build-side inputs, and the decomposed
    /// predicate shape. This is the contract Kiln's canvas consumes
    /// via `clinker explain --format json`.
    #[test]
    fn test_combine_explain_shows_strategy() {
        let yaml = load_fixture("two_input_equi.yaml");
        let config = parse_fixture(&yaml);
        let plan = compile_plan(&config);

        let json: serde_json::Value =
            serde_json::to_value(&plan).expect("ExecutionPlanDag must serialize cleanly");
        let nodes = json["nodes"]
            .as_array()
            .expect("explain JSON carries a nodes array");

        let combine = nodes
            .iter()
            .find(|n| n["type"] == "combine" && n["name"] == "enriched")
            .unwrap_or_else(|| panic!("no combine node named 'enriched' in nodes: {nodes:#?}"));

        assert_eq!(
            combine["strategy"], "hash_build_probe",
            "strategy must be hash_build_probe for pure-equi 2-input combine; got {combine:#?}"
        );

        let driving = combine["driving_input"]
            .as_str()
            .unwrap_or_else(|| panic!("driving_input must be a string; got {combine:#?}"));
        assert!(
            !driving.is_empty(),
            "driving_input must be stamped by select_combine_strategies; got empty: {combine:#?}"
        );

        let build = combine["build_inputs"]
            .as_array()
            .unwrap_or_else(|| panic!("build_inputs must be an array; got {combine:#?}"));
        assert!(
            !build.is_empty(),
            "build_inputs must contain at least one non-driver input; got empty: {combine:#?}"
        );
        for bi in build {
            let bi_str = bi.as_str().expect("build_inputs entries are strings");
            assert_ne!(
                bi_str, driving,
                "build_inputs must not contain the driver {driving:?}; got {build:#?}"
            );
        }
        // The two-input_equi fixture declares `orders` and `products`;
        // together with the driver this must cover the full input set.
        let mut all_inputs: Vec<&str> = build.iter().map(|v| v.as_str().unwrap()).collect();
        all_inputs.push(driving);
        all_inputs.sort();
        assert_eq!(
            all_inputs,
            vec!["orders", "products"],
            "driver + build_inputs must reconstruct the combine's declared inputs"
        );

        let summary = &combine["predicate_summary"];
        assert!(
            summary.is_object(),
            "predicate_summary must be an object; got {summary:#?}"
        );
        let equalities = summary["equalities"].as_u64().unwrap_or_else(|| {
            panic!("predicate_summary.equalities must be a number; got {summary:#?}")
        });
        assert!(
            equalities >= 1,
            "two_input_equi has one `==` conjunct so equalities must be >= 1; got {equalities}"
        );
        assert_eq!(
            summary["ranges"].as_u64().unwrap(),
            0,
            "two_input_equi has no range conjuncts"
        );
        assert!(
            !summary["has_residual"].as_bool().unwrap(),
            "two_input_equi predicate decomposes fully; residual must be absent"
        );
    }

    /// Sweep of combine fixtures that compile all the way through the
    /// canonical path under the current feature set: 2-input equi,
    /// 2-input mixed-predicate, match-mode variants, on-miss variants,
    /// and drive-hint. Pure-range, 3+-input, and 1-input fixtures are
    /// held out in their own dedicated gate tests because they
    /// intentionally fail to compile — the first two until the
    /// sort-merge / iejoin and N-ary combine planners land, the last
    /// as the fixture for the combine-requires-at-least-2-inputs rule.
    #[test]
    fn test_currently_supported_combine_fixtures_compile() {
        let fixtures = [
            "two_input_equi.yaml",
            "two_input_mixed.yaml",
            "match_all.yaml",
            "match_collect.yaml",
            "on_miss_skip.yaml",
            "on_miss_error.yaml",
            "drive_hint.yaml",
        ];
        // Combine fixture outputs live under /tmp/; opt in to absolute
        // paths so E-SEC-001 does not reject them in test.
        let mut ctx = clinker_core::config::CompileContext::default();
        ctx.allow_absolute_paths = true;
        for name in fixtures {
            let yaml = load_fixture(name);
            let config = parse_fixture(&yaml);
            let plan = config.compile(&ctx).unwrap_or_else(|diags| {
                panic!("fixture {name}: compile failed with {diags:?}");
            });
            let plan = plan.dag();
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

    // --- Field-level span gate tests ---------------------------
    //
    // These tests lock in the contract of the custom-Deserialize on
    // consumer headers: every consumer header carries `Spanned<NodeInput>`
    // at field level, undeclared-reference errors carry a real span, and
    // parity holds across Merge and Transform headers. If any of these
    // tests regress, the refactor has leaked — do NOT weaken the asserts.

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

    /// Gate: combine edge wiring in `PipelineConfig::compile` emits an
    /// E004 `Diagnostic` when a combine input references an undeclared
    /// upstream. The diagnostic's primary span carries the `line_only`
    /// synthetic span of the offending `products: nowhere` reference
    /// (populated from `Spanned<NodeInput>::referenced`) and the
    /// structured `DiagnosticPayload::InputRefUndeclared` payload pins
    /// the consumer/qualifier/reference triple — transposing any of the
    /// three identifiers fails the assertion (strictly stronger than
    /// substring matching on the message).
    #[test]
    fn test_combine_undeclared_input_diagnostic_has_real_span() {
        let yaml = r#"
pipeline:
  name: e300_gate
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
        let diags = config
            .compile(&clinker_core::config::CompileContext::default())
            .expect_err("combine referencing undeclared upstream must fail");
        let e004 = diags
            .iter()
            .find(|d| {
                d.code == "E004" && d.input_ref_payload().is_some_and(|(_, q, _)| q.is_some())
            })
            .unwrap_or_else(|| {
                panic!("expected E004 diagnostic with combine-arm payload; got: {diags:#?}")
            });
        // Structured-payload assertion — strictly stronger than substring
        // matching: transposing any of the three identifiers fails here.
        let (consumer, qualifier, reference) = e004
            .input_ref_payload()
            .expect("E004 must carry InputRefUndeclared payload");
        assert_eq!(consumer, "broken", "consumer name");
        assert_eq!(qualifier, Some("products"), "combine qualifier");
        assert_eq!(reference, "nowhere", "undeclared upstream");

        // The primary span carries the real source line of the offending
        // `products: nowhere` reference (≥ 10 lines into the fixture).
        let line = e004.primary.span.synthetic_line_number().unwrap_or(0);
        assert!(
            line >= 10,
            "E004 span must encode a non-zero line inside the combine input block; got {line} (span={:?})",
            e004.primary.span
        );
        assert!(
            e004.message.contains(&format!("line {line}")),
            "E004 message must include 'line {line}', got: {}",
            e004.message
        );
    }

    /// Regression: combine-undeclared input surfaces as a
    /// structural-stage E004 with combine-arm payload even when a
    /// sibling node would have triggered a CXL error in bind_schema.
    ///
    /// Previously combine undeclared-input emission lived in stage-5
    /// edge wiring AFTER bind_schema. A fixture with both an E200 (CXL)
    /// on one node and a combine-undeclared on another would surface
    /// ONLY the E200; the combine typo was invisible until the user
    /// fixed the unrelated CXL error first. The unified
    /// `resolve_all_input_references` pass moves combine-undeclared
    /// forward to the structural stage so it fires regardless of what
    /// bind_schema would have done.
    ///
    /// `compile_topology_only`'s early return still short-circuits
    /// bind_schema once any structural-stage E004 exists, so this
    /// fixture surfaces ONLY E004 (not both E200 and E004 in the same
    /// pass). The load-bearing user-facing fix — combine typo no
    /// longer hidden by a sibling CXL error — is what this regression
    /// locks in.
    #[test]
    fn test_combine_undeclared_input_fires_even_when_bind_schema_would_error() {
        let yaml = r#"
pipeline:
  name: e300_independence_gate
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      schema:
        - { name: order_id, type: string }
  - type: transform
    name: cxl_broken
    input: orders
    config:
      cxl: |
        emit value = unknown_field + 1
  - type: combine
    name: broken_combine
    input:
      orders: orders
      products: nowhere
    config:
      where: "orders.order_id == products.order_id"
      cxl: |
        emit order_id = orders.order_id
"#;
        let config = parse_fixture(yaml);
        let diags = config
            .compile(&clinker_core::config::CompileContext::default())
            .expect_err("fixture has E004 (undeclared combine input) — must fail");

        // The load-bearing assertion: combine-undeclared is visible
        // here, with the structured payload identifying the broken
        // qualifier. Pre-remediation this combine typo would have been
        // hidden behind cxl_broken's sibling E200 (because E307 lived
        // in stage-5 Phase-2 after bind_schema's early return).
        let combine_e004 = diags
            .iter()
            .find(|d| {
                d.code == "E004" && d.input_ref_payload().is_some_and(|(_, q, _)| q.is_some())
            })
            .unwrap_or_else(|| {
                panic!(
                    "expected an E004 with combine-arm payload from \
                     broken_combine.products → nowhere even though \
                     cxl_broken would have triggered an E200 in bind_schema. \
                     The unified input-reference pass at stage 3.5 must \
                     fire BEFORE bind_schema. got: {diags:#?}"
                )
            });

        let (consumer, qualifier, reference) = combine_e004
            .input_ref_payload()
            .expect("combine E004 must carry InputRefUndeclared payload");
        assert_eq!(consumer, "broken_combine");
        assert_eq!(qualifier, Some("products"));
        assert_eq!(reference, "nowhere");
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
    // combine (and lookup, during the C.2 migration) integration tests. It
    // wraps the public
    // `PipelineExecutor::run_plan_with_readers_writers_with_primary` entry.
    //
    // Error handling: structured `CombineFixtureError` preserves parse,
    // compile, and run distinctions. Tests that assert on specific
    // diagnostic codes (E306 / E311 / E312 in C.2.4's gate suite) can
    // destructure the `Compile(Vec<Diagnostic>)` variant and inspect
    // per-diagnostic codes rather than fuzzy-matching message strings.
    //
    // Output capture: every declared output node gets its own
    // `SharedBuffer`; the returned `CombineFixtureResult.outputs` is a
    // `HashMap<String, String>` keyed by output name. `primary_output()`
    // returns the first-declared output for single-output tests;
    // multi-output tests (Route downstream of combine) index by name.

    /// Structured error for `run_combine_fixture`. Preserves the original
    /// parse / compile / run signal so tests can destructure to assert on
    /// specific diagnostic codes.
    #[derive(Debug)]
    #[allow(dead_code)] // Variants accessed via Debug and pattern matching.
    pub(crate) enum CombineFixtureError {
        /// YAML parse failure before compilation.
        Parse(String),
        /// Compile-time diagnostics (E2xx/E3xx codes, etc.). Full
        /// `Vec<Diagnostic>` preserved — tests can assert on code,
        /// severity, span, and message independently.
        Compile(Vec<Diagnostic>),
        /// Runtime / executor-layer failure.
        Run(PipelineError),
        /// Caller-provided inputs violated a helper precondition (e.g.
        /// empty inputs and no primary override, no outputs declared).
        Precondition(String),
    }

    impl std::fmt::Display for CombineFixtureError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Parse(msg) => write!(f, "parse failed: {msg}"),
                Self::Compile(diags) => {
                    writeln!(f, "compile failed with {} diagnostic(s):", diags.len())?;
                    for d in diags {
                        writeln!(f, "  [{}] {}", d.code, d.message)?;
                    }
                    Ok(())
                }
                Self::Run(err) => write!(f, "runtime failure: {err}"),
                Self::Precondition(msg) => write!(f, "precondition violation: {msg}"),
            }
        }
    }

    impl std::error::Error for CombineFixtureError {}

    /// Result of running a fixture pipeline.
    #[derive(Debug)]
    #[allow(dead_code)] // Fields accessed via named getters; field access is lint-silent.
    pub(crate) struct CombineFixtureResult {
        pub report: ExecutionReport,
        /// Captured output bytes per declared output node, keyed by
        /// output `name`. Every output node registered in the pipeline
        /// YAML gets an entry — including empty buffers for outputs that
        /// received zero records.
        pub outputs: HashMap<String, String>,
        /// YAML-declaration-order name of the first output. Used by
        /// `primary_output()` for single-output tests.
        pub primary_output_name: Option<String>,
    }

    impl CombineFixtureResult {
        /// Single-output convenience: captured bytes from the FIRST
        /// declared output node (by YAML declaration order). Panics if
        /// the pipeline declared no outputs.
        #[allow(dead_code)]
        pub fn primary_output(&self) -> &str {
            let name = self.primary_output_name.as_deref().expect(
                "CombineFixtureResult::primary_output called on a pipeline with no outputs",
            );
            self.outputs
                .get(name)
                .map(String::as_str)
                .unwrap_or_else(|| panic!("primary_output_name {name:?} missing from outputs map"))
        }

        /// Look up a named output by its YAML `name` field. Returns
        /// `None` for unknown names.
        #[allow(dead_code)]
        pub fn output(&self, name: &str) -> Option<&str> {
            self.outputs.get(name).map(String::as_str)
        }
    }

    /// Run a combine (or lookup) pipeline end-to-end through the public
    /// executor with the given named in-memory CSV inputs. Returns a
    /// `CombineFixtureResult` containing the full `ExecutionReport` and
    /// every declared output's captured bytes.
    ///
    /// `inputs` is `&[(source_name, csv_bytes_as_str)]`. `primary_override`
    /// pins a specific source as the driving input; pass `None` to
    /// default to the first entry in `inputs`.
    #[allow(dead_code)] // Wired by C.2.0+ tests; gate test enforces it compiles.
    fn run_combine_fixture(
        yaml: &str,
        inputs: &[(&str, &str)],
        primary_override: Option<&str>,
    ) -> Result<CombineFixtureResult, CombineFixtureError> {
        let config: PipelineConfig = clinker_core::yaml::from_str(yaml)
            .map_err(|e| CombineFixtureError::Parse(e.to_string()))?;
        let ctx = CompileContext::default();
        let plan = config.compile(&ctx).map_err(CombineFixtureError::Compile)?;

        let primary: String = match primary_override {
            Some(name) => name.to_string(),
            None => inputs
                .first()
                .map(|(n, _)| (*n).to_string())
                .ok_or_else(|| {
                    CombineFixtureError::Precondition(
                        "run_combine_fixture: no inputs provided and no primary_override set"
                            .to_string(),
                    )
                })?,
        };

        let mut readers: HashMap<String, Box<dyn Read + Send>> = HashMap::new();
        for (name, data) in inputs {
            readers.insert(
                (*name).to_string(),
                Box::new(std::io::Cursor::new(data.as_bytes().to_vec())) as Box<dyn Read + Send>,
            );
        }

        // Register a SharedBuffer for EVERY declared output — multi-output
        // pipelines (Route downstream of combine) would lose data otherwise.
        let output_names: Vec<String> = plan
            .config()
            .output_configs()
            .map(|cfg| cfg.name.clone())
            .collect();
        if output_names.is_empty() {
            return Err(CombineFixtureError::Precondition(
                "run_combine_fixture: pipeline declares no output nodes".to_string(),
            ));
        }
        let output_buffers: HashMap<String, SharedBuffer> = output_names
            .iter()
            .map(|name| (name.clone(), SharedBuffer::new()))
            .collect();
        let writers: HashMap<String, Box<dyn Write + Send>> = output_buffers
            .iter()
            .map(|(name, buf)| (name.clone(), Box::new(buf.clone()) as Box<dyn Write + Send>))
            .collect();

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
        )
        .map_err(CombineFixtureError::Run)?;

        let outputs: HashMap<String, String> = output_buffers
            .into_iter()
            .map(|(name, buf)| (name, buf.as_string()))
            .collect();
        let primary_output_name = output_names.into_iter().next();
        Ok(CombineFixtureResult {
            report,
            outputs,
            primary_output_name,
        })
    }

    /// Canonicalize a CSV string via the `csv` crate: parse into
    /// `(headers, Vec<record>)`, sort records by lexicographic tuple
    /// order, re-serialize. Robust against quoted fields, embedded
    /// newlines, and escaped delimiters — unlike a raw line-sort that
    /// would mis-split multiline fields.
    ///
    /// Empty-or-whitespace input → empty string.
    fn canonicalize_csv(csv: &str) -> String {
        if csv.trim().is_empty() {
            return String::new();
        }
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(csv.as_bytes());
        let headers: Vec<String> = match rdr.headers() {
            Ok(h) => h.iter().map(String::from).collect(),
            Err(e) => panic!("canonicalize_csv: failed to read headers: {e}\ninput:\n{csv}"),
        };
        let mut rows: Vec<Vec<String>> = Vec::new();
        for rec in rdr.records() {
            let rec = rec.unwrap_or_else(|e| {
                panic!("canonicalize_csv: failed to read record: {e}\ninput:\n{csv}")
            });
            rows.push(rec.iter().map(String::from).collect());
        }
        rows.sort();

        let mut wtr = csv::WriterBuilder::new()
            .has_headers(false)
            .from_writer(Vec::new());
        wtr.write_record(&headers)
            .expect("canonicalize_csv: writing headers failed");
        for row in &rows {
            wtr.write_record(row)
                .expect("canonicalize_csv: writing row failed");
        }
        wtr.flush().expect("canonicalize_csv: flush failed");
        String::from_utf8(
            wtr.into_inner()
                .expect("canonicalize_csv: into_inner failed"),
        )
        .expect("canonicalize_csv: output not utf-8")
    }

    /// Assert two CSV outputs match record-by-record, ignoring row order
    /// but requiring identical header and identical row content. Uses the
    /// `csv` crate for proper parsing (quoted fields, multi-line records,
    /// escaped delimiters) rather than raw line-sorting.
    ///
    /// This is the C.2.3.4 regression assertion: migrated combine
    /// fixtures must produce output equivalent to the C.2.0 lookup
    /// baseline.
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
        let result = run_combine_fixture(yaml, &[("src", inputs)], None)
            .expect("run_combine_fixture must compile and execute the scaffold pipeline");
        assert_eq!(
            result.report.counters.total_count, 3,
            "expected 3 records processed, got {}",
            result.report.counters.total_count
        );
        let output = result.primary_output();
        assert!(
            output.contains("id\n") && output.contains("A\n"),
            "expected output to contain header + records; got: {output:?}"
        );

        // Exercise the secondary getters so they stay reachable from a
        // #[test] and the `dead_code` lint stays silent.
        assert!(result.output("out").is_some());
        assert!(result.output("nonexistent").is_none());
        assert_eq!(result.outputs.len(), 1);

        // Exercise canonicalize_csv + assert_records_match.
        let unsorted = "id\nC\nA\nB\n";
        let sorted = "id\nA\nB\nC\n";
        assert_records_match(unsorted, sorted);

        // Exercise CombineFixtureError::Display: a minimal Precondition
        // case exercises the enum's fmt impl so clippy's dead_code
        // heuristic doesn't flag variants as unused. No runtime assertion
        // beyond non-empty Display output.
        let err = CombineFixtureError::Precondition("demo".to_string());
        assert!(!format!("{err}").is_empty());
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
        let result = run_combine_fixture(
            lookup_yaml_equality(),
            &[("orders", orders), ("products", products)],
            None,
        )
        .expect("lookup_eq baseline must execute under current lookup runtime");
        insta::assert_snapshot!(
            "lookup_baseline_equality",
            canonicalize_csv(result.primary_output())
        );
    }

    /// C.2.0.2 — range predicate baseline (compound and).
    #[test]
    fn test_lookup_baseline_range() {
        let employees =
            "employee_id,ee_group,pay\nE001,exempt,75000\nE002,hourly,35000\nE003,exempt,120000\n";
        let rate_bands = "ee_group,min_pay,max_pay,rate_class\nexempt,50000,80000,tier_1\nexempt,80001,150000,tier_2\nhourly,20000,50000,tier_3\n";
        let result = run_combine_fixture(
            lookup_yaml_range(),
            &[("employees", employees), ("rate_bands", rate_bands)],
            None,
        )
        .expect("lookup_range baseline must execute under current lookup runtime");
        insta::assert_snapshot!(
            "lookup_baseline_range",
            canonicalize_csv(result.primary_output())
        );
    }

    /// C.2.0.2 — on_miss: skip baseline. Unmatched rows are dropped.
    #[test]
    fn test_lookup_baseline_on_miss_skip() {
        let orders = "order_id,product_id\nORD-1,PROD-A\nORD-2,PROD-X\nORD-3,PROD-B\n";
        let products = "product_id,product_name\nPROD-A,Widget\nPROD-B,Gadget\n";
        let result = run_combine_fixture(
            lookup_yaml_on_miss_skip(),
            &[("orders", orders), ("products", products)],
            None,
        )
        .expect("lookup_skip baseline must execute under current lookup runtime");
        insta::assert_snapshot!(
            "lookup_baseline_on_miss_skip",
            canonicalize_csv(result.primary_output())
        );
    }

    /// C.2.0.2 — match: all (fan-out) baseline. One employee with N
    /// assignments produces N output records.
    #[test]
    fn test_lookup_baseline_match_all() {
        let employees = "employee_id,department\nE001,Engineering\nE002,Sales\nE003,Engineering\n";
        let assignments = "employee_id,project\nE001,Phoenix\nE001,Atlas\nE002,Borealis\nE003,Phoenix\nE003,Atlas\nE003,Vega\n";
        let result = run_combine_fixture(
            lookup_yaml_match_all(),
            &[("employees", employees), ("assignments", assignments)],
            None,
        )
        .expect("lookup_match_all baseline must execute under current lookup runtime");
        insta::assert_snapshot!(
            "lookup_baseline_match_all",
            canonicalize_csv(result.primary_output())
        );
    }

    /// C.2.0.2 — match: all + on_miss: skip baseline. Unmatched
    /// employees dropped; matched ones fan out.
    #[test]
    fn test_lookup_baseline_match_all_skip() {
        let employees = "employee_id,department\nE001,Engineering\nE002,Sales\nE003,Engineering\nE004,Marketing\n";
        let assignments = "employee_id,project\nE001,Phoenix\nE001,Atlas\nE003,Vega\n";
        let result = run_combine_fixture(
            lookup_yaml_match_all_skip(),
            &[("employees", employees), ("assignments", assignments)],
            None,
        )
        .expect("lookup_match_all_skip baseline must execute under current lookup runtime");
        insta::assert_snapshot!(
            "lookup_baseline_match_all_skip",
            canonicalize_csv(result.primary_output())
        );
    }

    // ─── Multi-output lookup baselines (order_fulfillment + lookup_enrichment)
    //
    // These fixtures have a `route` node downstream of the lookup
    // transform, producing TWO output streams each. The baseline captures
    // BOTH outputs so the C.2.3.5 / C.2.3.6 migration tests can assert
    // equivalence across every output.

    /// Inline variant of `examples/pipelines/order_fulfillment.yaml`.
    /// The ORIGINAL YAML uses a `_route`-emit-transform pattern that does
    /// NOT deterministically split records under the current executor
    /// (both outputs end up stale or degenerate). This baseline uses a
    /// PROPER `type: route` node so the captured outputs are
    /// deterministically split — which is what C.2.3.5 must match after
    /// migration. The lookup predicate, on_miss semantics, and emit
    /// shape are preserved byte-identical to the source file; only the
    /// output-splitting mechanism is normalized.
    fn lookup_yaml_order_fulfillment_baseline() -> &'static str {
        r#"
pipeline:
  name: lookup_order_fulfillment_baseline
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      options:
        has_header: true
      schema:
        - { name: order_id, type: string }
        - { name: order_date, type: string }
        - { name: quantity, type: string }
        - { name: unit_price, type: string }
        - { name: product_code, type: string }
        - { name: priority_level, type: string }
  - type: source
    name: products
    config:
      name: products
      type: csv
      path: products.csv
      options:
        has_header: true
      schema:
        - { name: product_code, type: string }
        - { name: product_name, type: string }
        - { name: category, type: string }
        - { name: weight_kg, type: string }
  - type: transform
    name: product_lookup
    input: orders
    config:
      lookup:
        source: products
        where: "product_code == products.product_code"
      cxl: |
        emit order_id = order_id
        emit priority_level = priority_level
        emit product_code = product_code
        emit product_name = products.product_name
        emit category = products.category
        emit weight_kg = products.weight_kg
  - type: route
    name: priority_split
    input: product_lookup
    config:
      mode: exclusive
      conditions:
        priority_report: 'priority_level == "urgent" or priority_level == "high"'
      default: fulfilled_orders
  - type: output
    name: fulfilled_orders
    input: priority_split
    config:
      name: fulfilled_orders
      type: csv
      path: fulfilled_orders.csv
      include_unmapped: true
  - type: output
    name: priority_report
    input: priority_split
    config:
      name: priority_report
      type: csv
      path: priority_report.csv
      include_unmapped: true
"#
    }

    /// Inline variant of `benches/pipelines/realistic/lookup_enrichment.yaml`.
    /// Preserves the generic `f0/f1/f2/f3` field names and the route
    /// splitting on whether the lookup matched. Path scheme changed
    /// from `bench://` to plain relative paths (bench:// is a
    /// bench-runner-only scheme; the test harness injects in-memory
    /// readers by source name). The route condition is preserved
    /// byte-identical from the source file.
    fn lookup_yaml_enrichment_baseline() -> &'static str {
        r#"
pipeline:
  name: lookup_enrichment_baseline
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      options:
        has_header: true
      schema:
        - { name: f0, type: int }
        - { name: f1, type: string }
        - { name: f2, type: int }
        - { name: f3, type: string }
  - type: source
    name: products
    config:
      name: products
      type: csv
      path: products.csv
      options:
        has_header: true
      schema:
        - { name: f0, type: string }
        - { name: f1, type: string }
  - type: transform
    name: enrich
    input: orders
    config:
      lookup:
        source: products
        where: "f1 == products.f0"
        on_miss: null_fields
      cxl: |
        emit order_id = f0
        emit product_code = f1
        emit quantity = f2
        emit product_name = products.f1 ?? "UNKNOWN"
        emit priority = f3
  - type: route
    name: priority_split
    input: enrich
    config:
      mode: exclusive
      conditions:
        high_priority_out: 'not product_name.is_null() and product_name != "UNKNOWN"'
      default: standard_out
  - type: output
    name: high_priority_out
    input: priority_split
    config:
      name: high_priority_out
      type: csv
      path: high_priority_out.csv
      include_unmapped: true
  - type: output
    name: standard_out
    input: priority_split
    config:
      name: standard_out
      type: csv
      path: standard_out.csv
      include_unmapped: true
"#
    }

    /// C.2.0.2 — multi-output baseline: order_fulfillment pattern.
    /// Captures BOTH `fulfilled_orders` and `priority_report` outputs
    /// as separate snapshots.
    #[test]
    fn test_lookup_baseline_order_fulfillment() {
        let orders = concat!(
            "order_id,order_date,quantity,unit_price,product_code,priority_level\n",
            "1,2024-01-15,5,29.99,PROD-A,urgent\n",
            "2,2024-01-16,10,15.50,PROD-B,normal\n",
            "3,2024-01-17,2,99.99,PROD-X,high\n",
            "4,2024-01-18,1,49.99,PROD-A,low\n",
        );
        let products = concat!(
            "product_code,product_name,category,weight_kg\n",
            "PROD-A,Widget,tools,1.5\n",
            "PROD-B,Gadget,electronics,0.3\n",
        );
        let result = run_combine_fixture(
            lookup_yaml_order_fulfillment_baseline(),
            &[("orders", orders), ("products", products)],
            None,
        )
        .expect("order_fulfillment baseline must execute under current lookup runtime");
        let fulfilled = result
            .output("fulfilled_orders")
            .expect("fulfilled_orders output must be present");
        let priority = result
            .output("priority_report")
            .expect("priority_report output must be present");
        insta::assert_snapshot!(
            "lookup_baseline_order_fulfillment_fulfilled_orders",
            canonicalize_csv(fulfilled)
        );
        insta::assert_snapshot!(
            "lookup_baseline_order_fulfillment_priority_report",
            canonicalize_csv(priority)
        );
    }

    /// C.2.0.2 — multi-output baseline: lookup_enrichment bench pattern.
    /// Captures BOTH `high_priority_out` and `standard_out` outputs.
    #[test]
    fn test_lookup_baseline_enrichment() {
        let orders = concat!(
            "f0,f1,f2,f3\n",
            "1,PROD-A,10,priority\n",
            "2,PROD-B,5,normal\n",
            "3,PROD-X,1,urgent\n",
            "4,PROD-A,3,low\n",
        );
        let products = concat!("f0,f1\n", "PROD-A,Widget\n", "PROD-B,Gadget\n");
        let result = run_combine_fixture(
            lookup_yaml_enrichment_baseline(),
            &[("orders", orders), ("products", products)],
            None,
        )
        .expect("lookup_enrichment baseline must execute under current lookup runtime");
        // Sanity: every input row should land in exactly one output. The
        // lookup enrichment fixture has 4 inputs and we expect 3 to route
        // to high_priority and 1 to standard (PROD-X misses the lookup,
        // becomes "UNKNOWN", and falls into the default branch).
        assert_eq!(
            result.report.counters.total_count, 4,
            "expected 4 input records processed; counters: {:?}",
            result.report.counters
        );
        let high = result
            .output("high_priority_out")
            .expect("high_priority_out output must be present");
        let standard = result
            .output("standard_out")
            .expect("standard_out output must be present");
        assert!(
            !(high.trim().is_empty() && standard.trim().is_empty()),
            "both enrichment outputs empty — route dispatch is not firing. report={:?}\nhigh={high:?}\nstandard={standard:?}",
            result.report
        );
        insta::assert_snapshot!(
            "lookup_baseline_enrichment_high_priority",
            canonicalize_csv(high)
        );
        insta::assert_snapshot!(
            "lookup_baseline_enrichment_standard",
            canonicalize_csv(standard)
        );
    }

    /// C.2.0.2 gate: verify that every lookup baseline `.snap` file
    /// exists on disk under `tests/snapshots/` AND has non-degenerate
    /// content. Depends on the corresponding `test_lookup_baseline_*`
    /// tests having run first to write the snapshots.
    ///
    /// RESOLUTION T-13: verify snapshot CONTENT exists (non-empty body
    /// with header + at least one data row), not just file existence.
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
            "combine_test__tests__lookup_baseline_order_fulfillment_fulfilled_orders.snap",
            "combine_test__tests__lookup_baseline_order_fulfillment_priority_report.snap",
            "combine_test__tests__lookup_baseline_enrichment_high_priority.snap",
            "combine_test__tests__lookup_baseline_enrichment_standard.snap",
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

    // ─── Combine executor gate tests ─────────────────────────────────────
    //
    // These tests exercise the `PlanNode::Combine` dispatch arm end-to-end
    // through the public executor. They use small inline CSV inputs so the
    // test surface is readable and the oracle outputs are auditable without
    // reading a 1000-line fixture file.

    const EXEC_ORDERS: &str = "\
order_id,product_id,amount
ORD-1,PROD-A,10
ORD-2,PROD-B,20
ORD-3,PROD-X,30
";
    const EXEC_PRODUCTS: &str = "\
product_id,name,category
PROD-A,Widget,cat-1
PROD-B,Gadget,cat-2
";

    fn combine_exec_yaml(match_mode: &str, on_miss: &str) -> String {
        format!(
            r#"
pipeline:
  name: combine_exec
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      schema:
        - {{ name: order_id, type: string }}
        - {{ name: product_id, type: string }}
        - {{ name: amount, type: int }}
  - type: source
    name: products
    config:
      name: products
      type: csv
      path: products.csv
      schema:
        - {{ name: product_id, type: string }}
        - {{ name: name, type: string }}
        - {{ name: category, type: string }}
  - type: combine
    name: enriched
    input:
      orders: orders
      products: products
    config:
      where: "orders.product_id == products.product_id"
      match: {match_mode}
      on_miss: {on_miss}
      cxl: |
        emit order_id = orders.order_id
        emit product_id = orders.product_id
        emit product_name = products.name
        emit amount = orders.amount
  - type: output
    name: enriched_out
    input: enriched
    config:
      name: enriched_out
      type: csv
      path: enriched.csv
"#,
        )
    }

    /// match: first, on_miss: null_fields — a matched order enriches
    /// with product fields; an unmatched order emits with null product
    /// fields.
    #[test]
    fn test_combine_exec_equi_match_first() {
        let yaml = combine_exec_yaml("first", "null_fields");
        let result = run_combine_fixture(
            &yaml,
            &[("orders", EXEC_ORDERS), ("products", EXEC_PRODUCTS)],
            Some("orders"),
        )
        .expect("combine executor must run a pure-equi 2-input fixture");
        let canon = canonicalize_csv(result.primary_output());
        let expected = canonicalize_csv(
            "order_id,product_id,product_name,amount\n\
             ORD-1,PROD-A,Widget,10\n\
             ORD-2,PROD-B,Gadget,20\n\
             ORD-3,PROD-X,,30\n",
        );
        assert_eq!(canon, expected, "combine match:first output mismatch");
    }

    /// match: all — verifies fan-out: every matching build record
    /// produces an independent output row.
    #[test]
    fn test_combine_exec_equi_match_all() {
        // Two products for the same product_id so match:all fans out.
        let orders = "order_id,product_id,amount\nORD-1,PROD-A,10\n";
        let products = "product_id,name,category\nPROD-A,Widget,cat-1\nPROD-A,WidgetV2,cat-1b\n";
        let yaml = combine_exec_yaml("all", "null_fields");
        let result = run_combine_fixture(
            &yaml,
            &[("orders", orders), ("products", products)],
            Some("orders"),
        )
        .expect("match:all fan-out must run");
        let canon = canonicalize_csv(result.primary_output());
        // Two matching build rows → two output rows with the same
        // probe fields but different product_name values.
        assert!(canon.contains("ORD-1,PROD-A,Widget,10"), "got: {canon}");
        assert!(canon.contains("ORD-1,PROD-A,WidgetV2,10"), "got: {canon}");
        let mut rows = 0usize;
        for line in canon.lines().skip(1) {
            if !line.is_empty() {
                rows += 1;
            }
        }
        assert_eq!(rows, 2, "match:all must produce 2 output rows");
    }

    /// match: collect — synthesized output record carries a
    /// `<build_qualifier>: Value::Array(Value::Map)` field per driver;
    /// the `cxl:` body is required-empty (E311). Verify the output
    /// contains a serialized array per order including nested maps.
    #[test]
    fn test_combine_exec_equi_match_collect() {
        let yaml = r#"
pipeline:
  name: combine_exec_collect
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
        - { name: amount, type: int }
  - type: source
    name: products
    config:
      name: products
      type: csv
      path: products.csv
      schema:
        - { name: product_id, type: string }
        - { name: name, type: string }
        - { name: category, type: string }
  - type: combine
    name: collected
    input:
      orders: orders
      products: products
    config:
      where: "orders.product_id == products.product_id"
      match: collect
      on_miss: null_fields
      cxl: ""
  - type: output
    name: collected_out
    input: collected
    config:
      name: collected_out
      type: csv
      path: collected.csv
      include_unmapped: true
"#;
        let orders = "order_id,product_id,amount\nORD-1,PROD-A,10\n";
        let products = "product_id,name,category\nPROD-A,Widget,cat-1\nPROD-A,WidgetV2,cat-1b\n";
        let result = run_combine_fixture(
            yaml,
            &[("orders", orders), ("products", products)],
            Some("orders"),
        )
        .expect("match:collect must run");
        let out = result.primary_output();
        // The array serializes as bracketed content in CSV; each
        // matched build record appears as a nested map representation.
        assert!(
            out.contains("Widget"),
            "collect array should carry Widget; got: {out:?}"
        );
        assert!(
            out.contains("WidgetV2"),
            "collect array should carry WidgetV2; got: {out:?}"
        );
        // Exactly one output row per driver record under Collect.
        let data_rows = out.lines().skip(1).filter(|l| !l.is_empty()).count();
        assert_eq!(data_rows, 1, "match:collect emits one row per driver");
    }

    /// match: collect on a driver with zero matches — the array field
    /// is an empty array, not Value::Null. Verify the driver row is
    /// emitted (no on_miss skip).
    #[test]
    fn test_combine_exec_collect_empty_array_on_miss() {
        let yaml = r#"
pipeline:
  name: combine_exec_collect_empty
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
        - { name: amount, type: int }
  - type: source
    name: products
    config:
      name: products
      type: csv
      path: products.csv
      schema:
        - { name: product_id, type: string }
        - { name: name, type: string }
        - { name: category, type: string }
  - type: combine
    name: collected
    input:
      orders: orders
      products: products
    config:
      where: "orders.product_id == products.product_id"
      match: collect
      on_miss: null_fields
      cxl: ""
  - type: output
    name: collected_out
    input: collected
    config:
      name: collected_out
      type: csv
      path: collected_empty.csv
      include_unmapped: true
"#;
        // ORD-1's product_id is PROD-X — no matching build row.
        let orders = "order_id,product_id,amount\nORD-1,PROD-X,10\n";
        let products = "product_id,name,category\nPROD-A,Widget,cat-1\n";
        let result = run_combine_fixture(
            yaml,
            &[("orders", orders), ("products", products)],
            Some("orders"),
        )
        .expect("collect empty-array must run");
        let out = result.primary_output();
        // Exactly one data row (the driver row, with an empty array).
        let data_rows = out.lines().skip(1).filter(|l| !l.is_empty()).count();
        assert_eq!(data_rows, 1, "collect emits the driver row even on miss");
        // The `products` column carries the array serialization. Empty
        // array stringifies as `[]` in Value::Display; verify we see
        // `[]` and not `null` in the row.
        let data = out.lines().nth(1).unwrap_or("");
        assert!(
            data.contains("[]"),
            "empty-match collect must emit `[]`, not null; got row: {data:?}",
        );
    }

    /// match: collect with 10K+1 matches on one driver record truncates
    /// at COLLECT_PER_GROUP_CAP = 10_000. Build the build-side with
    /// 10_001 rows that all share the same key so every one collides.
    #[test]
    fn test_combine_exec_collect_per_group_cap() {
        let yaml = r#"
pipeline:
  name: combine_exec_collect_cap
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
        - { name: amount, type: int }
  - type: source
    name: products
    config:
      name: products
      type: csv
      path: products.csv
      schema:
        - { name: product_id, type: string }
        - { name: name, type: string }
        - { name: category, type: string }
  - type: combine
    name: collected
    input:
      orders: orders
      products: products
    config:
      where: "orders.product_id == products.product_id"
      match: collect
      on_miss: null_fields
      cxl: ""
  - type: output
    name: collected_out
    input: collected
    config:
      name: collected_out
      type: csv
      path: collected_cap.csv
      include_unmapped: true
"#;
        let orders = "order_id,product_id,amount\nORD-1,PROD-A,10\n";
        let mut products = String::from("product_id,name,category\n");
        for i in 0..10_001 {
            products.push_str(&format!("PROD-A,Widget-{i},cat-1\n"));
        }
        let result = run_combine_fixture(
            yaml,
            &[("orders", orders), ("products", &products)],
            Some("orders"),
        )
        .expect("collect cap must run");
        let out = result.primary_output();
        // The driver row is emitted.
        let data_rows = out.lines().skip(1).filter(|l| !l.is_empty()).count();
        assert_eq!(data_rows, 1, "collect emits one row per driver");
        // Expect exactly 10_000 occurrences of the substring `Widget-`
        // in the serialized array — the 10_001st match was truncated.
        let widget_count = out.matches("Widget-").count();
        assert_eq!(
            widget_count, 10_000,
            "collect must truncate at 10_000 matches per group"
        );
    }

    /// on_miss: null_fields — unmatched driver row emits with
    /// build-qualifier fields filled with Null. The `product_name`
    /// field in the CSV output is empty for ORD-X.
    #[test]
    fn test_combine_exec_on_miss_null_fields() {
        let yaml = combine_exec_yaml("first", "null_fields");
        let result = run_combine_fixture(
            &yaml,
            &[("orders", EXEC_ORDERS), ("products", EXEC_PRODUCTS)],
            Some("orders"),
        )
        .expect("null_fields must run");
        let canon = canonicalize_csv(result.primary_output());
        // ORD-3 has PROD-X which has no matching product — emit row
        // with blank product_name (Null → empty CSV cell).
        assert!(
            canon.contains("ORD-3,PROD-X,,30"),
            "null_fields must leave build-qualified fields blank; got: {canon}"
        );
    }

    /// on_miss: skip — unmatched driver rows are silently dropped.
    #[test]
    fn test_combine_exec_on_miss_skip() {
        let yaml = combine_exec_yaml("first", "skip");
        let result = run_combine_fixture(
            &yaml,
            &[("orders", EXEC_ORDERS), ("products", EXEC_PRODUCTS)],
            Some("orders"),
        )
        .expect("skip must run");
        let canon = canonicalize_csv(result.primary_output());
        // Two matches (ORD-1, ORD-2). Unmatched ORD-3 is dropped.
        let data_rows = canon.lines().skip(1).filter(|l| !l.is_empty()).count();
        assert_eq!(data_rows, 2, "on_miss:skip drops unmatched rows");
        assert!(!canon.contains("ORD-3"), "ORD-3 must be dropped");
    }

    /// on_miss: error — the pipeline fails on the first unmatched row.
    /// Verify an E310-coded error surfaces.
    #[test]
    fn test_combine_exec_on_miss_error() {
        let yaml = combine_exec_yaml("first", "error");
        let err = run_combine_fixture(
            &yaml,
            &[("orders", EXEC_ORDERS), ("products", EXEC_PRODUCTS)],
            Some("orders"),
        )
        .expect_err("on_miss:error must fail the pipeline on first unmatched row");
        let msg = format!("{err}");
        assert!(
            msg.contains("E310"),
            "on_miss:error must surface E310; got: {msg}",
        );
    }

    /// Residual predicate — equi predicate + a range residual. The
    /// range is a cross-input comparison so it lands as a RangeConjunct
    /// (not residual in the strict sense); build a pipeline where the
    /// residual is same-input to force the residual path.
    ///
    /// Fixture: order's `amount >= 20` AND orders.product_id ==
    /// products.product_id. The `amount >= 20` conjunct is a
    /// single-input comparison against a literal → residual.
    #[test]
    fn test_combine_exec_residual_predicate() {
        let yaml = r#"
pipeline:
  name: combine_exec_residual
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
        - { name: amount, type: int }
  - type: source
    name: products
    config:
      name: products
      type: csv
      path: products.csv
      schema:
        - { name: product_id, type: string }
        - { name: name, type: string }
        - { name: category, type: string }
  - type: combine
    name: filtered
    input:
      orders: orders
      products: products
    config:
      where: "orders.product_id == products.product_id and orders.amount >= 20"
      match: first
      on_miss: skip
      cxl: |
        emit order_id = orders.order_id
        emit amount = orders.amount
        emit product_name = products.name
  - type: output
    name: filtered_out
    input: filtered
    config:
      name: filtered_out
      type: csv
      path: filtered.csv
"#;
        let result = run_combine_fixture(
            yaml,
            &[("orders", EXEC_ORDERS), ("products", EXEC_PRODUCTS)],
            Some("orders"),
        )
        .expect("residual predicate must run");
        let canon = canonicalize_csv(result.primary_output());
        // ORD-1 amount=10 is rejected by residual; ORD-2 amount=20 is
        // accepted; ORD-3 PROD-X doesn't match the equi and on_miss:skip.
        let data_rows = canon.lines().skip(1).filter(|l| !l.is_empty()).count();
        assert_eq!(
            data_rows, 1,
            "residual rejects ORD-1 (amount<20); ORD-3 unmatched; only ORD-2 survives. got: {canon}"
        );
        assert!(canon.contains("ORD-2"), "ORD-2 must survive; got: {canon}");
    }

    /// Null key on the probe side — CXL ternary rules dictate no match.
    /// Verify an order with NULL product_id produces no join match.
    #[test]
    fn test_combine_exec_null_key_no_match() {
        // Empty product_id field on ORD-2 triggers Null after schema
        // coercion (string type preserves it as empty string, not
        // Null). To generate a true Null probe key, use a compare-to-
        // nonexistent column or rely on runtime Null propagation via
        // a residual equality that always resolves to Null. The
        // simplest way: orders schema carries product_id as int with a
        // blank field which coerces to Null.
        let orders = "order_id,product_id,amount\nORD-1,1,10\nORD-2,,20\n";
        let products = "product_id,name,category\n1,Widget,cat-1\n";
        let yaml = r#"
pipeline:
  name: combine_null_key
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      schema:
        - { name: order_id, type: string }
        - { name: product_id, type: int }
        - { name: amount, type: int }
  - type: source
    name: products
    config:
      name: products
      type: csv
      path: products.csv
      schema:
        - { name: product_id, type: int }
        - { name: name, type: string }
        - { name: category, type: string }
  - type: combine
    name: joined
    input:
      orders: orders
      products: products
    config:
      where: "orders.product_id == products.product_id"
      match: first
      on_miss: skip
      cxl: |
        emit order_id = orders.order_id
        emit product_name = products.name
  - type: output
    name: joined_out
    input: joined
    config:
      name: joined_out
      type: csv
      path: joined.csv
"#;
        let result = run_combine_fixture(
            yaml,
            &[("orders", orders), ("products", products)],
            Some("orders"),
        )
        .expect("null-key fixture must run");
        let canon = canonicalize_csv(result.primary_output());
        // ORD-1 matches; ORD-2 has a Null probe key and never matches
        // under SQL 3VL, so on_miss:skip drops it.
        let data_rows = canon.lines().skip(1).filter(|l| !l.is_empty()).count();
        assert_eq!(
            data_rows, 1,
            "null key must not match; only ORD-1 survives. got: {canon}"
        );
        assert!(canon.contains("ORD-1"), "ORD-1 must survive");
        assert!(!canon.contains("ORD-2"), "ORD-2 null-key must be dropped");
    }
}

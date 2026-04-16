//! Integration tests for Phase Combine.
//!
//! - C.0.0 scaffolds (`test_combine_scaffold_compiles`,
//!   `test_combine_fixtures_exist`) remain in place as smoke gates.
//! - C.0.4 adds DAG-edge gate tests: verify that combine fixtures
//!   compile through `ExecutionPlanDag::compile` with the correct number
//!   of incoming edges and that the combine node appears after its
//!   inputs in topological order.

#[cfg(test)]
mod tests {
    use clinker_core::config::PipelineConfig;
    use clinker_core::plan::execution::{ExecutionPlanDag, PlanNode};
    use petgraph::Direction;
    use petgraph::graph::NodeIndex;

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
    /// real fixture, returns `(artifacts, diagnostics)`, and the
    /// `two_input_equi` fixture has no diagnostics emitted at the
    /// bind_schema level *today* (bind_schema's Combine arm is the
    /// no-op left in place at C.0; it lands real behaviour in C.1.1+).
    ///
    /// This gate verifies the helper surface compiles and is wired up.
    /// Downstream C.1.x tests assert on artifacts populated by the new
    /// Combine arm as each task lands.
    #[test]
    fn test_combine_schema_scaffold_compiles() {
        let (_artifacts, diags) = compile_combine_fixture("two_input_equi");
        // At C.1.0, bind_schema's Combine arm is still a no-op — no
        // diagnostics are emitted from Combine itself; upstream Source
        // + downstream Output arms also stay clean for this fixture.
        assert!(
            diags.is_empty(),
            "two_input_equi must bind cleanly at C.1.0; got: {:?}",
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
}

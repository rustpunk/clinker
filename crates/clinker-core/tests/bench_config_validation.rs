use clinker_bench_support::workspace_root;
use clinker_core::config::{
    AggregateStrategyHint, CompileContext, InputFormat, NodeInput, OutputFormat, PipelineNode,
    SchemaSource, parse_config,
};
use std::fs;

/// Validates every YAML pipeline config in benches/pipelines/ (except future/)
/// parses successfully with parse_config(). Catches YAML syntax issues and
/// config validation failures early.
#[test]
fn test_all_bench_pipeline_configs_parse() {
    let root = workspace_root().join("benches/pipelines");
    let mut count = 0;
    for entry in glob::glob(root.join("**/*.yaml").to_str().unwrap()).unwrap() {
        let path = entry.unwrap();
        if path.components().any(|c| c.as_os_str() == "future") {
            continue;
        }
        let yaml = fs::read_to_string(&path).unwrap();
        parse_config(&yaml).unwrap_or_else(|e| {
            panic!("Failed to parse {}: {e}", path.display());
        });
        count += 1;
    }
    assert!(
        count > 0,
        "No pipeline configs found — check workspace_root()"
    );
}

/// Verifies that a deliberately-broken sentinel YAML in future/ does NOT cause
/// the harness to fail. Guards against glob skip regression.
#[test]
fn test_future_dir_excluded_from_parse_harness() {
    let root = workspace_root().join("benches/pipelines/future");
    let mut future_count = 0;
    for entry in glob::glob(root.join("*.yaml").to_str().unwrap()).unwrap() {
        let _path = entry.unwrap();
        future_count += 1;
    }
    assert!(
        future_count > 0,
        "Expected at least one YAML in future/ to prove exclusion works"
    );
}

/// format/ contains at least one CSV, JSON, XML, and fixed-width input config.
#[test]
fn test_all_four_formats_represented() {
    let root = workspace_root().join("benches/pipelines/format");
    let mut has_csv = false;
    let mut has_json = false;
    let mut has_xml = false;
    let mut has_fixed_width = false;

    for entry in glob::glob(root.join("*.yaml").to_str().unwrap()).unwrap() {
        let path = entry.unwrap();
        let yaml = fs::read_to_string(&path).unwrap();
        let config = parse_config(&yaml).unwrap();
        for source in config.source_configs() {
            match source.format.format_name() {
                "csv" => has_csv = true,
                "json" => has_json = true,
                "xml" => has_xml = true,
                "fixed_width" => has_fixed_width = true,
                _ => {}
            }
        }
    }

    assert!(has_csv, "No CSV input config found in format/");
    assert!(has_json, "No JSON input config found in format/");
    assert!(has_xml, "No XML input config found in format/");
    assert!(
        has_fixed_width,
        "No fixed-width input config found in format/"
    );
}

/// fixed_width_passthrough.yaml has inline format_schema with 2 FieldDef entries.
#[test]
fn test_fixed_width_inline_schema_materializes() {
    let root = workspace_root().join("benches/pipelines/format/fixed_width_passthrough.yaml");
    let yaml = fs::read_to_string(&root).unwrap();
    let config = parse_config(&yaml).unwrap();
    let source = config.source_configs().next().expect("no source node");

    match &source.schema {
        Some(SchemaSource::Inline(def)) => {
            let fields = def.fields.as_ref().expect("no fields in inline schema");
            assert_eq!(fields.len(), 2, "expected 2 FieldDef entries");
            assert_eq!(fields[0].start, Some(0));
            assert_eq!(fields[0].width, Some(10));
            assert_eq!(fields[1].start, Some(10));
            assert_eq!(fields[1].width, Some(10));
        }
        Some(SchemaSource::FilePath(p)) => {
            panic!("expected Inline schema, got FilePath({p})")
        }
        None => panic!("expected format_schema to be present"),
    }
}

/// cross_format_csv_to_xml.yaml: source is CSV, output is XML.
#[test]
fn test_cross_format_csv_to_xml_shapes() {
    let root = workspace_root().join("benches/pipelines/format/cross_format_csv_to_xml.yaml");
    let yaml = fs::read_to_string(&root).unwrap();
    let config = parse_config(&yaml).unwrap();

    let source = config.source_configs().next().expect("no source node");
    assert!(
        matches!(source.format, InputFormat::Csv(_)),
        "expected CSV input, got {}",
        source.format.format_name()
    );

    let output = config.output_configs().next().expect("no output node");
    assert!(
        matches!(output.format, OutputFormat::Xml(_)),
        "expected XML output, got {}",
        output.format.format_name()
    );
}

/// cxl_ops/ contains all 10 expected file stems.
#[test]
fn test_cxl_ops_category_complete() {
    let root = workspace_root().join("benches/pipelines/cxl_ops");
    let expected: std::collections::HashSet<&str> = [
        "string_methods",
        "numeric_methods",
        "date_methods",
        "type_conversions",
        "conditionals",
        "coalesce",
        "filter_selectivity",
        "distinct_full",
        "distinct_by_field",
        "complex_realistic",
    ]
    .into_iter()
    .collect();

    let mut found = std::collections::HashSet::new();
    for entry in glob::glob(root.join("*.yaml").to_str().unwrap()).unwrap() {
        let path = entry.unwrap();
        let stem = path.file_stem().unwrap().to_str().unwrap().to_string();
        found.insert(stem);
    }

    for name in &expected {
        assert!(found.contains(*name), "Missing CXL ops config: {name}.yaml");
    }
}

/// filter_selectivity.yaml has exactly 3 Transform nodes.
#[test]
fn test_filter_selectivity_has_three_transforms() {
    let path = workspace_root().join("benches/pipelines/cxl_ops/filter_selectivity.yaml");
    let yaml = fs::read_to_string(&path).unwrap();
    let config = parse_config(&yaml).unwrap();

    let transform_count = config
        .nodes
        .iter()
        .filter(|n| matches!(&n.value, PipelineNode::Transform { .. }))
        .count();

    assert_eq!(
        transform_count, 3,
        "expected 3 Transform nodes, found {transform_count}"
    );
}

/// complex_realistic.yaml CXL body contains >= 31 emit lines.
#[test]
fn test_complex_realistic_has_31_emits() {
    let path = workspace_root().join("benches/pipelines/cxl_ops/complex_realistic.yaml");
    let yaml = fs::read_to_string(&path).unwrap();
    let config = parse_config(&yaml).unwrap();

    let mut emit_count = 0;
    for node in &config.nodes {
        if let PipelineNode::Transform { config: body, .. } = &node.value {
            let src: &str = body.cxl.as_ref();
            emit_count += src
                .lines()
                .filter(|l| l.trim().starts_with("emit "))
                .count();
        }
    }

    assert!(
        emit_count >= 31,
        "expected >= 31 emit lines in complex_realistic, found {emit_count}"
    );
}

// ── Task 3.3 gate tests ──────────────────────────────────────────

/// future/ directory has expected stub files.
#[test]
fn test_future_stubs_exist() {
    let root = workspace_root().join("benches/pipelines/future");
    let stubs: Vec<_> = glob::glob(root.join("*.yaml").to_str().unwrap())
        .unwrap()
        .filter_map(Result::ok)
        .collect();
    assert!(
        stubs.len() >= 2,
        "Expected at least 2 future stubs, found {}",
        stubs.len()
    );
}

/// For every config with a Route node, every condition key and the default
/// value has a downstream node connected via route.port.
#[test]
fn test_multi_output_route_targets_match_outputs() {
    let root = workspace_root().join("benches/pipelines");
    for entry in glob::glob(root.join("**/*.yaml").to_str().unwrap()).unwrap() {
        let path = entry.unwrap();
        if path.components().any(|c| c.as_os_str() == "future") {
            continue;
        }
        let yaml = fs::read_to_string(&path).unwrap();
        let config = parse_config(&yaml).unwrap();

        for node in &config.nodes {
            if let PipelineNode::Route {
                header,
                config: body,
            } = &node.value
            {
                let route_name = &header.name;

                let expected_port = |port: &str| NodeInput::Port {
                    node: route_name.clone(),
                    port: port.to_string(),
                };

                // Helper: check if any node has this port as its input
                let has_downstream = |expected: &NodeInput| {
                    config.nodes.iter().any(|n| match &n.value {
                        PipelineNode::Output { header, .. }
                        | PipelineNode::Transform { header, .. }
                        | PipelineNode::Aggregate { header, .. }
                        | PipelineNode::Route { header, .. } => header.input == *expected,
                        _ => false,
                    })
                };

                // Each condition key should have a downstream node connected via route.port
                for key in body.conditions.keys() {
                    let expected = expected_port(key);
                    assert!(
                        has_downstream(&expected),
                        "{}: route condition '{}' has no downstream node connected to port '{}.{}'",
                        path.display(),
                        key,
                        route_name,
                        key,
                    );
                }
                // Default should also have matching downstream
                let expected_default = expected_port(&body.default);
                assert!(
                    has_downstream(&expected_default),
                    "{}: route default '{}' has no downstream node connected to port '{}.{}'",
                    path.display(),
                    body.default,
                    route_name,
                    body.default,
                );
            }
        }
    }
}

/// Correlated configs have sort_order and correlation_key.
#[test]
fn test_correlated_configs_have_sort_order_and_key() {
    let configs = [
        "execution_mode/sorted_streaming_correlated.yaml",
        "execution_mode/sorted_streaming_group_flush.yaml",
        "features/dlq_correlated.yaml",
    ];
    let root = workspace_root().join("benches/pipelines");
    for name in configs {
        let path = root.join(name);
        let yaml = fs::read_to_string(&path).unwrap_or_else(|e| panic!("{name}: {e}"));
        let config = parse_config(&yaml).unwrap_or_else(|e| panic!("{name}: {e}"));

        let source = config.source_configs().next().expect("no source");
        assert!(
            source.sort_order.as_ref().map_or(false, |s| !s.is_empty()),
            "{name}: source must have non-empty sort_order"
        );
        assert!(
            config.error_handling.correlation_key.is_some(),
            "{name}: error_handling must have correlation_key"
        );
    }
}

/// Two-pass configs have analytic_window with non-empty group_by.
#[test]
fn test_two_pass_configs_have_analytic_window_group_by() {
    let configs = [
        "execution_mode/two_pass_window_sum.yaml",
        "execution_mode/two_pass_window_all_fns.yaml",
        "execution_mode/two_pass_parallel_scaling.yaml",
    ];
    let root = workspace_root().join("benches/pipelines");
    for name in configs {
        let path = root.join(name);
        let yaml = fs::read_to_string(&path).unwrap_or_else(|e| panic!("{name}: {e}"));
        let config = parse_config(&yaml).unwrap_or_else(|e| panic!("{name}: {e}"));

        let has_window = config.nodes.iter().any(|n| {
            if let PipelineNode::Transform { config: body, .. } = &n.value {
                if let Some(window) = &body.analytic_window {
                    window
                        .get("group_by")
                        .and_then(|v| v.as_array())
                        .map_or(false, |a| !a.is_empty())
                } else {
                    false
                }
            } else {
                false
            }
        });
        assert!(
            has_window,
            "{name}: must have transform with analytic_window containing non-empty group_by"
        );
    }
}

/// Split configs have mutually exclusive threshold fields.
#[test]
fn test_split_config_max_fields_mutually_exclusive() {
    let root = workspace_root().join("benches/pipelines/features");

    // splitting_by_records: only max_records
    let yaml = fs::read_to_string(root.join("splitting_by_records.yaml")).unwrap();
    let config = parse_config(&yaml).unwrap();
    let output = config.output_configs().next().unwrap();
    let split = output.split.as_ref().expect("split config missing");
    assert!(
        split.max_records.is_some(),
        "splitting_by_records must set max_records"
    );
    assert!(
        split.max_bytes.is_none(),
        "splitting_by_records must NOT set max_bytes"
    );

    // splitting_by_bytes: only max_bytes
    let yaml = fs::read_to_string(root.join("splitting_by_bytes.yaml")).unwrap();
    let config = parse_config(&yaml).unwrap();
    let output = config.output_configs().next().unwrap();
    let split = output.split.as_ref().expect("split config missing");
    assert!(
        split.max_bytes.is_some(),
        "splitting_by_bytes must set max_bytes"
    );
    assert!(
        split.max_records.is_none(),
        "splitting_by_bytes must NOT set max_records"
    );

    // splitting_by_group_key: max_records + group_key
    let yaml = fs::read_to_string(root.join("splitting_by_group_key.yaml")).unwrap();
    let config = parse_config(&yaml).unwrap();
    let output = config.output_configs().next().unwrap();
    let split = output.split.as_ref().expect("split config missing");
    assert!(
        split.max_records.is_some(),
        "splitting_by_group_key must set max_records"
    );
    assert!(
        split.group_key.is_some(),
        "splitting_by_group_key must set group_key"
    );
}

/// validation_warn.yaml parses without error_handling block.
#[test]
fn test_validation_warn_without_error_handling_parses() {
    let path = workspace_root().join("benches/pipelines/features/validation_warn.yaml");
    let yaml = fs::read_to_string(&path).unwrap();
    let config = parse_config(&yaml).unwrap();
    // Default error_handling should be permissive (no DLQ required for warn)
    assert!(
        config.error_handling.dlq.is_none(),
        "validation_warn should have no DLQ config"
    );
}

/// Scale configs reference fields matching their filename convention.
#[test]
fn test_scale_narrow_and_wide_cxl_field_references() {
    let root = workspace_root().join("benches/pipelines/scale");

    // narrow_5fields: CXL references only f0..f4
    let yaml = fs::read_to_string(root.join("narrow_5fields.yaml")).unwrap();
    let config = parse_config(&yaml).unwrap();
    for node in &config.nodes {
        if let PipelineNode::Transform { config: body, .. } = &node.value {
            let src: &str = body.cxl.as_ref();
            // Should not reference f5 or higher
            assert!(
                !src.contains("f5"),
                "narrow_5fields should not reference f5+"
            );
        }
    }

    // wide_50fields: schema must declare exactly 50 columns (f0..f49)
    let yaml = fs::read_to_string(root.join("wide_50fields.yaml")).unwrap();
    let config = parse_config(&yaml).unwrap();
    let field_count = config
        .nodes
        .iter()
        .find_map(|n| match &n.value {
            PipelineNode::Source { config: body, .. } => Some(body.schema.columns.len()),
            _ => None,
        })
        .expect("no source in wide_50fields");
    assert_eq!(
        field_count, 50,
        "wide_50fields schema must declare exactly 50 columns, found {field_count}"
    );
    // CXL references should include at least one field with index >= 40
    let mut has_high_field = false;
    for node in &config.nodes {
        if let PipelineNode::Transform { config: body, .. } = &node.value {
            let src: &str = body.cxl.as_ref();
            if src.contains("f40") || src.contains("f41") || src.contains("f49") {
                has_high_field = true;
            }
        }
    }
    assert!(
        has_high_field,
        "wide_50fields must reference fields with index >= 40"
    );
}

/// Aggregate configs have correct strategy hints.
#[test]
fn test_aggregate_configs_have_correct_strategy() {
    let root = workspace_root().join("benches/pipelines/execution_mode");

    // hash aggregate configs should have Hash strategy
    for name in [
        "hash_aggregate_low_cardinality.yaml",
        "hash_aggregate_high_cardinality.yaml",
    ] {
        let yaml = fs::read_to_string(root.join(name)).unwrap();
        let config = parse_config(&yaml).unwrap();
        for node in &config.nodes {
            if let PipelineNode::Aggregate { config: body, .. } = &node.value {
                assert_eq!(
                    body.strategy,
                    AggregateStrategyHint::Hash,
                    "{name}: expected Hash strategy"
                );
            }
        }
    }

    // streaming_aggregate_presorted uses Hash strategy (data generator
    // can't produce sorted data; streaming aggregation requires presorted input)
    let yaml = fs::read_to_string(root.join("streaming_aggregate_presorted.yaml")).unwrap();
    let config = parse_config(&yaml).unwrap();
    for node in &config.nodes {
        if let PipelineNode::Aggregate { config: body, .. } = &node.value {
            assert_eq!(
                body.strategy,
                AggregateStrategyHint::Hash,
                "streaming_aggregate_presorted: expected Hash strategy"
            );
        }
    }
    assert!(
        config.source_configs().next().is_some(),
        "streaming_aggregate_presorted: must have a source"
    );
}

/// Plan-time compilation gate: a representative subset of bench configs must
/// survive `compile()` (topology + CXL type-checking), not just YAML parsing.
/// Guards against schema type mismatches (e.g. numeric ops on string fields).
#[test]
fn test_representative_configs_compile_plan() {
    let root = workspace_root();
    let ctx = CompileContext::new(&root);
    let bench_root = root.join("benches/pipelines");

    // Representative subset spanning CXL ops, routes, aggregates, and windows.
    let configs = [
        "cxl_ops/filter_selectivity.yaml",
        "cxl_ops/complex_realistic.yaml",
        "cxl_ops/numeric_methods.yaml",
        "cxl_ops/coalesce.yaml",
        "execution_mode/streaming_multi_output.yaml",
        "execution_mode/hash_aggregate_low_cardinality.yaml",
        "execution_mode/two_pass_window_sum.yaml",
        "features/validation_error.yaml",
    ];

    for name in configs {
        let path = bench_root.join(name);
        let yaml = fs::read_to_string(&path).unwrap_or_else(|e| panic!("{name}: {e}"));
        let config = parse_config(&yaml).unwrap_or_else(|e| panic!("{name}: parse error: {e}"));
        config.compile(&ctx).unwrap_or_else(|diags| {
            let msgs: Vec<String> = diags.iter().map(|d| format!("  {d:?}")).collect();
            panic!("{name}: compile failed:\n{}", msgs.join("\n"));
        });
    }
}

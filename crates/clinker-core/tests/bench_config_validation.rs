use clinker_bench_support::workspace_root;
use clinker_core::config::{InputFormat, OutputFormat, PipelineNode, SchemaSource, parse_config};
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

use std::fs;
use std::path::{Path, PathBuf};

use clinker_plan::config::{CompileContext, load_config, parse_config};
use clinker_plan::schema::{ExternalSchemaShape, source_schema_facts};
use clinker_schema::{
    CoverageFacet, CoverageStatus, ReportSubject, SchemaIndex, WarningKind, analyze_pipeline,
    analyze_pipeline_file, analyze_schema_file, parse_schema,
};

fn write_pipeline(root: &Path, schema_path: &Path) -> PathBuf {
    fs::write(root.join("input.csv"), "id\n1\n").unwrap();
    let pipeline = format!(
        "pipeline:\n  name: parity\nnodes:\n  - type: source\n    name: src\n    config:\n      name: src\n      type: csv\n      path: input.csv\n      schema: '{}'\n  - type: output\n    name: out\n    input: src\n    config:\n      name: out\n      type: csv\n      path: output.csv\n",
        schema_path.display()
    );
    let path = root.join("pipeline.yaml");
    fs::write(&path, pipeline).unwrap();
    path
}

fn planner_compiles(root: &Path, pipeline: &Path) -> bool {
    let config = match load_config(pipeline) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("planner load failed: {error}");
            return false;
        }
    };
    match config.compile(&CompileContext::new(root)) {
        Ok(_) => true,
        Err(diagnostics) => {
            eprintln!("planner compile failed: {diagnostics:#?}");
            false
        }
    }
}

#[test]
fn tracer_analyzed_advisory_does_not_claim_planner_outcome() {
    let dir = tempfile::tempdir().unwrap();
    let schema_path = dir.path().join("advisory.schema.yaml");
    fs::write(
        &schema_path,
        "_schema:\n  name: advisory\n  format: csv\nfields:\n  - { name: id, type: int, nullable: false }\n",
    )
    .unwrap();
    let pipeline = write_pipeline(dir.path(), &schema_path);

    let analysis = analyze_schema_file(&schema_path);
    assert_eq!(analysis.report.subject, ReportSubject::Schema);
    assert_eq!(analysis.report.status, CoverageStatus::Analyzed);
    assert!(analysis.schema.is_some());
    assert!(!planner_compiles(dir.path(), &pipeline));
}

#[test]
fn tracer_planner_shape_is_reported_as_partial_not_as_advisory_success() {
    let dir = tempfile::tempdir().unwrap();
    let schema_path = dir.path().join("planner.schema.yaml");
    fs::write(&schema_path, "- { name: id, type: int }\n").unwrap();
    let pipeline = write_pipeline(dir.path(), &schema_path);

    let analysis = analyze_schema_file(&schema_path);
    assert_eq!(analysis.report.status, CoverageStatus::Partial);
    assert!(analysis.schema.is_none());
    assert!(
        analysis
            .report
            .unsupported_facets
            .contains(&CoverageFacet::PlannerSchemaShape)
    );
    let facts = source_schema_facts(&schema_path).expect("planner-owned parser facts");
    assert_eq!(facts.shape, ExternalSchemaShape::Columns);
    assert_eq!(facts.declared_columns, 1);
    assert!(planner_compiles(dir.path(), &pipeline));
}

#[test]
fn all_advisory_statuses_are_explicit_and_non_acceptance_sounding() {
    let dir = tempfile::tempdir().unwrap();

    let empty = dir.path().join("empty.schema.yaml");
    fs::write(&empty, "\n").unwrap();
    assert_eq!(
        analyze_schema_file(&empty).report.status,
        CoverageStatus::Skipped
    );

    let malformed = dir.path().join("malformed.schema.yaml");
    fs::write(&malformed, "_schema: [\n").unwrap();
    assert_eq!(
        analyze_schema_file(&malformed).report.status,
        CoverageStatus::Failed
    );

    let partial = dir.path().join("partial.schema.yaml");
    fs::write(
        &partial,
        "_schema:\n  name: partial\n  format: json\nfields:\n  - { name: values, type: array }\n",
    )
    .unwrap();
    assert_eq!(
        analyze_schema_file(&partial).report.status,
        CoverageStatus::Partial
    );

    for status in [
        CoverageStatus::Analyzed,
        CoverageStatus::Partial,
        CoverageStatus::Skipped,
        CoverageStatus::Failed,
    ] {
        let spelling = status.as_str();
        assert!(!spelling.contains("valid"));
        assert!(!spelling.contains("accept"));
        assert!(!spelling.contains("canonical"));
    }
}

#[test]
fn unresolved_structured_reference_is_reported_with_a_location_and_reason() {
    let dir = tempfile::tempdir().unwrap();
    let pipeline = write_pipeline(dir.path(), Path::new("missing.schema.yaml"));
    let report = analyze_pipeline_file(&pipeline, dir.path());

    assert_eq!(report.subject, ReportSubject::Pipeline);
    assert_eq!(report.status, CoverageStatus::Failed);
    assert_eq!(report.references.len(), 1);
    assert_eq!(
        report.references[0].schema_path,
        PathBuf::from("missing.schema.yaml")
    );
    assert_eq!(report.references[0].location.path, pipeline);
    assert!(
        report
            .reasons
            .iter()
            .any(|reason| reason.code == "unresolved-reference")
    );
}

#[test]
fn advisory_reports_and_reasons_are_deterministic() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("partial.schema.yaml");
    fs::write(
        &path,
        "_schema:\n  name: partial\n  format: parquet\nfields:\n  - { name: values, type: array }\n",
    )
    .unwrap();

    let first = analyze_schema_file(&path).report;
    let second = analyze_schema_file(&path).report;
    assert_eq!(first, second);
    assert_eq!(first.status, CoverageStatus::Partial);
    assert!(first.reasons.windows(2).all(|pair| {
        (pair[0].code, pair[0].message.as_str()) <= (pair[1].code, pair[1].message.as_str())
    }));
    assert!(
        first
            .reasons
            .iter()
            .all(|reason| reason.location.path == path && !reason.message.is_empty())
    );
}

#[test]
fn validation_reports_heuristic_reach_separately_from_warnings() {
    let dir = tempfile::tempdir().unwrap();
    let schema_path = dir.path().join("source.schema.yaml");
    let schema = parse_schema(
        "_schema:\n  name: source\n  format: csv\nfields:\n  - { name: id, type: int }\n",
        &schema_path,
    )
    .unwrap();
    let index = SchemaIndex::build(vec![schema]);
    let yaml = format!(
        "pipeline:\n  name: advisory\nnodes:\n  - type: source\n    name: src\n    config:\n      name: src\n      type: csv\n      path: input.csv\n      schema: '{}'\n  - type: transform\n    name: inspect\n    input: src\n    config:\n      cxl: 'emit result = missing_field'\n",
        schema_path.display()
    );
    let config = parse_config(&yaml).unwrap();

    let analysis = analyze_pipeline(&config, &index, dir.path(), Path::new("pipeline.yaml"));
    assert_eq!(analysis.report.status, CoverageStatus::Partial);
    assert!(
        analysis
            .report
            .unsupported_facets
            .contains(&CoverageFacet::TransformFieldScan)
    );
    assert!(
        analysis
            .warnings
            .iter()
            .any(|warning| warning.kind == WarningKind::FieldNotFound)
    );
}

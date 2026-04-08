//! Phase 14 end-to-end integration tests.
//!
//! Tests: $meta.* cross-transform, include_metadata output, file splitting by count,
//! key-group preservation, correlated DLQ, multi-output splitting, $meta in routes.

use std::collections::HashMap;
use std::io::{self, Cursor, Read, Write};
use std::sync::{Arc, Mutex};

use clinker_core::config::parse_config;
use clinker_core::executor::{PipelineExecutor, PipelineRunParams};

/// Test-only: enable absolute paths in compile() path validation. Tests
/// in this file use tempfile::tempdir() which produces absolute paths.
/// Call from any test that uses an absolute path in its YAML. Idempotent.
fn enable_absolute_paths() {
    // SAFETY: cargo test threads race on set_var; but this value is
    // idempotent ("1") and only ever set (never unset) within this test
    // binary, so concurrent writers observe the same string.
    unsafe {
        std::env::set_var("CLINKER_ALLOW_ABSOLUTE_PATHS", "1");
    }
}

/// Thread-safe in-memory buffer for capturing output in integration tests.
#[derive(Clone, Default)]
struct SharedBuffer(Arc<Mutex<Vec<u8>>>);

impl SharedBuffer {
    fn new() -> Self {
        Self::default()
    }

    fn as_string(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

impl Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.lock().unwrap().flush()
    }
}

/// Build PipelineRunParams from a parsed config.
fn test_params(config: &clinker_core::config::PipelineConfig) -> PipelineRunParams {
    let pipeline_vars = config
        .pipeline
        .vars
        .as_ref()
        .map(|v| clinker_core::config::convert_pipeline_vars(v))
        .unwrap_or_default();
    PipelineRunParams {
        execution_id: "integration-test".to_string(),
        batch_id: "batch-001".to_string(),
        pipeline_vars,
        shutdown_token: None,
    }
}

/// Run a single-input, single-output pipeline. Returns (report, output_csv).
fn run_single(yaml: &str, csv_input: &str) -> (clinker_core::executor::ExecutionReport, String) {
    let config = parse_config(yaml).unwrap();
    let params = test_params(&config);

    let readers: HashMap<String, Box<dyn Read + Send>> = HashMap::from([(
        config.source_configs().next().unwrap().name.clone(),
        Box::new(Cursor::new(csv_input.as_bytes().to_vec())) as Box<dyn Read + Send>,
    )]);

    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        config.output_configs().next().unwrap().name.clone(),
        Box::new(buf.clone()) as Box<dyn Write + Send>,
    )]);

    let report = PipelineExecutor::run_plan_with_readers_writers(
        &clinker_core::config::PipelineConfig::compile(&config).expect("compile"),
        readers,
        writers,
        &params,
    )
    .unwrap();
    (report, buf.as_string())
}

/// Run a multi-output pipeline. Returns (report, HashMap<output_name, csv_string>).
fn run_multi(
    yaml: &str,
    csv_input: &str,
) -> (
    clinker_core::executor::ExecutionReport,
    HashMap<String, String>,
) {
    let config = parse_config(yaml).unwrap();
    let params = test_params(&config);

    let readers: HashMap<String, Box<dyn Read + Send>> = HashMap::from([(
        config.source_configs().next().unwrap().name.clone(),
        Box::new(Cursor::new(csv_input.as_bytes().to_vec())) as Box<dyn Read + Send>,
    )]);

    let buffers: HashMap<String, SharedBuffer> = config
        .output_configs()
        .map(|o| (o.name.clone(), SharedBuffer::new()))
        .collect();

    let writers: HashMap<String, Box<dyn Write + Send>> = buffers
        .iter()
        .map(|(name, buf)| (name.clone(), Box::new(buf.clone()) as Box<dyn Write + Send>))
        .collect();

    let report = PipelineExecutor::run_plan_with_readers_writers(
        &clinker_core::config::PipelineConfig::compile(&config).expect("compile"),
        readers,
        writers,
        &params,
    )
    .unwrap();

    let outputs: HashMap<String, String> = buffers
        .iter()
        .map(|(name, buf)| (name.clone(), buf.as_string()))
        .collect();

    (report, outputs)
}

// ---------------------------------------------------------------------------
// 14.4.1: $meta.* set in one transform, read in next, stripped from output
// ---------------------------------------------------------------------------

#[test]
fn test_e2e_meta_cross_transform() {
    let yaml = r#"
pipeline:
  name: meta_cross_transform
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
- type: transform
  name: tag
  input: src
  config:
    cxl: 'emit $meta.tier = "high"

      emit name = name

      emit value = value

      '
- type: transform
  name: use_meta
  input: tag
  config:
    cxl: 'emit name = name

      emit value = value

      emit tier_copy = $meta.tier

      '
- type: output
  name: out
  input: use_meta
  config:
    name: out
    path: output.csv
    type: csv
"#;
    let csv = "name,value\nAlice,100\nBob,200\n";
    let (report, output) = run_single(yaml, csv);

    assert_eq!(report.counters.ok_count, 2);

    // tier_copy should contain the value set by $meta.tier in the first transform
    assert!(
        output.contains("high"),
        "tier_copy should be 'high': {output}"
    );

    // $meta.tier should NOT appear in the output columns (stripped by default)
    let header = output.lines().next().unwrap();
    assert!(
        !header.contains("meta.tier"),
        "meta.tier should not be in output header: {header}"
    );
    assert!(
        header.contains("tier_copy"),
        "tier_copy should be in output header: {header}"
    );
}

// ---------------------------------------------------------------------------
// 14.4.2: include_metadata: true -> meta fields in output
// ---------------------------------------------------------------------------

#[test]
fn test_e2e_meta_include_in_output() {
    let yaml = r#"
pipeline:
  name: meta_include_output
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
- type: transform
  name: tag
  input: src
  config:
    cxl: 'emit $meta.tier = "gold"

      emit $meta.region = "US"

      emit name = name

      emit value = value

      '
- type: output
  name: out
  input: tag
  config:
    name: out
    path: output.csv
    type: csv
    include_metadata: all
"#;
    let csv = "name,value\nAlice,100\nBob,200\n";
    let (report, output) = run_single(yaml, csv);

    assert_eq!(report.counters.ok_count, 2);

    let header = output.lines().next().unwrap();
    assert!(
        header.contains("meta.tier"),
        "include_metadata: all should add meta.tier to header: {header}"
    );
    assert!(
        header.contains("meta.region"),
        "include_metadata: all should add meta.region to header: {header}"
    );

    // Values should be present in data rows
    let lines: Vec<&str> = output.lines().collect();
    assert!(
        lines[1].contains("gold"),
        "row 1 should contain 'gold': {}",
        lines[1]
    );
    assert!(
        lines[1].contains("US"),
        "row 1 should contain 'US': {}",
        lines[1]
    );
}

// ---------------------------------------------------------------------------
// 14.4.3: file splitting by record count
// ---------------------------------------------------------------------------

#[test]
fn test_e2e_split_by_count() {
    enable_absolute_paths();
    let dir = tempfile::tempdir().unwrap();
    let output_path = dir.path().join("results.csv");

    let yaml = format!(
        r#"
pipeline:
  name: split_by_count
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
- type: transform
  name: passthrough
  input: src
  config:
    cxl: 'emit id = id

      emit value = value

      '
- type: output
  name: out
  input: passthrough
  config:
    name: out
    path: '{}'
    type: csv
    split:
      max_records: 30
"#,
        output_path.display()
    );

    // Generate 100 records
    let mut csv = String::from("id,value\n");
    for i in 1..=100 {
        csv.push_str(&format!("{i},{}\n", i * 10));
    }

    let config = parse_config(&yaml).unwrap();
    let params = test_params(&config);

    let readers: HashMap<String, Box<dyn Read + Send>> = HashMap::from([(
        "src".to_string(),
        Box::new(Cursor::new(csv.as_bytes().to_vec())) as Box<dyn Read + Send>,
    )]);

    // For split output, the writer is unused (SplittingWriter creates files).
    // We still need to provide one to satisfy the API.
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn Write + Send>,
    )]);

    let report = PipelineExecutor::run_plan_with_readers_writers(
        &clinker_core::config::PipelineConfig::compile(&config).expect("compile"),
        readers,
        writers,
        &params,
    )
    .unwrap();
    assert_eq!(report.counters.ok_count, 100);

    // Check split files: 30 + 30 + 30 + 10 = 4 files
    let mut split_files: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "csv"))
        .collect();
    split_files.sort_by_key(|e| e.path());

    assert_eq!(
        split_files.len(),
        4,
        "100 records / 30 per file = 4 files, got: {:?}",
        split_files.iter().map(|e| e.path()).collect::<Vec<_>>()
    );

    // Verify record counts per file (header + data rows)
    let counts: Vec<usize> = split_files
        .iter()
        .map(|e| {
            let content = std::fs::read_to_string(e.path()).unwrap();
            content.lines().count() - 1 // subtract header
        })
        .collect();
    assert_eq!(counts, vec![30, 30, 30, 10], "record counts: {counts:?}");

    // Each file should have a header
    for entry in &split_files {
        let content = std::fs::read_to_string(entry.path()).unwrap();
        let header = content.lines().next().unwrap();
        assert!(
            header.contains("id") && header.contains("value"),
            "each split should have header: {header}"
        );
    }
}

// ---------------------------------------------------------------------------
// 14.4.4: file splitting preserving key groups
// ---------------------------------------------------------------------------

#[test]
fn test_e2e_split_preserves_groups() {
    enable_absolute_paths();
    let dir = tempfile::tempdir().unwrap();
    let output_path = dir.path().join("grouped.csv");

    let yaml = format!(
        r#"
pipeline:
  name: split_groups
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    sort_order:
    - employee_id
- type: transform
  name: passthrough
  input: src
  config:
    cxl: 'emit emp = employee_id

      emit val = value

      '
- type: output
  name: out
  input: passthrough
  config:
    name: out
    path: '{}'
    type: csv
    split:
      max_records: 5
      group_key: emp
"#,
        output_path.display()
    );

    // 4 employees with varying group sizes, sorted by employee_id
    // A: 3 records, B: 4 records, C: 2 records, D: 3 records = 12 total
    // With max_records=5 and group_key=emp:
    //   File 1: A(3) + B(4) = 7 records (B starts after limit, stays together)
    //   File 2: C(2) + D(3) = 5 records
    let csv = "employee_id,value\n\
        A,1\nA,2\nA,3\n\
        B,4\nB,5\nB,6\nB,7\n\
        C,8\nC,9\n\
        D,10\nD,11\nD,12\n";

    let config = parse_config(&yaml).unwrap();
    let params = test_params(&config);

    let readers: HashMap<String, Box<dyn Read + Send>> = HashMap::from([(
        "src".to_string(),
        Box::new(Cursor::new(csv.as_bytes().to_vec())) as Box<dyn Read + Send>,
    )]);

    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn Write + Send>,
    )]);

    let report = PipelineExecutor::run_plan_with_readers_writers(
        &clinker_core::config::PipelineConfig::compile(&config).expect("compile"),
        readers,
        writers,
        &params,
    )
    .unwrap();
    assert_eq!(report.counters.ok_count, 12);

    let mut split_files: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "csv"))
        .collect();
    split_files.sort_by_key(|e| e.path());

    // Verify no employee group is split across files
    for entry in &split_files {
        let content = std::fs::read_to_string(entry.path()).unwrap();
        let mut emp_ids: Vec<&str> = Vec::new();
        for line in content.lines().skip(1) {
            // First field is emp
            let emp = line.split(',').next().unwrap();
            emp_ids.push(emp);
        }
        // Check contiguity: each unique emp should appear in a contiguous block
        let unique_emps: Vec<&str> = emp_ids
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        for emp in &unique_emps {
            let positions: Vec<usize> = emp_ids
                .iter()
                .enumerate()
                .filter(|(_, e)| *e == emp)
                .map(|(i, _)| i)
                .collect();
            if positions.len() > 1 {
                let first = positions[0];
                let last = positions[positions.len() - 1];
                assert_eq!(
                    last - first + 1,
                    positions.len(),
                    "emp {emp} should be contiguous in file {}",
                    entry.path().display()
                );
            }
        }
    }

    // Total records across all files should be 12
    let total: usize = split_files
        .iter()
        .map(|e| {
            let content = std::fs::read_to_string(e.path()).unwrap();
            content.lines().count() - 1
        })
        .sum();
    assert_eq!(total, 12, "total records across all split files");
}

// ---------------------------------------------------------------------------
// 14.4.5: correlated DLQ with sorted input
// ---------------------------------------------------------------------------

#[test]
fn test_e2e_correlated_dlq() {
    let yaml = r#"
pipeline:
  name: correlated_dlq_e2e
error_handling:
  strategy: continue
  correlation_key: employee_id
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
- type: transform
  name: validate
  input: src
  config:
    cxl: 'emit emp = employee_id

      emit val = value.to_int()

      '
- type: output
  name: out
  input: validate
  config:
    name: out
    path: output.csv
    type: csv
    include_unmapped: true
"#;

    // Group A: 3 records, one bad -> entire group DLQ'd
    // Group B: 2 records, all good -> emitted
    // Group C: 1 record, bad -> DLQ'd
    let csv = "employee_id,value\n\
        A,100\nA,bad\nA,300\n\
        B,400\nB,500\n\
        C,oops\n";

    let config = parse_config(yaml).unwrap();
    let params = test_params(&config);

    let readers: HashMap<String, Box<dyn Read + Send>> = HashMap::from([(
        "src".to_string(),
        Box::new(Cursor::new(csv.as_bytes().to_vec())) as Box<dyn Read + Send>,
    )]);

    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn Write + Send>,
    )]);

    let report = PipelineExecutor::run_plan_with_readers_writers(
        &clinker_core::config::PipelineConfig::compile(&config).expect("compile"),
        readers,
        writers,
        &params,
    )
    .unwrap();
    let output = buf.as_string();

    // Group A (3) + Group C (1) = 4 DLQ'd
    assert_eq!(report.counters.dlq_count, 4, "4 records DLQ'd");
    // Group B (2) emitted
    assert_eq!(report.counters.ok_count, 2, "2 records emitted");

    // Output should contain group B
    assert!(output.contains("B"), "output has group B: {output}");
    assert!(
        !output.contains(",bad"),
        "output should not contain bad: {output}"
    );

    // DLQ entries: check trigger flags
    assert_eq!(report.dlq_entries.len(), 4);
    let root_causes = report.dlq_entries.iter().filter(|e| e.trigger).count();
    assert_eq!(root_causes, 2, "2 root causes (bad in A, oops in C)");
    let collaterals = report.dlq_entries.iter().filter(|e| !e.trigger).count();
    assert_eq!(collaterals, 2, "2 collateral records from group A");

    // Collateral entries should mention "correlated with failure"
    let collateral = report.dlq_entries.iter().find(|e| !e.trigger).unwrap();
    assert!(
        collateral
            .error_message
            .contains("correlated with failure in group"),
        "collateral reason: {}",
        collateral.error_message
    );
}

// ---------------------------------------------------------------------------
// 14.4.6: multi-output + file splitting (each output splits independently)
// ---------------------------------------------------------------------------

#[test]
fn test_e2e_multi_output_split() {
    enable_absolute_paths();
    let dir = tempfile::tempdir().unwrap();
    let high_path = dir.path().join("high.csv");
    let low_path = dir.path().join("low.csv");

    let yaml = format!(
        r#"
pipeline:
  name: multi_output_split
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
- type: transform
  name: classify_emit
  input: src
  config:
    cxl: 'emit id = id

      emit amount_val = amount.to_int()

      '
- type: route
  name: classify
  input: classify_emit
  config:
    conditions:
      high: amount_val > 100
    default: low
- type: output
  name: high
  input: classify
  config:
    name: high
    path: '{}'
    type: csv
    split:
      max_records: 3
- type: output
  name: low
  input: classify
  config:
    name: low
    path: '{}'
    type: csv
    split:
      max_records: 4
"#,
        high_path.display(),
        low_path.display()
    );

    // 5 high (amount > 100), 6 low (amount <= 100) = 11 total
    let csv = "id,amount\n\
        1,200\n2,50\n3,300\n4,10\n5,400\n6,20\n7,500\n8,30\n9,600\n10,40\n11,50\n";

    let (report, _) = run_multi(&yaml, csv);
    assert_eq!(report.counters.ok_count, 11);

    // High files: 5 records / 3 per file = 2 files
    let high_files: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("high")
        })
        .collect();
    assert_eq!(
        high_files.len(),
        2,
        "5 high records / 3 per file = 2 files, got {}",
        high_files.len()
    );

    // Low files: 6 records / 4 per file = 2 files
    let low_files: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("low")
        })
        .collect();
    assert_eq!(
        low_files.len(),
        2,
        "6 low records / 4 per file = 2 files, got {}",
        low_files.len()
    );
}

// ---------------------------------------------------------------------------
// 14.4.7: $meta.* used in route conditions
// ---------------------------------------------------------------------------

#[test]
fn test_e2e_meta_in_route_condition() {
    let yaml = r#"
pipeline:
  name: meta_route
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
- type: transform
  name: tag_tier_emit
  input: src
  config:
    cxl: 'let amt = amount.to_int()

      emit $meta.tier = if amt > 1000 then "high" else "low"

      emit name = name

      emit amount = amount

      '
- type: route
  name: tag_tier
  input: tag_tier_emit
  config:
    conditions:
      premium: $meta.tier == 'high'
    default: standard
- type: output
  name: premium
  input: tag_tier
  config:
    name: premium
    path: premium.csv
    type: csv
- type: output
  name: standard
  input: tag_tier
  config:
    name: standard
    path: standard.csv
    type: csv
"#;

    let csv = "name,amount\nAlice,5000\nBob,200\nCarol,3000\nDave,50\n";
    let (report, outputs) = run_multi(yaml, csv);

    assert_eq!(report.counters.ok_count, 4);

    let premium = &outputs["premium"];
    let standard = &outputs["standard"];

    // Alice (5000) and Carol (3000) -> premium
    assert!(
        premium.contains("Alice"),
        "premium should have Alice: {premium}"
    );
    assert!(
        premium.contains("Carol"),
        "premium should have Carol: {premium}"
    );

    // Bob (200) and Dave (50) -> standard
    assert!(
        standard.contains("Bob"),
        "standard should have Bob: {standard}"
    );
    assert!(
        standard.contains("Dave"),
        "standard should have Dave: {standard}"
    );

    // Cross-check: premium should NOT have Bob/Dave
    assert!(
        !premium.contains("Bob"),
        "premium should not have Bob: {premium}"
    );
    assert!(
        !standard.contains("Alice"),
        "standard should not have Alice: {standard}"
    );
}

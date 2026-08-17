use std::process::Command;

use clinker_plan::config::{CompileContext, PipelineConfig, parse_config};

#[allow(dead_code)]
#[path = "../src/credential_profile.rs"]
mod credential_profile;

use credential_profile::admit_uncredentialed_run_capabilities;

fn clinker_bin() -> &'static str {
    env!("CARGO_BIN_EXE_clinker")
}

fn direct_file_plan() -> clinker_plan::plan::CompiledPlan {
    let config: PipelineConfig = parse_config(
        r#"
pipeline: { name: zero_credential_admission }
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      schema: [{ name: id, type: string }]
"#,
    )
    .expect("fixture parses");
    config
        .compile(&CompileContext::default())
        .expect("fixture compiles")
}

#[test]
fn zero_credential_plan_admits_exact_compiled_group_capacity() {
    let plan = direct_file_plan();
    let activation = plan.dag().source_activation();
    let group = &activation.groups()[0];

    let mut admitted =
        admit_uncredentialed_run_capabilities(&plan).expect("credential-free plan admits");
    let active = admitted
        .take_group(group.id())
        .expect("compiled group transfers once");

    assert_eq!(active.capacity(), group.capacity());
    assert_eq!(active.capacity().resource_units(), 1);
    assert_eq!(active.capacity().opener_units(), 1);
    assert_eq!(active.capacity().credential_handle_units(), 0);
}

#[test]
fn credential_free_file_run_crosses_admitted_executor_boundary() {
    let workspace = tempfile::tempdir().expect("workspace");
    std::fs::write(workspace.path().join("orders.csv"), "id\n1\n2\n").expect("source fixture");
    let pipeline = workspace.path().join("pipeline.yaml");
    std::fs::write(
        &pipeline,
        r#"pipeline: { name: admitted_cli_run }
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      schema: [{ name: id, type: string }]
  - type: output
    name: out
    input: orders
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#,
    )
    .expect("pipeline fixture");

    let output = Command::new(clinker_bin())
        .current_dir(workspace.path())
        .arg("run")
        .arg(&pipeline)
        .output()
        .expect("spawn clinker");

    assert!(
        output.status.success(),
        "credential-free run must be admitted; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        std::fs::read_to_string(workspace.path().join("out.csv")).expect("published output"),
        "id\n1\n2\n"
    );
}

#[test]
fn catalog_backed_body_source_runs_through_the_real_cli_factory() {
    let workspace = tempfile::tempdir().expect("workspace");
    std::fs::create_dir_all(workspace.path().join("compositions")).expect("composition directory");
    std::fs::create_dir_all(workspace.path().join("data")).expect("data directory");
    std::fs::write(workspace.path().join("driver.csv"), "seed\ngo\n").expect("driver fixture");
    std::fs::write(workspace.path().join("data/orders.txt"), "H0001\nD0002\n")
        .expect("catalog resource");
    std::fs::write(
        workspace.path().join("clinker.toml"),
        r#"[catalog.resources.orders]
kind = "file"
path = "data/orders.txt"
access = "read"
"#,
    )
    .expect("workspace config");
    std::fs::write(
        workspace.path().join("compositions/reader.comp.yaml"),
        r#"_compose:
  name: reader
  inputs: {}
  outputs: { out: read }
  config_schema: {}
  resources_schema:
    input: { kind: file, required: true }
nodes:
  - type: source
    name: read
    config:
      name: read
      type: fixed_width
      resource: input
      on_unmapped: { mode: drop }
      schema:
        discriminator: { start: 0, width: 1 }
        records:
          - id: header
            tag: H
            columns: [{ name: batch, type: string, start: 1, width: 4 }]
          - id: detail
            tag: D
            columns: [{ name: id, type: int, start: 1, width: 4 }]
"#,
    )
    .expect("composition fixture");
    let pipeline = workspace.path().join("pipeline.yaml");
    std::fs::write(
        &pipeline,
        r#"pipeline: { name: admitted_body_source_cli }
nodes:
  - type: source
    name: driver
    config:
      name: driver
      type: csv
      path: driver.csv
      schema: [{ name: seed, type: string }]
  - type: composition
    name: read_orders
    input: driver
    use: compositions/reader.comp.yaml
    inputs: {}
    resources: { input: orders }
  - type: output
    name: out
    input: read_orders
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: false
"#,
    )
    .expect("pipeline fixture");

    let output = Command::new(clinker_bin())
        .current_dir(workspace.path())
        .arg("run")
        .arg(&pipeline)
        .output()
        .expect("spawn clinker");

    assert!(
        output.status.success(),
        "catalog-backed body Source must run; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let published =
        std::fs::read_to_string(workspace.path().join("out.csv")).expect("published output");
    assert!(published.contains("0001"), "{published:?}");
    assert!(published.contains('2'), "{published:?}");
    assert!(!published.contains("orders.txt"));
}

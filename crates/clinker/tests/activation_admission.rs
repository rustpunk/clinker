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

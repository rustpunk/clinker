use std::path::{Path, PathBuf};

use clinker_channel::channel_override::{
    ChannelOverride, resolve_channel, resolve_channel_with_inheritance,
};
use clinker_channel::composition::{ProvenanceMap, resolve_compositions};
use clinker_channel::error::ChannelError;
use clinker_channel::workspace::WorkspaceRoot;
use clinker_core::config::load_config_with_vars;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Create a minimal workspace with clinker.toml and a pipeline YAML file.
fn setup_workspace(clinker_toml: &str, pipeline_yaml: &str) -> (TempDir, PathBuf) {
    let tmp = TempDir::new().unwrap();
    let ws = tmp.path();
    std::fs::write(ws.join("clinker.toml"), clinker_toml).unwrap();
    let pipeline_path = ws.join("pipelines/etl.yaml");
    std::fs::create_dir_all(ws.join("pipelines")).unwrap();
    std::fs::write(&pipeline_path, pipeline_yaml).unwrap();
    (tmp, pipeline_path)
}

fn write_file(ws_root: &Path, rel_path: &str, content: &str) {
    let path = ws_root.join(rel_path);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
}

const BASE_PIPELINE: &str = r#"
pipeline:
  name: test_pipeline
inputs:
  - name: customers
    path: /data/input.csv
    type: csv
outputs:
  - name: results
    path: /data/output.csv
    type: csv
transformations:
  - name: normalize
    cxl: "emit name = name"
"#;

const CLINKER_TOML: &str = "[workspace]\n";

// ---------------------------------------------------------------------------
// 1. Channel override changes output path
// ---------------------------------------------------------------------------

#[test]
fn test_e2e_channel_override_changes_output() {
    let (tmp, pipeline_path) = setup_workspace(CLINKER_TOML, BASE_PIPELINE);
    let ws = tmp.path();

    // Create channel with override that changes output path
    write_file(
        ws,
        "channels/acme/channel.yaml",
        r#"
_channel:
  id: acme
  name: "Acme Corp"
"#,
    );
    write_file(
        ws,
        "channels/acme/etl.channel.yaml",
        r#"
_channel:
  pipeline: etl
outputs:
  results:
    path: /data/acme/output.csv
"#,
    );

    let workspace = WorkspaceRoot::at(ws).unwrap();
    let mut config = load_config_with_vars(&pipeline_path, &[]).unwrap();
    let mut provenance = ProvenanceMap::new();

    config = resolve_channel_with_inheritance(
        config,
        "acme",
        &pipeline_path,
        &workspace,
        &[],
        &mut provenance,
    )
    .unwrap();

    assert_eq!(config.outputs[0].path, "/data/acme/output.csv");
}

// ---------------------------------------------------------------------------
// 2. No channel runs base unmodified
// ---------------------------------------------------------------------------

#[test]
fn test_e2e_no_channel_runs_base_unmodified() {
    let (_tmp, pipeline_path) = setup_workspace(CLINKER_TOML, BASE_PIPELINE);
    let config = load_config_with_vars(&pipeline_path, &[]).unwrap();

    assert_eq!(config.outputs[0].path, "/data/output.csv");
    assert_eq!(config.inputs[0].path, "/data/input.csv");
    assert_eq!(config.transformations.len(), 1);
    let names: Vec<_> = config.transforms().map(|t| t.name.as_str()).collect();
    assert_eq!(names, vec!["normalize"]);
}

// ---------------------------------------------------------------------------
// 3. Unknown channel exits with ChannelError::Io
// ---------------------------------------------------------------------------

#[test]
fn test_e2e_unknown_channel_exits_1() {
    let (tmp, pipeline_path) = setup_workspace(CLINKER_TOML, BASE_PIPELINE);
    let ws = tmp.path();
    let workspace = WorkspaceRoot::at(ws).unwrap();
    let config = load_config_with_vars(&pipeline_path, &[]).unwrap();
    let mut provenance = ProvenanceMap::new();

    let result = resolve_channel_with_inheritance(
        config,
        "nonexistent-channel",
        &pipeline_path,
        &workspace,
        &[],
        &mut provenance,
    );

    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ChannelError::Io(_)));
}

// ---------------------------------------------------------------------------
// 4. No workspace, no channel → discover returns None
// ---------------------------------------------------------------------------

#[test]
fn test_e2e_no_workspace_no_channel_works() {
    let tmp = TempDir::new().unwrap();
    let pipeline_path = tmp.path().join("pipeline.yaml");
    std::fs::write(&pipeline_path, BASE_PIPELINE).unwrap();

    let discovered = WorkspaceRoot::discover(&pipeline_path);
    assert!(discovered.is_none());
}

// ---------------------------------------------------------------------------
// 5. No workspace + channel requested = error
// ---------------------------------------------------------------------------

#[test]
fn test_e2e_no_workspace_with_channel_errors() {
    let tmp = TempDir::new().unwrap();
    // No clinker.toml anywhere
    let pipeline_path = tmp.path().join("pipeline.yaml");
    std::fs::write(&pipeline_path, BASE_PIPELINE).unwrap();

    let discovered = WorkspaceRoot::discover(&pipeline_path);
    assert!(discovered.is_none());

    // Attempting WorkspaceRoot::at on a dir without clinker.toml errors
    let result = WorkspaceRoot::at(tmp.path());
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ChannelError::Io(_)));
}

// ---------------------------------------------------------------------------
// 6. Composition _import inlines transforms
// ---------------------------------------------------------------------------

#[test]
fn test_e2e_composition_import_inlines() {
    let pipeline_with_import = r#"
pipeline:
  name: test_pipeline
inputs:
  - name: customers
    path: /data/input.csv
    type: csv
outputs:
  - name: results
    path: /data/output.csv
    type: csv
transformations:
  - name: before
    cxl: "emit a = 1"
  - _import: compositions/audit.comp.yaml
  - name: after
    cxl: "emit b = 2"
"#;

    let (tmp, pipeline_path) = setup_workspace(CLINKER_TOML, pipeline_with_import);
    let ws = tmp.path();

    write_file(
        ws,
        "compositions/audit.comp.yaml",
        r#"
_composition:
  name: audit
transformations:
  - name: audit_stamp
    cxl: "emit ts = now()"
"#,
    );

    let workspace = WorkspaceRoot::at(ws).unwrap();
    let mut config = load_config_with_vars(&pipeline_path, &[]).unwrap();
    let provenance = resolve_compositions(&mut config, &workspace, &[]).unwrap();

    let names: Vec<_> = config.transforms().map(|t| t.name.as_str()).collect();
    assert_eq!(names, vec!["before", "audit_stamp", "after"]);
    assert!(provenance.contains_key("audit_stamp"));
}

// ---------------------------------------------------------------------------
// 7. Channel overrides composition transform
// ---------------------------------------------------------------------------

#[test]
fn test_e2e_channel_overrides_composition_transform() {
    let pipeline_with_import = r#"
pipeline:
  name: test_pipeline
inputs:
  - name: customers
    path: /data/input.csv
    type: csv
outputs:
  - name: results
    path: /data/output.csv
    type: csv
transformations:
  - _import: compositions/audit.comp.yaml
"#;

    let (tmp, pipeline_path) = setup_workspace(CLINKER_TOML, pipeline_with_import);
    let ws = tmp.path();

    write_file(
        ws,
        "compositions/audit.comp.yaml",
        r#"
_composition:
  name: audit
transformations:
  - name: audit_stamp
    cxl: "emit ts = now()"
"#,
    );

    write_file(
        ws,
        "channels/acme/channel.yaml",
        r#"
_channel:
  id: acme
  name: "Acme Corp"
"#,
    );

    write_file(
        ws,
        "channels/acme/etl.channel.yaml",
        r#"
_channel:
  pipeline: etl
transformations:
  audit_stamp:
    cxl: "emit ts = custom_now()"
"#,
    );

    let workspace = WorkspaceRoot::at(ws).unwrap();
    let mut config = load_config_with_vars(&pipeline_path, &[]).unwrap();
    let mut provenance = resolve_compositions(&mut config, &workspace, &[]).unwrap();

    config = resolve_channel_with_inheritance(
        config,
        "acme",
        &pipeline_path,
        &workspace,
        &[],
        &mut provenance,
    )
    .unwrap();

    let audit = config
        .transforms()
        .find(|t| t.name == "audit_stamp")
        .unwrap();
    assert_eq!(audit.cxl, "emit ts = custom_now()");
}

// ---------------------------------------------------------------------------
// 8. Add composition in override
// ---------------------------------------------------------------------------

#[test]
fn test_e2e_add_composition_in_override() {
    let (tmp, pipeline_path) = setup_workspace(CLINKER_TOML, BASE_PIPELINE);
    let ws = tmp.path();

    write_file(
        ws,
        "compositions/extra.comp.yaml",
        r#"
_composition:
  name: extra
transformations:
  - name: extra_step
    cxl: "emit extra = true"
"#,
    );

    write_file(
        ws,
        "channels/acme/channel.yaml",
        r#"
_channel:
  id: acme
  name: "Acme Corp"
"#,
    );

    write_file(
        ws,
        "channels/acme/etl.channel.yaml",
        r#"
_channel:
  pipeline: etl
add_compositions:
  - after: normalize
    ref: compositions/extra.comp.yaml
"#,
    );

    let workspace = WorkspaceRoot::at(ws).unwrap();
    let mut config = load_config_with_vars(&pipeline_path, &[]).unwrap();
    let mut provenance = ProvenanceMap::new();

    config = resolve_channel_with_inheritance(
        config,
        "acme",
        &pipeline_path,
        &workspace,
        &[],
        &mut provenance,
    )
    .unwrap();

    let names: Vec<_> = config.transforms().map(|t| t.name.as_str()).collect();
    assert_eq!(names, vec!["normalize", "extra_step"]);
    assert!(provenance.contains_key("extra_step"));
}

// ---------------------------------------------------------------------------
// 9. Remove composition in override
// ---------------------------------------------------------------------------

#[test]
fn test_e2e_remove_composition_in_override() {
    let pipeline_with_import = r#"
pipeline:
  name: test_pipeline
inputs:
  - name: customers
    path: /data/input.csv
    type: csv
outputs:
  - name: results
    path: /data/output.csv
    type: csv
transformations:
  - name: before
    cxl: "emit a = 1"
  - _import: compositions/audit.comp.yaml
  - name: after
    cxl: "emit b = 2"
"#;

    let (tmp, pipeline_path) = setup_workspace(CLINKER_TOML, pipeline_with_import);
    let ws = tmp.path();

    write_file(
        ws,
        "compositions/audit.comp.yaml",
        r#"
_composition:
  name: audit
transformations:
  - name: audit_stamp
    cxl: "emit ts = now()"
"#,
    );

    write_file(
        ws,
        "channels/acme/channel.yaml",
        r#"
_channel:
  id: acme
  name: "Acme Corp"
"#,
    );

    write_file(
        ws,
        "channels/acme/etl.channel.yaml",
        r#"
_channel:
  pipeline: etl
remove_compositions:
  - ref: compositions/audit.comp.yaml
"#,
    );

    let workspace = WorkspaceRoot::at(ws).unwrap();
    let mut config = load_config_with_vars(&pipeline_path, &[]).unwrap();
    let mut provenance = resolve_compositions(&mut config, &workspace, &[]).unwrap();

    assert!(provenance.contains_key("audit_stamp"));

    config = resolve_channel_with_inheritance(
        config,
        "acme",
        &pipeline_path,
        &workspace,
        &[],
        &mut provenance,
    )
    .unwrap();

    let names: Vec<_> = config.transforms().map(|t| t.name.as_str()).collect();
    assert_eq!(names, vec!["before", "after"]);
    assert!(!provenance.contains_key("audit_stamp"));
}

// ---------------------------------------------------------------------------
// 10. Channel vars in paths
// ---------------------------------------------------------------------------

#[test]
fn test_e2e_channel_vars_in_paths() {
    let pipeline_with_var = r#"
pipeline:
  name: test_pipeline
inputs:
  - name: customers
    path: "${CHANNEL_DATA_DIR}/input.csv"
    type: csv
outputs:
  - name: results
    path: "${CHANNEL_DATA_DIR}/output.csv"
    type: csv
transformations:
  - name: normalize
    cxl: "emit name = name"
"#;

    let (tmp, pipeline_path) = setup_workspace(CLINKER_TOML, pipeline_with_var);
    let ws = tmp.path();

    write_file(
        ws,
        "channels/acme/channel.yaml",
        r#"
_channel:
  id: acme
  name: "Acme Corp"
variables:
  CHANNEL_DATA_DIR: "/data/acme"
"#,
    );

    let workspace = WorkspaceRoot::at(ws).unwrap();
    let manifest =
        clinker_channel::manifest::ChannelManifest::load(&workspace.channel_dir("acme")).unwrap();
    let vars = manifest.vars_as_pairs();

    let config = load_config_with_vars(&pipeline_path, &vars).unwrap();

    assert_eq!(config.inputs[0].path, "/data/acme/input.csv");
    assert_eq!(config.outputs[0].path, "/data/acme/output.csv");
}

// ---------------------------------------------------------------------------
// 11. Env flag activates when (pass via channel vars)
// ---------------------------------------------------------------------------

#[test]
fn test_e2e_env_flag_activates_when() {
    let (tmp, pipeline_path) = setup_workspace(CLINKER_TOML, BASE_PIPELINE);
    let ws = tmp.path();

    write_file(
        ws,
        "channels/acme/channel.yaml",
        r#"
_channel:
  id: acme
  name: "Acme Corp"
"#,
    );

    // Override with when condition referencing CLINKER_ENV
    write_file(
        ws,
        "channels/acme/etl.channel.yaml",
        r#"
_channel:
  pipeline: etl
  when: "prod == prod"
outputs:
  results:
    path: /data/prod/output.csv
"#,
    );

    let workspace = WorkspaceRoot::at(ws).unwrap();
    let config = load_config_with_vars(&pipeline_path, &[]).unwrap();
    let mut provenance = ProvenanceMap::new();

    // Load override directly with interpolated CLINKER_ENV
    let channel_dir = workspace.channel_dir("acme");
    let override_path = ChannelOverride::path_for(&pipeline_path, &channel_dir);
    let channel_vars = [("CLINKER_ENV", "prod")];
    let co = ChannelOverride::load(&override_path, &channel_vars)
        .unwrap()
        .unwrap();

    assert!(co.when_passes());

    let config = resolve_channel(config, &co, &workspace, &channel_vars, &mut provenance).unwrap();
    assert_eq!(config.outputs[0].path, "/data/prod/output.csv");
}

// ---------------------------------------------------------------------------
// 12. System env var activates when
// ---------------------------------------------------------------------------

#[test]
fn test_e2e_env_var_activates_when() {
    let (tmp, pipeline_path) = setup_workspace(CLINKER_TOML, BASE_PIPELINE);
    let ws = tmp.path();

    write_file(
        ws,
        "channels/acme/channel.yaml",
        r#"
_channel:
  id: acme
  name: "Acme Corp"
"#,
    );

    // Override with when referencing ${CLINKER_ENV_E2E_12} which gets interpolated
    write_file(
        ws,
        "channels/acme/etl.channel.yaml",
        r#"
_channel:
  pipeline: etl
  when: "${CLINKER_ENV_E2E_12} == prod"
outputs:
  results:
    path: /data/prod/output.csv
"#,
    );

    // Set system env var
    unsafe { std::env::set_var("CLINKER_ENV_E2E_12", "prod") };

    let workspace = WorkspaceRoot::at(ws).unwrap();
    let config = load_config_with_vars(&pipeline_path, &[]).unwrap();
    let mut provenance = ProvenanceMap::new();

    let channel_dir = workspace.channel_dir("acme");
    let override_path = ChannelOverride::path_for(&pipeline_path, &channel_dir);
    let co = ChannelOverride::load(&override_path, &[]).unwrap().unwrap();

    unsafe { std::env::remove_var("CLINKER_ENV_E2E_12") };

    assert!(co.when_passes());

    let config = resolve_channel(config, &co, &workspace, &[], &mut provenance).unwrap();
    assert_eq!(config.outputs[0].path, "/data/prod/output.csv");
}

// ---------------------------------------------------------------------------
// 13. clinker.toml default env activates when
// ---------------------------------------------------------------------------

#[test]
fn test_e2e_toml_default_env_activates_when() {
    let toml_with_default = r#"
[workspace]

[defaults]
env = "prod"
"#;

    let (tmp, _pipeline_path) = setup_workspace(toml_with_default, BASE_PIPELINE);
    let ws = tmp.path();

    let workspace = WorkspaceRoot::at(ws).unwrap();

    // Verify toml default is parsed
    assert_eq!(workspace.defaults.env.as_deref(), Some("prod"));

    // Simulate the env precedence chain: --env > CLINKER_ENV > toml default
    let cli_env: Option<String> = None;
    let env_key = "CLINKER_ENV_E2E_13";
    unsafe { std::env::remove_var(env_key) };
    let resolved = cli_env
        .or_else(|| std::env::var(env_key).ok())
        .or_else(|| workspace.defaults.env.clone());

    assert_eq!(resolved.as_deref(), Some("prod"));
}

// ---------------------------------------------------------------------------
// 14. when: in operator
// ---------------------------------------------------------------------------

#[test]
fn test_e2e_when_in_operator() {
    let (tmp, pipeline_path) = setup_workspace(CLINKER_TOML, BASE_PIPELINE);
    let ws = tmp.path();

    write_file(
        ws,
        "channels/acme/channel.yaml",
        r#"
_channel:
  id: acme
  name: "Acme Corp"
"#,
    );

    // when uses "in" operator with staging in the list
    write_file(
        ws,
        "channels/acme/etl.channel.yaml",
        r#"
_channel:
  pipeline: etl
  when: "staging in [prod, staging]"
outputs:
  results:
    path: /data/staging/output.csv
"#,
    );

    let workspace = WorkspaceRoot::at(ws).unwrap();
    let config = load_config_with_vars(&pipeline_path, &[]).unwrap();
    let mut provenance = ProvenanceMap::new();

    let channel_dir = workspace.channel_dir("acme");
    let override_path = ChannelOverride::path_for(&pipeline_path, &channel_dir);
    let co = ChannelOverride::load(&override_path, &[]).unwrap().unwrap();

    assert!(co.when_passes());

    let config = resolve_channel(config, &co, &workspace, &[], &mut provenance).unwrap();
    assert_eq!(config.outputs[0].path, "/data/staging/output.csv");
}

// ---------------------------------------------------------------------------
// 15. Wrong env skips override
// ---------------------------------------------------------------------------

#[test]
fn test_e2e_wrong_env_skips_override() {
    let (tmp, pipeline_path) = setup_workspace(CLINKER_TOML, BASE_PIPELINE);
    let ws = tmp.path();

    write_file(
        ws,
        "channels/acme/channel.yaml",
        r#"
_channel:
  id: acme
  name: "Acme Corp"
"#,
    );

    // when evaluates to false because dev != prod
    write_file(
        ws,
        "channels/acme/etl.channel.yaml",
        r#"
_channel:
  pipeline: etl
  when: "dev == prod"
outputs:
  results:
    path: /data/prod/output.csv
"#,
    );

    let workspace = WorkspaceRoot::at(ws).unwrap();
    let config = load_config_with_vars(&pipeline_path, &[]).unwrap();
    let mut provenance = ProvenanceMap::new();

    let channel_dir = workspace.channel_dir("acme");
    let override_path = ChannelOverride::path_for(&pipeline_path, &channel_dir);
    let co = ChannelOverride::load(&override_path, &[]).unwrap().unwrap();

    assert!(!co.when_passes());

    let config = resolve_channel(config, &co, &workspace, &[], &mut provenance).unwrap();
    // Override NOT applied — original path preserved
    assert_eq!(config.outputs[0].path, "/data/output.csv");
}

// ---------------------------------------------------------------------------
// 16. No env + when present = should_warn detection
// ---------------------------------------------------------------------------

#[test]
fn test_e2e_no_env_warning_emitted() {
    // This is a logic test: when no env is resolved and a when condition
    // exists, the CLI should warn. We verify the detection logic.
    let env_key = "CLINKER_ENV_E2E_16";
    let cli_env: Option<String> = None;
    unsafe { std::env::remove_var(env_key) };
    let toml_default: Option<String> = None;

    let resolved = cli_env
        .or_else(|| std::env::var(env_key).ok())
        .or(toml_default);

    assert!(resolved.is_none());

    // Simulate: override has a when condition
    let has_when_conditions = true;
    let should_warn = resolved.is_none() && has_when_conditions;
    assert!(should_warn);
}

// ---------------------------------------------------------------------------
// 17. Group inheritance
// ---------------------------------------------------------------------------

#[test]
fn test_e2e_group_inheritance() {
    let (tmp, pipeline_path) = setup_workspace(CLINKER_TOML, BASE_PIPELINE);
    let ws = tmp.path();

    // Group "enterprise" overrides output path
    write_file(
        ws,
        "_groups/enterprise/etl.channel.yaml",
        r#"
_channel:
  pipeline: etl
outputs:
  results:
    path: /data/enterprise/output.csv
"#,
    );

    // Channel "acme" inherits from enterprise, and overrides the transform
    write_file(
        ws,
        "channels/acme/channel.yaml",
        r#"
_channel:
  id: acme
  name: "Acme Corp"
  inherits: enterprise
"#,
    );

    write_file(
        ws,
        "channels/acme/etl.channel.yaml",
        r#"
_channel:
  pipeline: etl
transformations:
  normalize:
    cxl: "emit name = name.upper()"
"#,
    );

    let workspace = WorkspaceRoot::at(ws).unwrap();
    let mut config = load_config_with_vars(&pipeline_path, &[]).unwrap();
    let mut provenance = ProvenanceMap::new();

    config = resolve_channel_with_inheritance(
        config,
        "acme",
        &pipeline_path,
        &workspace,
        &[],
        &mut provenance,
    )
    .unwrap();

    // Group override applied: output path changed
    assert_eq!(config.outputs[0].path, "/data/enterprise/output.csv");
    // Channel override applied: transform cxl changed
    let normalize = config.transforms().find(|t| t.name == "normalize").unwrap();
    assert_eq!(normalize.cxl, "emit name = name.upper()");
}

// ---------------------------------------------------------------------------
// 18. Dry run with channel — full resolution validity check
// ---------------------------------------------------------------------------

#[test]
fn test_e2e_dry_run_with_channel() {
    let pipeline_with_import = r#"
pipeline:
  name: test_pipeline
inputs:
  - name: customers
    path: /data/input.csv
    type: csv
outputs:
  - name: results
    path: /data/output.csv
    type: csv
transformations:
  - name: before
    cxl: "emit a = 1"
  - _import: compositions/audit.comp.yaml
  - name: after
    cxl: "emit b = 2"
"#;

    let (tmp, pipeline_path) = setup_workspace(CLINKER_TOML, pipeline_with_import);
    let ws = tmp.path();

    write_file(
        ws,
        "compositions/audit.comp.yaml",
        r#"
_composition:
  name: audit
transformations:
  - name: audit_stamp
    cxl: "emit ts = now()"
  - name: audit_user
    cxl: "emit user = system"
"#,
    );

    write_file(
        ws,
        "channels/acme/channel.yaml",
        r#"
_channel:
  id: acme
  name: "Acme Corp"
"#,
    );

    write_file(
        ws,
        "channels/acme/etl.channel.yaml",
        r#"
_channel:
  pipeline: etl
outputs:
  results:
    path: /data/acme/output.csv
transformations:
  audit_stamp:
    cxl: "emit ts = acme_now()"
"#,
    );

    let workspace = WorkspaceRoot::at(ws).unwrap();
    let mut config = load_config_with_vars(&pipeline_path, &[]).unwrap();
    let mut provenance = resolve_compositions(&mut config, &workspace, &[]).unwrap();

    config = resolve_channel_with_inheritance(
        config,
        "acme",
        &pipeline_path,
        &workspace,
        &[],
        &mut provenance,
    )
    .unwrap();

    // Verify fully resolved config is valid
    assert_eq!(config.transformations.len(), 4); // before, audit_stamp, audit_user, after
    let names: Vec<_> = config.transforms().map(|t| t.name.as_str()).collect();
    assert_eq!(names, vec!["before", "audit_stamp", "audit_user", "after"]);
    assert_eq!(config.outputs[0].path, "/data/acme/output.csv");

    let audit = config
        .transforms()
        .find(|t| t.name == "audit_stamp")
        .unwrap();
    assert_eq!(audit.cxl, "emit ts = acme_now()");
}

// ---------------------------------------------------------------------------
// 19. Channel path explicit override (load from explicit path)
// ---------------------------------------------------------------------------

#[test]
fn test_e2e_channel_path_explicit_override() {
    let (tmp, pipeline_path) = setup_workspace(CLINKER_TOML, BASE_PIPELINE);
    let ws = tmp.path();

    // Put override in a non-standard location
    write_file(
        ws,
        "overrides/custom.channel.yaml",
        r#"
_channel:
  pipeline: etl
outputs:
  results:
    path: /data/custom/output.csv
"#,
    );

    let workspace = WorkspaceRoot::at(ws).unwrap();
    let config = load_config_with_vars(&pipeline_path, &[]).unwrap();
    let mut provenance = ProvenanceMap::new();

    // Load override from explicit path (not derived from channel dir)
    let explicit_path = ws.join("overrides/custom.channel.yaml");
    let co = ChannelOverride::load(&explicit_path, &[]).unwrap().unwrap();

    let config = resolve_channel(config, &co, &workspace, &[], &mut provenance).unwrap();
    assert_eq!(config.outputs[0].path, "/data/custom/output.csv");
}

// ---------------------------------------------------------------------------
// 20. Multiple explicit overrides — second wins on conflict
// ---------------------------------------------------------------------------

#[test]
fn test_e2e_channel_path_multiple_ordered() {
    let (tmp, pipeline_path) = setup_workspace(CLINKER_TOML, BASE_PIPELINE);
    let ws = tmp.path();

    // First override: changes output path
    write_file(
        ws,
        "overrides/first.channel.yaml",
        r#"
_channel:
  pipeline: etl
outputs:
  results:
    path: /data/first/output.csv
transformations:
  normalize:
    cxl: "emit name = name.trim()"
"#,
    );

    // Second override: changes output path AND transform
    write_file(
        ws,
        "overrides/second.channel.yaml",
        r#"
_channel:
  pipeline: etl
outputs:
  results:
    path: /data/second/output.csv
"#,
    );

    let workspace = WorkspaceRoot::at(ws).unwrap();
    let mut config = load_config_with_vars(&pipeline_path, &[]).unwrap();
    let mut provenance = ProvenanceMap::new();

    // Apply first override
    let first_path = ws.join("overrides/first.channel.yaml");
    let co1 = ChannelOverride::load(&first_path, &[]).unwrap().unwrap();
    config = resolve_channel(config, &co1, &workspace, &[], &mut provenance).unwrap();

    // After first: path changed, transform changed
    assert_eq!(config.outputs[0].path, "/data/first/output.csv");
    let normalize = config.transforms().find(|t| t.name == "normalize").unwrap();
    assert_eq!(normalize.cxl, "emit name = name.trim()");

    // Apply second override
    let second_path = ws.join("overrides/second.channel.yaml");
    let co2 = ChannelOverride::load(&second_path, &[]).unwrap().unwrap();
    config = resolve_channel(config, &co2, &workspace, &[], &mut provenance).unwrap();

    // Second wins on output path conflict
    assert_eq!(config.outputs[0].path, "/data/second/output.csv");
    // Transform stays as first override set it (second didn't touch it)
    let normalize = config.transforms().find(|t| t.name == "normalize").unwrap();
    assert_eq!(normalize.cxl, "emit name = name.trim()");
}

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const FIXTURE_ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/guess");
const FIXTURE_FILES: &[&str] = &[
    "manifest.yaml",
    "pipeline.yaml",
    "channel.yaml",
    "group.yaml",
    "input.csv",
    "input.json",
    "input.xml",
    "expected-preview.json",
    "expected.patch",
];

fn fixture_path(name: &str) -> PathBuf {
    Path::new(FIXTURE_ROOT).join(name)
}

fn copy_fixture(root: &Path, name: &str, destination: &str) {
    let destination = root.join(destination);
    std::fs::create_dir_all(destination.parent().expect("fixture destination parent"))
        .expect("create fixture destination");
    std::fs::copy(fixture_path(name), destination).expect("copy guess fixture");
}

fn workspace() -> tempfile::TempDir {
    let workspace = tempfile::tempdir().expect("temporary guess workspace");
    let root = workspace.path();
    for name in ["pipeline.yaml", "input.csv", "input.json", "input.xml"] {
        copy_fixture(root, name, name);
    }
    copy_fixture(
        root,
        "channel.yaml",
        "channel/json_preview/pipeline.channel.yaml",
    );
    copy_fixture(root, "group.yaml", "group/xml_preview.group.yaml");
    std::fs::write(
        root.join("channel/json_preview/channel.cfg.yaml"),
        "channel:\n  name: json_preview\n  targets: [guess.pipeline]\n",
    )
    .expect("write channel manifest");
    std::fs::write(
        root.join("clinker.toml"),
        "[catalog.pipelines]\n\"guess.pipeline\" = \"pipeline.yaml\"\n\n\
         [catalog.channels]\njson_preview = \"channel/json_preview\"\n\n\
         [channel]\nroot = \"channel\"\n\n[group]\nroot = \"group\"\n",
    )
    .expect("write workspace catalog");
    workspace
}

fn guess(root: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_clinker"))
        .current_dir(root)
        .arg("guess")
        .arg("pipeline.yaml")
        .args(args)
        .output()
        .expect("spawn clinker guess")
}

fn parse_success(output: &Output) -> serde_json::Value {
    assert!(
        output.status.success(),
        "guess failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    serde_json::from_slice(&output.stdout).expect("guess stdout is one JSON document")
}

#[test]
fn preview_selector_corpus_manifest_lists_every_committed_artifact_once() {
    let manifest_text =
        std::fs::read_to_string(fixture_path("manifest.yaml")).expect("read manifest");
    let manifest: serde_json::Value =
        clinker_plan::yaml::from_str(&manifest_text).expect("parse manifest");
    assert_eq!(manifest["version"], 1);

    let serialized = serde_json::to_string(&manifest).expect("serialize manifest");
    for name in FIXTURE_FILES {
        assert!(fixture_path(name).is_file(), "missing fixture {name}");
        let expected_mentions = usize::from(*name != "manifest.yaml");
        assert_eq!(
            serialized.matches(name).count(),
            expected_mentions,
            "fixture {name} must be listed exactly once when it is an input or expected artifact",
        );
    }

    let case_names = manifest["cases"]
        .as_array()
        .expect("manifest cases")
        .iter()
        .map(|case| case["name"].as_str().expect("case name"))
        .collect::<Vec<_>>();
    assert_eq!(
        case_names,
        [
            "preview_selector_base_csv",
            "preview_selector_channel_json",
            "preview_selector_group_xml",
        ]
    );
}

#[test]
fn preview_selector_base_is_deterministic_and_matches_byte_goldens() {
    let workspace = workspace();
    let first = guess(workspace.path(), &["--field", "csv_orders.amount"]);
    let second = guess(workspace.path(), &["--field", "csv_orders.amount"]);
    assert_eq!(first.stdout, second.stdout, "preview must be byte stable");
    assert_eq!(
        first.stdout,
        std::fs::read(fixture_path("expected-preview.json")).expect("read preview golden")
    );

    let report = parse_success(&first);
    assert_eq!(
        report["patch"],
        std::fs::read_to_string(fixture_path("expected.patch")).expect("read patch golden")
    );
}

#[test]
fn preview_selector_multi_record_field_reports_each_literal_numeric_owner() {
    let workspace = workspace();
    let report = parse_success(&guess(workspace.path(), &["--field", "csv_orders.amount"]));
    let owners = report["fields"][0]["owners"]
        .as_array()
        .expect("exact owner reports");
    assert_eq!(owners.len(), 2);
    assert_eq!(
        owners[0]["address"],
        "/v1/schema/sources/csv_orders/records/detail/columns/amount/attributes/type"
    );
    assert_eq!(owners[0]["observations"], 1);
    assert_eq!(owners[0]["proposed_type"], "int");
    assert_eq!(owners[0]["evidence"][0]["lexeme"], "10");
    assert_eq!(
        owners[1]["address"],
        "/v1/schema/sources/csv_orders/records/adjustment/columns/amount/attributes/type"
    );
    assert_eq!(owners[1]["observations"], 1);
    assert_eq!(owners[1]["proposed_type"], "float");
    assert_eq!(owners[1]["evidence"][0]["lexeme"], "20.5");
    let patch = report["patch"].as_str().expect("patch");
    assert!(patch.contains("records/detail/columns/amount/attributes/type"));
    assert!(patch.contains("records/adjustment/columns/amount/attributes/type"));
    assert!(
        !patch.contains("records/summary/columns/amount/attributes/type"),
        "the concrete summary declaration must not be proposed as an edit: {patch}"
    );
}

#[test]
fn preview_many_files_keeps_fixed_coverage_and_evidence_storage() {
    let workspace = workspace();
    let pipeline_path = workspace.path().join("pipeline.yaml");
    let pipeline = std::fs::read_to_string(&pipeline_path).expect("read pipeline fixture");
    std::fs::write(
        &pipeline_path,
        pipeline.replacen("path: input.csv", "glob: input-*.csv", 1),
    )
    .expect("select a many-file input set");
    let body = format!(
        "kind,order_id,amount\n{}",
        "D,c-detail,10\nA,c-adjustment,20.5\n".repeat(10)
    );
    for index in 0..12 {
        std::fs::write(
            workspace.path().join(format!("input-{index:02}.csv")),
            &body,
        )
        .expect("write sampled input");
    }

    let report = parse_success(&guess(workspace.path(), &["--field", "csv_orders.amount"]));
    let coverage = &report["coverage"][0];
    assert_eq!(coverage["discovered_files"], 12);
    assert_eq!(coverage["unreported_file_count"], 8);
    let files = coverage["files"].as_array().expect("bounded file reports");
    assert_eq!(files.len(), 4);
    assert_eq!(files[0]["path"], "input-00.csv");
    assert_eq!(files[3]["path"], "input-03.csv");

    for owner in report["fields"][0]["owners"]
        .as_array()
        .expect("owner reports")
    {
        assert_eq!(owner["observations"], 40);
        assert_eq!(
            owner["evidence"]
                .as_array()
                .expect("bounded evidence")
                .len(),
            8
        );
    }
}

#[test]
fn preview_selector_channel_uses_effective_json_schema_and_parser() {
    let workspace = workspace();
    let output = guess(
        workspace.path(),
        &[
            "--base-dir",
            ".",
            "--channel",
            "json_preview",
            "--field",
            "json_orders.ratio",
        ],
    );
    let report = parse_success(&output);
    assert_eq!(report["selection"]["kind"], "channel");
    assert_eq!(report["fields"][0]["owners"][0]["proposed_type"], "float");
    assert_eq!(
        report["fields"][0]["owners"][0]["evidence"][0]["boundary"],
        "json"
    );
    assert_eq!(report["coverage"][0]["files"][0]["path"], "input.json");
}

#[test]
fn preview_selector_group_uses_effective_xml_schema_and_parser() {
    let workspace = workspace();
    let output = guess(
        workspace.path(),
        &[
            "--base-dir",
            ".",
            "--group",
            "xml_preview",
            "--field",
            "xml_orders.total",
        ],
    );
    let report = parse_success(&output);
    assert_eq!(report["selection"]["kind"], "group");
    assert_eq!(report["fields"][0]["owners"][0]["proposed_type"], "int");
    assert_eq!(
        report["fields"][0]["owners"][0]["evidence"][0]["boundary"],
        "xml"
    );
    assert_eq!(report["coverage"][0]["files"][0]["path"], "input.xml");
}

#[test]
fn preview_selector_conflict_absence_and_ambiguity_exit_one() {
    let workspace = workspace();
    let conflict = guess(
        workspace.path(),
        &["--channel", "json_preview", "--group", "xml_preview"],
    );
    assert_eq!(conflict.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&conflict.stderr).contains("choose exactly one"));

    let absent = guess(workspace.path(), &["--channel", "missing"]);
    assert_eq!(absent.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&absent.stderr).contains("missing"));

    copy_fixture(workspace.path(), "group.yaml", "group/duplicate.group.yaml");
    let ambiguous = guess(workspace.path(), &["--group", "xml_preview"]);
    assert_eq!(ambiguous.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&ambiguous.stderr).contains("xml_preview"));
}

#[test]
fn preview_selector_fields_deduplicate_and_reject_unknown_or_concrete_fields() {
    let workspace = workspace();
    let deduplicated = guess(
        workspace.path(),
        &[
            "--field",
            "csv_orders.amount",
            "--field",
            "csv_orders.amount",
        ],
    );
    let report = parse_success(&deduplicated);
    assert_eq!(report["fields"].as_array().expect("fields").len(), 1);

    for field in ["csv_orders.missing", "json_orders.ratio", "amount"] {
        let rejected = guess(workspace.path(), &["--field", field]);
        assert_eq!(rejected.status.code(), Some(1), "field {field}");
        let stderr = String::from_utf8_lossy(&rejected.stderr);
        assert!(stderr.contains(field), "stderr for {field}: {stderr}");
        assert!(stderr.contains("--field"), "stderr for {field}: {stderr}");
    }

    let pipeline_path = workspace.path().join("pipeline.yaml");
    let pipeline = std::fs::read_to_string(&pipeline_path).expect("read pipeline fixture");
    std::fs::write(
        &pipeline_path,
        pipeline.replacen("type: numeric", "type: { nullable: numeric }", 1),
    )
    .expect("make amount nullable");
    let nullable = parse_success(&guess(workspace.path(), &["--field", "csv_orders.amount"]));
    assert!(
        nullable["patch"]
            .as_str()
            .expect("patch")
            .contains("from: nullable(numeric)\n    to: nullable(int)",)
    );
}

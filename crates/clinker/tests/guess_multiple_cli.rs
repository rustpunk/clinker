use std::path::Path;
use std::process::{Command, Output};

fn guess(root: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_clinker"))
        .current_dir(root)
        .arg("guess")
        .arg("pipeline.yaml")
        .args(args)
        .output()
        .expect("spawn clinker guess")
}

fn write_pipeline(root: &Path, format: &str, options: &str, input: &str, extension: &str) {
    std::fs::write(
        root.join("pipeline.yaml"),
        format!(
            "pipeline:\n  name: multiplicity_guess\nnodes:\n  - type: source\n    name: values\n    config:\n      name: values\n      type: {format}\n      path: input.{extension}{options}\n      schema:\n        - name: tags\n          type: string\n"
        ),
    )
    .expect("write multiplicity pipeline");
    std::fs::write(root.join(format!("input.{extension}")), input)
        .expect("write multiplicity input");
}

fn parse_report(output: &Output) -> serde_json::Value {
    serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "guess stdout is not a JSON report: {error}\nstatus: {:?}\nstdout:\n{}\nstderr:\n{}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        )
    })
}

fn assert_written_multiplicity(root: &Path, expected_split: Option<&str>) {
    let output = guess(root, &["--write", "--field", "values.tags"]);
    assert_eq!(
        output.status.code(),
        Some(0),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    let report = parse_report(&output);
    assert_eq!(report["write"]["status"], "written");
    assert_eq!(report["multiplicity"][0]["outcome"], "conclusive");
    assert_eq!(report["multiplicity"][0]["multi_records"], 1);
    let config = std::fs::read_to_string(root.join("pipeline.yaml"))
        .expect("read written multiplicity pipeline");
    assert!(config.contains("multiple: true"), "{config}");
    match expected_split {
        Some(delimiter) => {
            assert!(config.contains("split_values:"), "{config}");
            assert!(
                config.contains(&format!("delimiter: {delimiter}")),
                "{config}"
            );
        }
        None => assert!(!config.contains("split_values:"), "{config}"),
    }
}

#[test]
fn tracer_xml_json_csv_multiplicity_evidence() {
    let xml = tempfile::tempdir().expect("temporary XML workspace");
    write_pipeline(
        xml.path(),
        "xml",
        "\n      options:\n        record_path: root/row",
        "<root><row><tags>a</tags></row><row><tags>a</tags><tags>b</tags></row></root>",
        "xml",
    );
    assert_written_multiplicity(xml.path(), None);

    let json = tempfile::tempdir().expect("temporary JSON workspace");
    write_pipeline(
        json.path(),
        "json",
        "\n      options:\n        format: array",
        r#"[{"tags":[]},{"tags":["a"]},{"tags":["a","b"]}]"#,
        "json",
    );
    assert_written_multiplicity(json.path(), None);

    let singleton_json = tempfile::tempdir().expect("temporary singleton JSON workspace");
    write_pipeline(
        singleton_json.path(),
        "json",
        "\n      options:\n        format: array",
        r#"[{"tags":[]},{"tags":["a"]},{"tags":["b"]}]"#,
        "json",
    );
    let before = std::fs::read(singleton_json.path().join("pipeline.yaml"))
        .expect("read singleton pipeline");
    let output = guess(
        singleton_json.path(),
        &["--write", "--field", "values.tags"],
    );
    assert_eq!(output.status.code(), Some(3));
    let report = parse_report(&output);
    assert_eq!(report["write"]["reason"], "unresolved_evidence");
    assert_eq!(report["multiplicity"][0]["outcome"], "unconfirmed");
    assert_eq!(
        std::fs::read(singleton_json.path().join("pipeline.yaml"))
            .expect("read unchanged singleton pipeline"),
        before
    );

    let csv = tempfile::tempdir().expect("temporary CSV workspace");
    write_pipeline(csv.path(), "csv", "", "tags\na|b\nc|d\n", "csv");
    assert_written_multiplicity(csv.path(), Some("'|'"));

    let ambiguous_csv = tempfile::tempdir().expect("temporary ambiguous CSV workspace");
    write_pipeline(
        ambiguous_csv.path(),
        "csv",
        "",
        "tags\na|b;c\nd|e;f\n",
        "csv",
    );
    let before = std::fs::read(ambiguous_csv.path().join("pipeline.yaml"))
        .expect("read ambiguous pipeline");
    let output = guess(
        ambiguous_csv.path(),
        &["--write", "--field", "values.tags"],
    );
    assert_eq!(output.status.code(), Some(3));
    let report = parse_report(&output);
    assert_eq!(report["write"]["reason"], "unresolved_evidence");
    assert_eq!(report["multiplicity"][0]["outcome"], "review_only");
    assert_eq!(report["multiplicity"][0]["reason"], "ambiguous_interpretation");
    assert_eq!(
        std::fs::read(ambiguous_csv.path().join("pipeline.yaml"))
            .expect("read unchanged ambiguous pipeline"),
        before
    );
}

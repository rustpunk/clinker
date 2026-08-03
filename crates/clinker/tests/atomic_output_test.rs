//! Atomic output: every output shape lands through destination-local staging
//! so a pipeline failure cannot leave a truncated final file behind.

use std::path::PathBuf;
use std::process::Command;

fn clinker_bin() -> &'static str {
    env!("CARGO_BIN_EXE_clinker")
}

#[test]
fn successful_run_leaves_final_path_only() {
    let dir = tempfile::tempdir().expect("tempdir");

    std::fs::write(dir.path().join("input.csv"), "id,name\n1,Alice\n2,Bob\n").expect("write input");

    let output_path = dir.path().join("out.csv");
    let pipeline_path = dir.path().join("pipeline.yaml");
    let pipeline = r#"pipeline:
  name: atomic_output_smoke
error_handling:
  strategy: continue
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
      - { name: id, type: int }
      - { name: name, type: string }
- type: output
  name: out
  input: src
  config:
    name: out
    path: out.csv
    type: csv
    include_unmapped: true
"#;
    std::fs::write(&pipeline_path, pipeline).expect("write pipeline");

    let output = Command::new(clinker_bin())
        .current_dir(dir.path())
        .arg("run")
        .arg(&pipeline_path)
        .output()
        .expect("spawn clinker");
    assert!(
        output.status.success(),
        "clinker run must succeed.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    assert!(
        output_path.exists(),
        "final output path must exist after success"
    );
    let body = std::fs::read_to_string(&output_path).expect("read output");
    assert!(
        body.contains("Alice") && body.contains("Bob"),
        "rows: {body}"
    );

    // Sweep the dir for hidden partial/reservation files — none should remain.
    let tmp_leftovers: Vec<PathBuf> = std::fs::read_dir(dir.path())
        .expect("readdir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with(".clinker-") || n.starts_with(".tmp"))
        })
        .collect();
    assert!(
        tmp_leftovers.is_empty(),
        "no temp files should remain after success: {tmp_leftovers:?}"
    );
}

#[test]
fn missing_input_run_does_not_create_final_path() {
    let dir = tempfile::tempdir().expect("tempdir");

    // Reader-creation failure: the CLI fails opening the input file
    // BEFORE writers / temp files are constructed, so no temp file
    // appears either. Critical contract: the final output path must
    // not exist.
    let output_path = dir.path().join("out.csv");
    let pipeline_path = dir.path().join("pipeline.yaml");
    let pipeline = r#"pipeline:
  name: atomic_output_failure_smoke
error_handling:
  strategy: continue
nodes:
- type: source
  name: src
  config:
    name: src
    path: does-not-exist.csv
    type: csv
    schema:
      - { name: id, type: int }
- type: output
  name: out
  input: src
  config:
    name: out
    path: out.csv
    type: csv
    include_unmapped: true
"#;
    std::fs::write(&pipeline_path, pipeline).expect("write pipeline");

    let status = Command::new(clinker_bin())
        .current_dir(dir.path())
        .arg("run")
        .arg(&pipeline_path)
        .status()
        .expect("spawn clinker");
    assert!(
        !status.success(),
        "missing-input run must fail with a non-zero exit"
    );

    // Final output must NOT exist — atomic rename was never performed.
    assert!(
        !output_path.exists(),
        "final output path must not exist after failure"
    );
}

#[test]
fn executor_failure_preserves_partial_tempfile() {
    let dir = tempfile::tempdir().expect("tempdir");

    // Input present; CXL `1 / 0` triggers a runtime DivisionByZero on
    // the first record, and `strategy: fail_fast` aborts the executor
    // immediately. This exercises the post-writer-construction failure
    // path, which is where the CLI must preserve the temp file with a
    // WARN log so an operator can inspect partial output.
    std::fs::write(dir.path().join("input.csv"), "id,name\n1,Alice\n2,Bob\n").expect("write input");
    let output_path = dir.path().join("out.csv");
    std::fs::write(&output_path, "previous valid output\n").expect("write previous output");
    let pipeline_path = dir.path().join("pipeline.yaml");
    let pipeline = r#"pipeline:
  name: atomic_output_runtime_failure
error_handling:
  strategy: fail_fast
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
      - { name: id, type: int }
      - { name: name, type: string }
- type: transform
  name: divzero
  input: src
  config:
    cxl: |
      emit id = id
      emit boom = id / 0
- type: output
  name: out
  input: divzero
  config:
    name: out
    path: out.csv
    type: csv
    include_unmapped: true
    if_exists: overwrite
"#;
    std::fs::write(&pipeline_path, pipeline).expect("write pipeline");

    let status = Command::new(clinker_bin())
        .current_dir(dir.path())
        .arg("run")
        .arg(&pipeline_path)
        .status()
        .expect("spawn clinker");
    assert!(
        !status.success(),
        "divzero pipeline must abort with non-zero exit"
    );

    assert_eq!(
        std::fs::read_to_string(&output_path).expect("read previous output"),
        "previous valid output\n",
        "overwrite must leave the previous final untouched until success"
    );

    // A destination-local hidden file remains so an operator can inspect the
    // attempted replacement.
    let leftovers: Vec<_> = std::fs::read_dir(dir.path())
        .expect("readdir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with(".clinker-out.csv-") && n.ends_with(".partial"))
        })
        .collect();
    assert!(
        !leftovers.is_empty(),
        "temp file must be preserved after runtime failure"
    );
}

#[test]
fn failed_force_run_preserves_existing_final() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join("input.csv"), "id\n1\n").expect("write input");
    std::fs::write(dir.path().join("out.csv"), "previous\n").expect("write previous output");
    let pipeline_path = dir.path().join("pipeline.yaml");
    std::fs::write(
        &pipeline_path,
        r#"pipeline:
  name: force_failure_preserves_final
error_handling:
  strategy: fail_fast
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
      - { name: id, type: int }
- type: transform
  name: fail
  input: src
  config:
    cxl: |
      emit boom = id / 0
- type: output
  name: out
  input: fail
  config:
    name: out
    path: out.csv
    type: csv
    if_exists: error
"#,
    )
    .expect("write pipeline");

    let output = Command::new(clinker_bin())
        .current_dir(dir.path())
        .arg("run")
        .arg(&pipeline_path)
        .arg("--force")
        .output()
        .expect("spawn clinker");

    assert!(!output.status.success(), "forced failing run must fail");
    assert_eq!(
        std::fs::read_to_string(dir.path().join("out.csv")).expect("read previous output"),
        "previous\n"
    );
}

#[test]
fn fan_out_outputs_publish_only_after_success() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join("input-a.csv"), "id\n1\n").expect("write input a");
    std::fs::write(dir.path().join("input-b.csv"), "id\n2\n").expect("write input b");
    for name in ["out_input-a.csv", "out_input-b.csv"] {
        std::fs::write(dir.path().join(name), "previous\n").expect("write previous fan-out");
    }
    let pipeline_path = dir.path().join("pipeline.yaml");
    std::fs::write(
        &pipeline_path,
        r#"pipeline:
  name: fan_out_atomic_commit
nodes:
- type: source
  name: src
  config:
    name: src
    glob: input-*.csv
    type: csv
    schema:
      - { name: id, type: int }
- type: output
  name: out
  input: src
  config:
    name: out
    path: out_{source_file}.csv
    type: csv
    if_exists: overwrite
"#,
    )
    .expect("write pipeline");

    let output = Command::new(clinker_bin())
        .current_dir(dir.path())
        .arg("run")
        .arg(&pipeline_path)
        .output()
        .expect("spawn clinker");
    assert!(
        output.status.success(),
        "fan-out run failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let output_a =
        std::fs::read_to_string(dir.path().join("out_input-a.csv")).expect("read fan-out a");
    let output_b =
        std::fs::read_to_string(dir.path().join("out_input-b.csv")).expect("read fan-out b");
    let files: Vec<_> = std::fs::read_dir(dir.path())
        .expect("read fan-out directory")
        .filter_map(Result::ok)
        .map(|entry| entry.file_name())
        .collect();
    assert!(
        output_a.contains('1'),
        "fan-out a: {output_a:?}; files={files:?}; stdout={}; stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output_b.contains('2'), "fan-out b: {output_b:?}");
}

#[test]
fn failed_fan_out_run_preserves_every_existing_final() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join("input-a.csv"), "id\n1\n").expect("write input a");
    std::fs::write(dir.path().join("input-b.csv"), "id\n2\n").expect("write input b");
    for name in ["out_input-a.csv", "out_input-b.csv"] {
        std::fs::write(dir.path().join(name), format!("previous {name}\n"))
            .expect("write previous fan-out");
    }
    let pipeline_path = dir.path().join("pipeline.yaml");
    std::fs::write(
        &pipeline_path,
        r#"pipeline:
  name: fan_out_failure_preserves_finals
error_handling:
  strategy: fail_fast
nodes:
- type: source
  name: src
  config:
    name: src
    glob: input-*.csv
    type: csv
    schema:
      - { name: id, type: int }
- type: transform
  name: fail
  input: src
  config:
    cxl: |
      emit boom = id / 0
- type: output
  name: out
  input: fail
  config:
    name: out
    path: out_{source_file}.csv
    type: csv
    if_exists: overwrite
"#,
    )
    .expect("write pipeline");

    let output = Command::new(clinker_bin())
        .current_dir(dir.path())
        .arg("run")
        .arg(&pipeline_path)
        .output()
        .expect("spawn clinker");
    assert!(!output.status.success(), "failing fan-out run must fail");
    for name in ["out_input-a.csv", "out_input-b.csv"] {
        assert_eq!(
            std::fs::read_to_string(dir.path().join(name)).expect("read previous fan-out"),
            format!("previous {name}\n")
        );
    }
}

#[test]
fn fan_out_rejects_a_linked_destination_parent() {
    let dir = tempfile::tempdir().expect("tempdir");
    let outside = tempfile::tempdir().expect("outside tempdir");
    std::fs::write(dir.path().join("input-a.csv"), "id\n1\n").expect("write input");
    #[cfg(unix)]
    std::os::unix::fs::symlink(outside.path(), dir.path().join("dest")).expect("link destination");
    #[cfg(windows)]
    {
        let status = Command::new("cmd")
            .args(["/C", "mklink", "/J"])
            .arg(dir.path().join("dest"))
            .arg(outside.path())
            .status()
            .expect("create destination junction");
        assert!(status.success());
    }
    let pipeline_path = dir.path().join("pipeline.yaml");
    std::fs::write(
        &pipeline_path,
        r#"pipeline:
  name: fan_out_link_rejected
nodes:
- type: source
  name: src
  config:
    name: src
    glob: input-*.csv
    type: csv
    schema:
      - { name: id, type: int }
- type: output
  name: out
  input: src
  config:
    name: out
    path: dest/{source_file}.csv
    type: csv
"#,
    )
    .expect("write pipeline");

    let output = Command::new(clinker_bin())
        .current_dir(dir.path())
        .arg("run")
        .arg(&pipeline_path)
        .output()
        .expect("spawn clinker");
    assert!(!output.status.success(), "linked fan-out parent must fail");
    assert!(
        std::fs::read_dir(outside.path())
            .expect("read outside")
            .next()
            .is_none(),
        "no external file may be created"
    );
}

#[test]
fn split_rollover_failure_preserves_existing_segment() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join("input.csv"), "id,group\n1,A\n2,A\n").expect("write input");
    std::fs::write(dir.path().join("result_0001.csv"), "previous segment\n")
        .expect("write previous segment");
    let pipeline_path = dir.path().join("pipeline.yaml");
    std::fs::write(
        &pipeline_path,
        r#"pipeline:
  name: split_failure_preserves_segment
error_handling:
  strategy: fail_fast
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
      - { name: id, type: int }
      - { name: group, type: string }
- type: output
  name: out
  input: src
  config:
    name: out
    path: result.csv
    type: csv
    if_exists: overwrite
    split:
      max_records: 1
      group_key: group
      oversize_group: error
"#,
    )
    .expect("write pipeline");

    let output = Command::new(clinker_bin())
        .current_dir(dir.path())
        .arg("run")
        .arg(&pipeline_path)
        .output()
        .expect("spawn clinker");

    assert!(!output.status.success(), "oversize split group must fail");
    assert_eq!(
        std::fs::read_to_string(dir.path().join("result_0001.csv")).expect("read previous segment"),
        "previous segment\n"
    );
    assert!(
        std::fs::read_dir(dir.path())
            .expect("read output directory")
            .filter_map(Result::ok)
            .any(|entry| entry
                .file_name()
                .to_string_lossy()
                .starts_with(".clinker-result_0001.csv-")),
        "failed split must retain a hidden segment partial"
    );
}

#[test]
fn unique_suffix_concurrent_runs_each_get_distinct_outputs() {
    // Spawn multiple `clinker run` processes simultaneously against the
    // same output directory with `if_exists: unique_suffix`. Each must
    // claim a distinct path. The reservation pattern in main.rs (via
    // open_output's OpenOptions::create_new walk) is what guarantees
    // race-safety; without it, two processes could both pick `out-1.csv`
    // and one would clobber the other on persist.
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join("input.csv"), "id,name\n1,Alice\n").expect("write input");

    let pipeline_path = dir.path().join("pipeline.yaml");
    let pipeline = r#"pipeline:
  name: unique_suffix_concurrent
error_handling:
  strategy: continue
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
      - { name: id, type: int }
      - { name: name, type: string }
- type: output
  name: out
  input: src
  config:
    name: out
    path: out.csv
    type: csv
    if_exists: unique_suffix
    include_unmapped: true
"#;
    std::fs::write(&pipeline_path, pipeline).expect("write pipeline");

    // Pre-touch the bare path so every process must walk to a suffix.
    std::fs::write(dir.path().join("out.csv"), "").expect("touch bare");

    let processes = 4;
    let handles: Vec<_> = (0..processes)
        .map(|_| {
            let dir_path = dir.path().to_path_buf();
            let pipeline_path = pipeline_path.clone();
            std::thread::spawn(move || {
                Command::new(clinker_bin())
                    .current_dir(&dir_path)
                    .arg("run")
                    .arg(&pipeline_path)
                    .output()
                    .expect("spawn clinker")
            })
        })
        .collect();

    for h in handles {
        let out = h.join().expect("thread join");
        assert!(
            out.status.success(),
            "concurrent clinker run failed.\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr),
        );
    }

    // Bare out.csv (the pre-touch) plus one file per concurrent process.
    let outputs: Vec<PathBuf> = std::fs::read_dir(dir.path())
        .expect("readdir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("out") && n.ends_with(".csv"))
        })
        .collect();
    assert_eq!(
        outputs.len(),
        processes + 1,
        "expected {} distinct output files (1 pre-touch + {processes} runs); got {outputs:?}",
        processes + 1,
    );

    // Every run produced a non-empty CSV with the input row — proves
    // no run silently lost its data to a clobber.
    for out in &outputs {
        let body = std::fs::read_to_string(out).unwrap_or_default();
        if out.file_name().and_then(|n| n.to_str()) == Some("out.csv") {
            // pre-touch: empty
            continue;
        }
        assert!(
            body.contains("Alice"),
            "{}: missing data; clobber suspected. content={body:?}",
            out.display(),
        );
    }
}

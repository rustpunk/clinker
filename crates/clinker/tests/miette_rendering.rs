//! CLI integration test for miette-rendered pipeline errors.
//!
//! The `clinker run <bad.yaml>` path must surface errors via the
//! miette graphical reporter with the source file attached. This
//! test shells out to the compiled `clinker` binary (`CARGO_BIN_EXE`
//! is injected by Cargo for integration tests) and asserts stderr
//! contains the config filename — proof that `NamedSource` reached
//! the report handler.
//!
//! A plan-time gate additionally has to keep its diagnostic code, its help
//! paragraph, and the YAML line it fired on. Those three are what let a user
//! reach `clinker explain --code <CODE>` and fix the config; the run path used
//! to drop all three by flattening the compile diagnostics into a message
//! list.

use std::process::Command;

/// Path to the `clinker` binary built by Cargo for this test run.
fn clinker_bin() -> &'static str {
    env!("CARGO_BIN_EXE_clinker")
}

#[test]
fn test_diagnostic_renders_via_miette_in_cli() {
    // Write a deliberately broken YAML config: the `nodes:` section
    // is missing entirely, so `serde-saphyr` rejects it at parse
    // time. The error surfaces through `PipelineError::Config` and
    // is rendered by `render_pipeline_error` via miette.
    let tmp = tempdir_path();
    let bad_yaml_path = tmp.join("bad_pipeline.yaml");
    std::fs::write(
        &bad_yaml_path,
        "pipeline:\n  name: broken\n# missing required `nodes:` field\n",
    )
    .expect("write bad yaml");

    let output = Command::new(clinker_bin())
        .arg("run")
        .arg(&bad_yaml_path)
        .output()
        .expect("spawn clinker");

    // Must have failed (non-zero exit).
    assert!(
        !output.status.success(),
        "clinker run on bad yaml must fail; got status {:?}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr),
    );

    let stderr = String::from_utf8_lossy(&output.stderr);

    // The rendered diagnostic must include the config filename.
    // `NamedSource` puts this in the diagnostic header; the
    // `WrappedPipelineError::Display` impl also injects it into the
    // main message, so either path satisfies the contract.
    assert!(
        stderr.contains("bad_pipeline.yaml"),
        "stderr must mention the config filename; got:\n{stderr}"
    );

    // And the miette diagnostic code marker must appear — proof
    // that the report went through miette's handler rather than
    // the fallback `tracing::error!` path.
    assert!(
        stderr.contains("clinker::pipeline_error"),
        "stderr must carry the miette diagnostic code; got:\n{stderr}"
    );

    // Cleanup.
    let _ = std::fs::remove_dir_all(&tmp);
}

#[test]
fn test_explain_cxl_type_error_renders_via_miette() {
    // Compile-time CXL typecheck must surface through the `--explain`
    // path as well. A transform that references an unknown
    // column against the declared schema is rejected at compile() time,
    // BEFORE any file on disk is read. The resulting diagnostic must
    // render via miette with the config filename in the header.
    let tmp = tempdir_path();
    let bad_yaml_path = tmp.join("cxl_type_error.yaml");
    std::fs::write(
        &bad_yaml_path,
        r#"pipeline:
  name: cxl_bad
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: data/definitely_not_on_disk.csv
      schema:
        - { name: amount, type: int }
  - type: transform
    name: t
    input: src
    config:
      cxl: "emit bogus = not_a_column + 1"
  - type: output
    name: out
    input: t
    config:
      name: out
      type: csv
      path: data/out.csv
"#,
    )
    .expect("write cxl_type_error yaml");

    let output = Command::new(clinker_bin())
        .arg("run")
        .arg(&bad_yaml_path)
        .arg("--explain")
        .output()
        .expect("spawn clinker");

    assert!(
        !output.status.success(),
        "clinker run --explain on cxl type error must fail; got status {:?}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr),
    );

    let stderr = String::from_utf8_lossy(&output.stderr);

    // File-location annotation: the config filename must appear in
    // miette's rendered output — proof that `NamedSource` reached the
    // report handler through the --explain path.
    assert!(
        stderr.contains("cxl_type_error.yaml"),
        "stderr must mention the config filename; got:\n{stderr}"
    );

    // The diagnostic's own code heads the report — proof it went through
    // miette carrying the structured diagnostic, not the tracing fallback
    // and not a flattened message list.
    assert!(
        stderr.contains("E203"),
        "stderr must carry the diagnostic's own code; got:\n{stderr}"
    );

    let _ = std::fs::remove_dir_all(&tmp);
}

/// A pipeline whose source declares a JSONPath-shaped `record_path`, which the
/// E363 grammar gate rejects at compile time.
fn jsonpath_record_path_yaml() -> &'static str {
    r#"pipeline:
  name: bad_record_path
nodes:
  - type: source
    name: src
    config:
      name: src
      type: json
      path: in.json
      options:
        record_path: "$.rows"
      schema:
        - { name: amount, type: int }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
"#
}

/// Remove SGR colour escapes. miette emits them on Linux and macOS and may
/// omit them elsewhere; either way they sit between the characters a needle
/// spans, so they are stripped rather than matched around.
fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(i) = rest.find('\u{1b}') {
        out.push_str(&rest[..i]);
        // CSI: `ESC [` then parameter bytes, terminated by an ASCII letter.
        let after = &rest[i + 1..];
        let after = after.strip_prefix('[').unwrap_or(after);
        match after.find(|c: char| c.is_ascii_alphabetic()) {
            Some(end) => rest = &after[end + 1..],
            None => {
                rest = "";
                break;
            }
        }
    }
    out.push_str(rest);
    out
}

/// Flatten a rendered report so an assertion tests the diagnostic rather than
/// the terminal it was rendered into.
///
/// miette hard-wraps its message and help paragraphs to the terminal width,
/// which differs between a developer's terminal and CI. A phrase that sits on
/// one line in one place arrives split in the other — and not merely across a
/// newline: each continuation line carries a `│` gutter marker, so the break
/// lands *inside* the phrase as `node names │ differ only in case`. Dropping
/// the frame characters and then collapsing every run of whitespace makes a
/// `contains` mean "this phrase is present", at any width and on any platform.
///
/// This matters as much for the negative assertions below as for the positive
/// ones: a wrapped occurrence of a heading would otherwise satisfy `!contains`
/// for the wrong reason.
fn flatten_report(stderr: &str) -> String {
    // miette's box-drawing frame: the continuation gutter, the snippet border,
    // and the label leaders. None of it is diagnostic content, and the theme
    // that selects these characters is platform-dependent.
    const FRAME: &[char] = &['│', '·', '╭', '╰', '╮', '╯', '─', '┬', '├', '┤'];
    strip_ansi(stderr)
        .replace(FRAME, " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// Whether a flattened report quotes a source snippet drawn from `file`.
///
/// A snippet header spells `<path>:<line>:<col>]`, so a digit right after the
/// filename is the giveaway — as distinct from the message prefix, which is
/// `<path>: <message>`.
fn quotes_a_snippet_from(flat: &str, file: &str) -> bool {
    flat.split(&format!("{file}:"))
        .skip(1)
        .any(|rest| rest.starts_with(|c: char| c.is_ascii_digit()))
}

/// Assert the three things a plan-time diagnostic must keep on the way to the
/// terminal, plus the absence of the transform-compilation misattribution.
fn assert_plan_diagnostic_intact(stderr: &str) {
    let flat = flatten_report(stderr);

    // 1. The code, so `clinker explain --code E363` is reachable from the
    //    error the user is looking at.
    assert!(
        flat.contains("E363"),
        "stderr must carry the diagnostic code; got:\n{stderr}"
    );

    // 2. The help paragraph naming the fix. Every gate attaches one; none of
    //    it used to reach the terminal. The needle is deliberately a phrase
    //    that appears only in the help and not in the message -- the message
    //    also describes the grammar, so a needle common to both would pass
    //    even with the help dropped again.
    assert!(
        flat.contains("help: `record_path` on a `json` source"),
        "stderr must render the help as help; got:\n{stderr}"
    );
    assert!(
        flat.contains("no empty segments. It takes precedence over `format:`"),
        "stderr must carry the whole help paragraph, not a prefix of it; \
         got:\n{stderr}"
    );

    // 3. The source line. The gate fires on the `- type: source` node, which
    //    is line 4 of the fixture, so miette's snippet must be anchored there
    //    and must quote that line.
    //
    //    Anchored on the header's trailing `:<line>:<col>]` rather than on the
    //    file path: the path is a long temp directory that differs per run and
    //    per platform (and, on Windows, in separator), while this token is
    //    short, ASCII, and unique in the report. The quoted line is checked by
    //    its YAML content for the same reason -- the gutter's box-drawing
    //    characters are theme-dependent.
    assert!(
        flat.contains(":4:1]"),
        "stderr must anchor the snippet at line 4, column 1; got:\n{stderr}"
    );
    assert!(
        flat.contains("- type: source"),
        "stderr must quote the offending YAML line; got:\n{stderr}"
    );

    // The failure is in a source's config, not a CXL transform -- and there is
    // no transform in this pipeline at all.
    assert!(
        !flat.contains("CXL compilation failed for transform"),
        "a source-config gate must not be labelled a transform compilation \
         failure; got:\n{stderr}"
    );
}

#[test]
fn test_plan_time_gate_keeps_code_help_and_span_on_the_run_path() {
    let tmp = tempdir_path();
    let yaml_path = tmp.join("bad_record_path.yaml");
    std::fs::write(&yaml_path, jsonpath_record_path_yaml()).expect("write yaml");

    let output = Command::new(clinker_bin())
        .arg("run")
        .arg(&yaml_path)
        .output()
        .expect("spawn clinker");

    assert!(
        !output.status.success(),
        "clinker run on a rejected record_path must fail; got status {:?}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr),
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_plan_diagnostic_intact(&stderr);

    let _ = std::fs::remove_dir_all(&tmp);
}

#[test]
fn test_plan_time_gate_keeps_code_help_and_span_under_explain() {
    let tmp = tempdir_path();
    let yaml_path = tmp.join("bad_record_path.yaml");
    std::fs::write(&yaml_path, jsonpath_record_path_yaml()).expect("write yaml");

    let output = Command::new(clinker_bin())
        .arg("run")
        .arg(&yaml_path)
        .arg("--explain")
        .output()
        .expect("spawn clinker");

    assert!(
        !output.status.success(),
        "clinker run --explain on a rejected record_path must fail; got status {:?}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr),
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_plan_diagnostic_intact(&stderr);

    let _ = std::fs::remove_dir_all(&tmp);
}

#[test]
fn test_plan_time_gate_keeps_code_help_and_span_under_dry_run() {
    let tmp = tempdir_path();
    let yaml_path = tmp.join("bad_record_path.yaml");
    std::fs::write(&yaml_path, jsonpath_record_path_yaml()).expect("write yaml");

    let output = Command::new(clinker_bin())
        .arg("run")
        .arg(&yaml_path)
        .arg("--dry-run")
        .output()
        .expect("spawn clinker");

    assert!(
        !output.status.success(),
        "dry-run must compile and reject the same plan-time gate as a real run; \
         got status {:?}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr),
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_plan_diagnostic_intact(&stderr);

    let _ = std::fs::remove_dir_all(&tmp);
}

#[test]
fn test_dry_run_empty_pipeline_returns_a_diagnostic_instead_of_panicking() {
    let tmp = tempdir_path();
    let yaml_path = tmp.join("empty.yaml");
    std::fs::write(&yaml_path, "pipeline:\n  name: empty\nnodes: []\n").expect("write yaml");

    let output = Command::new(clinker_bin())
        .arg("run")
        .arg(&yaml_path)
        .arg("--dry-run")
        .output()
        .expect("spawn clinker");

    assert!(
        !output.status.success(),
        "an empty pipeline must fail dry-run validation"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    let normalized_stderr = stderr.split_whitespace().collect::<Vec<_>>().join(" ");
    assert!(
        normalized_stderr.contains("pipeline must declare at least one source node"),
        "stderr must explain the source requirement; got:\n{stderr}"
    );
    assert!(
        !stderr.contains("panicked at"),
        "invalid user input must not panic; got:\n{stderr}"
    );

    let _ = std::fs::remove_dir_all(&tmp);
}

#[test]
fn test_bare_dry_run_does_not_require_input_or_create_output() {
    let tmp = tempdir_path();
    let yaml_path = tmp.join("no_io.yaml");
    let output_path = tmp.join("result.csv");
    std::fs::write(
        &yaml_path,
        r#"pipeline:
  name: no_io
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: missing.csv
      schema:
        - { name: amount, type: int }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: result.csv
"#,
    )
    .expect("write yaml");

    let output = Command::new(clinker_bin())
        .arg("run")
        .arg(&yaml_path)
        .arg("--dry-run")
        .output()
        .expect("spawn clinker");

    assert!(
        output.status.success(),
        "bare dry-run must stop before runtime source discovery; stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !output_path.exists(),
        "bare dry-run must not create the configured output"
    );

    let _ = std::fs::remove_dir_all(&tmp);
}

#[test]
fn test_plan_diagnostic_without_its_own_pointer_gets_the_explain_hint() {
    // E203 (CXL name resolution) attaches no help of its own, so the renderer
    // is the only thing that can tell the user the code is explainable. A gate
    // whose help already names `clinker explain --code <CODE>` must not have a
    // second pointer stapled on.
    let tmp = tempdir_path();
    let yaml_path = tmp.join("cxl_unresolved.yaml");
    std::fs::write(
        &yaml_path,
        r#"pipeline:
  name: cxl_unresolved
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: amount, type: int }
  - type: transform
    name: t
    input: src
    config:
      cxl: "emit bogus = not_a_column + 1"
  - type: output
    name: out
    input: t
    config:
      name: out
      type: csv
      path: out.csv
"#,
    )
    .expect("write yaml");

    let output = Command::new(clinker_bin())
        .arg("run")
        .arg(&yaml_path)
        .output()
        .expect("spawn clinker");

    let stderr = String::from_utf8_lossy(&output.stderr);
    // Flattened for the same reason as `assert_plan_diagnostic_intact`: the
    // pointer is short enough to survive today's width, but it sits in a help
    // paragraph miette wraps, so matching raw stderr would make this test a
    // function of the terminal.
    let flat = flatten_report(&stderr);
    assert!(
        flat.contains("See: clinker explain --code E203"),
        "a diagnostic with no help of its own must still be told where its \
         explain page is; got:\n{stderr}"
    );
    assert_eq!(
        flat.matches("clinker explain --code E203").count(),
        1,
        "the explain pointer must appear exactly once; got:\n{stderr}"
    );

    let _ = std::fs::remove_dir_all(&tmp);
}

#[test]
fn test_composition_body_diagnostic_does_not_point_into_the_pipeline_file() {
    // A plan-time span is a bare line number with no file identity, and a
    // composition body's gates number lines in the *body* file. Resolving one
    // against the pipeline file underlines unrelated YAML -- here it landed on
    // the output node -- or, past the file's end, silently nothing. A plan
    // that binds a body therefore renders without a snippet at all.
    let tmp = tempdir_path();
    std::fs::write(
        tmp.join("body.comp.yaml"),
        r#"_compose:
  name: body
  inputs:
    b_in:
      schema:
        - { name: customer_id, type: string }
  outputs:
    b_out: body_proj
  config_schema: {}

nodes:
  - type: transform
    name: body_proj
    input: b_in
    config:
      cxl: |
        emit z = not_a_column_in_the_body
"#,
    )
    .expect("write body");
    let yaml_path = tmp.join("main.yaml");
    std::fs::write(
        &yaml_path,
        r#"pipeline:
  name: comp_span
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: customer_id, type: string }
  - type: composition
    name: c1
    input: src
    use: ./body.comp.yaml
    inputs:
      b_in: src
  - type: output
    name: out
    input: c1
    config:
      name: out
      type: csv
      path: out.csv
"#,
    )
    .expect("write main");

    let output = Command::new(clinker_bin())
        .arg("run")
        .arg(&yaml_path)
        .output()
        .expect("spawn clinker");

    assert!(
        !output.status.success(),
        "an unresolved identifier in a composition body must fail the run"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    let flat = flatten_report(&stderr);

    // The diagnostic still arrives whole.
    assert!(flat.contains("E203"), "got:\n{stderr}");
    assert!(flat.contains("not_a_column_in_the_body"), "got:\n{stderr}");
    // But nothing from the pipeline file is underlined, because no line of it
    // is implicated.
    assert!(
        !quotes_a_snippet_from(&flat, "main.yaml"),
        "a body-file diagnostic must not anchor a snippet in the pipeline \
         file; got:\n{stderr}"
    );
    assert!(
        !flat.contains("declared here"),
        "no snippet label may be drawn from an unattributable line; \
         got:\n{stderr}"
    );
    // With no snippet header to name it, the message still says which
    // pipeline failed.
    assert!(flat.contains("main.yaml"), "got:\n{stderr}");

    let _ = std::fs::remove_dir_all(&tmp);
}

#[test]
fn test_warning_is_not_painted_as_an_error() {
    // `compile()` returns warnings alongside errors in its `Err` vector, and
    // the whole vector is rendered. A W002 advisory styled identically to the
    // E363 that actually stopped the run leaves the user unable to tell which
    // is which.
    let tmp = tempdir_path();
    let yaml_path = tmp.join("warn_and_error.yaml");
    std::fs::write(
        &yaml_path,
        r#"pipeline:
  name: warn_and_error
nodes:
  - type: source
    name: Src
    config:
      name: Src
      type: json
      path: in.json
      options:
        record_path: "$.rows"
      schema:
        - { name: amount, type: int }
  - type: output
    name: src
    input: Src
    config:
      name: out
      type: csv
      path: out.csv
"#,
    )
    .expect("write yaml");

    let output = Command::new(clinker_bin())
        .arg("run")
        .arg(&yaml_path)
        .output()
        .expect("spawn clinker");

    assert!(!output.status.success(), "the E363 gate must fail the run");
    let stderr = String::from_utf8_lossy(&output.stderr);
    let flat = flatten_report(&stderr);

    // Both diagnostics are present...
    assert!(flat.contains("W002"), "got:\n{stderr}");
    assert!(flat.contains("E363"), "got:\n{stderr}");
    // ...and they are not styled the same. miette marks an error `x` and a
    // warning with its own glyph, so exactly one report is an error.
    assert_eq!(
        flat.matches('\u{d7}').count(),
        1,
        "exactly one report -- the error -- may carry the error marker; \
         got:\n{stderr}"
    );

    let _ = std::fs::remove_dir_all(&tmp);
}

#[test]
fn test_two_location_diagnostic_renders_both_locations() {
    // E164 is unactionable with half of it shown: the node the author has to
    // change is the runtime consumer, which is the secondary label.
    let tmp = tempdir_path();
    let yaml_path = tmp.join("two_location.yaml");
    std::fs::write(
        &yaml_path,
        r#"pipeline:
  name: two_location
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: amount, type: int }
  - type: transform
    name: init_t
    input: src
    config:
      phase: init
      cxl: |
        emit doubled = amount * 2
  - type: transform
    name: runtime_t
    input: init_t
    config:
      cxl: |
        emit tripled = doubled + 1
  - type: output
    name: out
    input: runtime_t
    config:
      name: out
      type: csv
      path: out.csv
"#,
    )
    .expect("write yaml");

    let output = Command::new(clinker_bin())
        .arg("run")
        .arg(&yaml_path)
        .output()
        .expect("spawn clinker");

    assert!(!output.status.success(), "the E164 gate must fail the run");
    let stderr = String::from_utf8_lossy(&output.stderr);
    let flat = flatten_report(&stderr);

    assert!(flat.contains("E164"), "got:\n{stderr}");
    assert!(
        flat.contains("init node"),
        "the primary label must be rendered; got:\n{stderr}"
    );
    assert!(
        flat.contains("runtime consumer"),
        "the secondary label names the node the author must change, and must \
         be rendered too; got:\n{stderr}"
    );

    let _ = std::fs::remove_dir_all(&tmp);
}

/// Write a composition body plus a caller that trips a body-file diagnostic,
/// optionally giving the caller two node names that differ only in case so the
/// compile also yields a W002 warning.
fn write_body_and_caller(tmp: &std::path::Path, warn: bool) -> std::path::PathBuf {
    std::fs::write(
        tmp.join("body.comp.yaml"),
        r#"_compose:
  name: body
  inputs:
    b_in:
      schema:
        - { name: customer_id, type: string }
  outputs:
    b_out: body_proj
  config_schema: {}

nodes:
  - type: transform
    name: body_proj
    input: b_in
    config:
      cxl: |
        emit z = not_a_column_in_the_body
"#,
    )
    .expect("write body");
    let (source_name, comp_name) = if warn { ("Src", "src") } else { ("src", "c1") };
    let yaml_path = tmp.join("main.yaml");
    std::fs::write(
        &yaml_path,
        format!(
            r#"pipeline:
  name: comp_span
nodes:
  - type: source
    name: {source_name}
    config:
      name: {source_name}
      type: csv
      path: in.csv
      schema:
        - {{ name: customer_id, type: string }}
  - type: composition
    name: {comp_name}
    input: {source_name}
    use: ./body.comp.yaml
    inputs:
      b_in: {source_name}
  - type: output
    name: out
    input: {comp_name}
    config:
      name: out
      type: csv
      path: out.csv
"#
        ),
    )
    .expect("write main");
    yaml_path
}

#[test]
fn test_warning_on_the_unanchored_path_is_not_called_an_error() {
    // With no snippet to carry the filename, the message names it instead --
    // but "pipeline error in ..." on a warning contradicts the glyph beside
    // it. `test_warning_is_not_painted_as_an_error` covers only the
    // snippet-bearing path, where the prefix is absent for other reasons.
    let tmp = tempdir_path();
    let yaml_path = write_body_and_caller(&tmp, true);

    let output = Command::new(clinker_bin())
        .arg("run")
        .arg(&yaml_path)
        .output()
        .expect("spawn clinker");

    let stderr = String::from_utf8_lossy(&output.stderr);
    let flat = flatten_report(&stderr);

    assert!(flat.contains("W002"), "got:\n{stderr}");
    // Assertions are scoped to the W002 report rather than to the whole
    // stream: the E203 error below it legitimately says "pipeline error in",
    // and no needle may span the temp path, which miette breaks mid-token at
    // some widths.
    let (warning_report, _) = flat
        .split_once("E203")
        .expect("the body-file error follows the warning");
    assert!(
        warning_report.contains("node names differ only in case"),
        "got:\n{stderr}"
    );
    // The warning still names the file it came from...
    assert!(
        warning_report.contains("main.yaml"),
        "the warning must still name its file; got:\n{stderr}"
    );
    // ...without calling itself a pipeline error.
    assert!(
        !warning_report.contains("pipeline error"),
        "a warning must not be prefixed as an error; got:\n{stderr}"
    );

    let _ = std::fs::remove_dir_all(&tmp);
}

#[test]
fn test_resolver_diagnostic_keeps_its_own_code_and_help() {
    // A composition body reading a parent-scope var its `scoped_vars` schema
    // does not opt into raises E173, which carries the help naming the exact
    // field to add. Wrapping it as E203 published two contradicting codes and
    // dropped that help, leaving the reader with a pointer to a page about a
    // different failure.
    let tmp = tempdir_path();
    std::fs::write(
        tmp.join("scoped.comp.yaml"),
        r#"_compose:
  name: scoped
  inputs:
    s_in:
      schema:
        - { name: customer_id, type: string }
  outputs:
    s_out: s_proj
  config_schema: {}

nodes:
  - type: transform
    name: s_proj
    input: s_in
    config:
      cxl: |
        emit z = $pipeline.batch_label
"#,
    )
    .expect("write body");
    let yaml_path = tmp.join("main.yaml");
    std::fs::write(
        &yaml_path,
        r#"pipeline:
  name: scoped_var
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: customer_id, type: string }
  - type: transform
    name: tag
    input: src
    config:
      declares:
        - { name: batch_label, scope: pipeline, type: string }
      cxl: |
        emit $pipeline.batch_label = "spring"
  - type: composition
    name: c1
    input: tag
    use: ./scoped.comp.yaml
    inputs:
      s_in: tag
  - type: output
    name: out
    input: c1
    config:
      name: out
      type: csv
      path: out.csv
"#,
    )
    .expect("write main");

    let output = Command::new(clinker_bin())
        .arg("run")
        .arg(&yaml_path)
        .output()
        .expect("spawn clinker");

    assert!(!output.status.success(), "the scoped-var gate must fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    let flat = flatten_report(&stderr);

    assert!(
        flat.contains("E173"),
        "the resolver's own code must head the report; got:\n{stderr}"
    );
    assert!(
        !flat.contains("E203"),
        "the wrapping code must not appear alongside it; got:\n{stderr}"
    );
    // The code is the header, so the message must not repeat it.
    assert!(
        !flat.contains("[E173]"),
        "the code must be stated once, not embedded in the message too; \
         got:\n{stderr}"
    );
    assert!(
        flat.contains("_compose.scoped_vars.pipeline"),
        "the resolver's help names the field to add and must survive; \
         got:\n{stderr}"
    );

    let _ = std::fs::remove_dir_all(&tmp);
}

const SECRET: &str = "sftp://user:hunter2SECRETTOKEN@files.example.com/in";
const MULTILINE_SECRET: &str =
    "-----BEGIN KEY-----\n# MIIBOgIBAAJBAKSECRETLINE\n# -----END KEY-----";
const CR_SECRET: &str = "CR_FIRST_SECRET\r# CR_SECOND_SECRET";

#[test]
fn test_a_resolved_env_var_never_reaches_the_rendered_snippet() {
    // The loader substitutes `${VAR}` before parsing, so a span's line number
    // indexes the interpolated text. Quoting that text to make the numbers
    // line up would print resolved credentials: miette shows the underlined
    // line plus one either side, so a `path: "${SFTP_URL}"` on or beside the
    // offending node lands in stderr and in any CI log capturing it.
    //
    // Both shapes are covered because they exercise different halves of the
    // fix: the single-line value tests that the raw text is what gets quoted,
    // and the multi-line value tests that the snippet is dropped when the two
    // texts stop sharing line numbering.
    let tmp = tempdir_path();

    // (a) A single-line secret sitting on the very line the gate underlines.
    //     Flow style puts the whole node — including `path:` — on one line.
    let inline = tmp.join("inline_secret.yaml");
    std::fs::write(
        &inline,
        r#"pipeline:
  name: inline_secret
nodes:
  - { type: source, name: src, config: { name: src, type: json, path: "${LEAK_TEST_URL}", options: { record_path: "$.rows" }, schema: [{ name: amount, type: int }] } }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
"#,
    )
    .expect("write inline fixture");

    // (b) A multi-line value ahead of the offending node, which shifts every
    //     later line and so breaks the correspondence between the two texts.
    let shifted = tmp.join("shifted_secret.yaml");
    std::fs::write(
        &shifted,
        r#"pipeline:
  name: shifted_secret
# ${LEAK_TEST_PEM}
nodes:
  - type: source
    name: src
    config:
      name: src
      type: json
      path: "${LEAK_TEST_URL}"
      options:
        record_path: "$.rows"
      schema:
        - { name: amount, type: int }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
"#,
    )
    .expect("write shifted fixture");

    // (c) A lone-CR value shifts saphyr's YAML line numbering just like LF,
    //     even though it contains no `\n` byte.
    let shifted_cr = tmp.join("shifted_cr_secret.yaml");
    std::fs::write(
        &shifted_cr,
        r#"pipeline:
  name: shifted_cr_secret
# ${LEAK_TEST_CR}
nodes:
  - type: source
    name: src
    config:
      name: src
      type: json
      path: "${LEAK_TEST_URL}"
      options:
        record_path: "$.rows"
      schema:
        - { name: amount, type: int }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
"#,
    )
    .expect("write lone-CR fixture");

    for fixture in [&inline, &shifted, &shifted_cr] {
        let output = Command::new(clinker_bin())
            .arg("run")
            .arg(fixture)
            .env("LEAK_TEST_URL", SECRET)
            .env("LEAK_TEST_PEM", MULTILINE_SECRET)
            .env("LEAK_TEST_CR", CR_SECRET)
            .output()
            .expect("spawn clinker");

        assert!(
            !output.status.success(),
            "the record_path gate must fail so a diagnostic is rendered"
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);

        // Asserted on raw output, deliberately un-flattened: a secret split
        // across a wrap is still a leaked secret, so each line of a multi-line
        // value is checked on its own.
        for needle in [SECRET, "hunter2SECRETTOKEN"] {
            assert!(
                !stderr.contains(needle),
                "a resolved credential reached stderr from {}:\n{stderr}",
                fixture.display()
            );
            assert!(
                !stdout.contains(needle),
                "a resolved credential reached stdout"
            );
        }
        for line in MULTILINE_SECRET.lines() {
            let line = line.trim_start_matches("# ").trim();
            assert!(
                !stderr.contains(line),
                "a resolved multi-line value reached stderr from {}: {line:?}\n{stderr}",
                fixture.display()
            );
        }
        for line in CR_SECRET.split('\r') {
            let line = line.trim_start_matches("# ").trim();
            assert!(
                !stderr.contains(line),
                "a resolved lone-CR value reached stderr from {}: {line:?}\n{stderr}",
                fixture.display()
            );
        }
        // The diagnostic still arrives — this is not passing by rendering
        // nothing at all.
        let flat = flatten_report(&stderr);
        assert!(flat.contains("E363"), "got:\n{stderr}");
        if fixture != &inline {
            let filename = fixture.file_name().unwrap().to_string_lossy();
            assert!(
                !quotes_a_snippet_from(&flat, &filename),
                "a line-shifting substitution must suppress the raw snippet; got:\n{stderr}"
            );
        }
    }

    // The reference itself is fine to show, and proves the snippet was drawn
    // from the raw file rather than suppressed in the single-line case.
    let output = Command::new(clinker_bin())
        .arg("run")
        .arg(&inline)
        .env("LEAK_TEST_URL", SECRET)
        .env("LEAK_TEST_PEM", MULTILINE_SECRET)
        .output()
        .expect("spawn clinker");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        flatten_report(&stderr).contains("${LEAK_TEST_URL}"),
        "the raw reference should be quoted in place of its value; got:\n{stderr}"
    );

    let _ = std::fs::remove_dir_all(&tmp);
}

#[test]
fn test_a_validation_gate_renders_under_its_own_code() {
    // A large family of gates reports through `ConfigError::Validation` with
    // its code at the head of the message rather than as a structured
    // `Diagnostic`. Those used to render under the placeholder
    // `clinker::pipeline_error`, so the code never reached the header and the
    // report carried none of the parts the docs describe -- while the
    // identical class of failure raised as a `Diagnostic` got all of them.
    let tmp = tempdir_path();
    let yaml_path = tmp.join("bad_threshold.yaml");
    std::fs::write(
        &yaml_path,
        r#"pipeline:
  name: bad_threshold
  memory:
    resume_threshold: 0.99
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: amount, type: int }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
"#,
    )
    .expect("write fixture");

    let output = Command::new(clinker_bin())
        .arg("run")
        .arg(&yaml_path)
        .output()
        .expect("spawn clinker");

    assert!(!output.status.success(), "the threshold gate must fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    let flat = flatten_report(&stderr);

    assert!(
        flat.contains("E324"),
        "the gate's own code must head the report; got:\n{stderr}"
    );
    assert!(
        !flat.contains("clinker::pipeline_error"),
        "the placeholder code must not stand in for a code the message carries; \
         got:\n{stderr}"
    );
    // Lifted into the header, so the prefix that carried it is gone from the
    // message -- stated once per report, as for a structured diagnostic.
    assert!(
        !flat.contains("[E324]"),
        "the code must be stated once, not left embedded in the message too; \
         got:\n{stderr}"
    );
    // This gate spells its own pointer, so exactly one must survive.
    assert_eq!(
        flat.matches("clinker explain --code E324").count(),
        1,
        "the explain pointer must appear exactly once; got:\n{stderr}"
    );

    let _ = std::fs::remove_dir_all(&tmp);
}

#[test]
fn test_an_overlay_error_is_not_blamed_on_the_pipeline_file() {
    // Overlay errors route through the same renderer as plan diagnostics,
    // which prefixes a label-less report with `pipeline error in <file>:`. The
    // offending input is the channel file, so naming the pipeline sent the
    // author to a document with nothing wrong in it.
    let tmp = tempdir_path();
    std::fs::write(tmp.join("in.csv"), "amount\n1\n").expect("write input");
    std::fs::write(
        tmp.join("pipe.yaml"),
        r#"pipeline:
  name: overlay_attrib
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: amount, type: int }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
"#,
    )
    .expect("write pipeline");

    let tenant = tmp.join("channel").join("acme");
    std::fs::create_dir_all(&tenant).expect("create tenant dir");
    std::fs::write(
        tenant.join("pipe.channel.yaml"),
        "channel:\n  target: ../../pipe.yaml\nconfig:\n  nosuchnode.nosuchparam: 1\n",
    )
    .expect("write overlay");

    let output = Command::new(clinker_bin())
        .arg("run")
        .arg("pipe.yaml")
        .arg("--channel")
        .arg("acme")
        .current_dir(&tmp)
        .output()
        .expect("spawn clinker");

    assert!(
        !output.status.success(),
        "an overlay key matching no parameter must fail the run"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    let flat = flatten_report(&stderr);

    assert!(flat.contains("E113"), "got:\n{stderr}");
    assert!(
        !flat.contains("pipeline error in"),
        "the pipeline file is not the offending input and must not be named as \
         it; got:\n{stderr}"
    );
    // The message still identifies what failed and where -- the attribution is
    // dropped, not the identification.
    assert!(
        flat.contains("nosuchnode.nosuchparam"),
        "the report must still name the offending overlay key; got:\n{stderr}"
    );

    let _ = std::fs::remove_dir_all(&tmp);
}

/// Write a workspace whose `pipe.yaml` trips E363, plus a channel `acme`
/// carrying `overlay_body`. Returns the workspace root.
fn write_channel_workspace(overlay_body: &str) -> std::path::PathBuf {
    let tmp = tempdir_path();
    std::fs::write(tmp.join("in.json"), "{\"rows\":[]}\n").expect("write input");
    std::fs::write(tmp.join("pipe.yaml"), jsonpath_record_path_yaml()).expect("write pipeline");
    let tenant = tmp.join("channel").join("acme");
    std::fs::create_dir_all(&tenant).expect("create tenant dir");
    std::fs::write(
        tenant.join("pipe.channel.yaml"),
        format!("channel:\n  target: ../../pipe.yaml\n{overlay_body}"),
    )
    .expect("write overlay");
    tmp
}

/// Run `clinker run pipe.yaml --channel acme` inside `dir`.
fn run_channel_in(dir: &std::path::Path) -> std::process::Output {
    Command::new(clinker_bin())
        .arg("run")
        .arg("pipe.yaml")
        .arg("--channel")
        .arg("acme")
        .current_dir(dir)
        .output()
        .expect("spawn clinker")
}

#[test]
fn test_a_channel_that_rewrites_nothing_keeps_the_source_snippet() {
    // Suppression is keyed on whether the text being quoted is still the text
    // that was compiled -- not on whether a channel was selected. A channel
    // carrying only var overlays contributes no op, no source patch and no
    // composition `config:` fold, and vars are applied to the compiled plan
    // afterwards, so `pipe.yaml` is byte-for-byte what the compiler saw. Its
    // snippet is correct, and dropping it costs the author a source line for
    // nothing.
    let tmp = write_channel_workspace(
        "vars:\n  pipeline:\n    tenant_label: { type: string, default: \"acme\" }\n",
    );

    let output = run_channel_in(&tmp);
    assert!(!output.status.success(), "the record_path gate must fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    let flat = flatten_report(&stderr);

    assert!(flat.contains("E363"), "got:\n{stderr}");
    assert!(
        quotes_a_snippet_from(&flat, "pipe.yaml"),
        "a channel that rewrote nothing must not cost the author their \
         snippet; got:\n{stderr}"
    );
    // Drawn from the raw file, which is also what was compiled here.
    assert!(
        flat.contains("record_path"),
        "the quoted line must be the offending one; got:\n{stderr}"
    );

    let _ = std::fs::remove_dir_all(&tmp);
}

#[test]
fn test_a_channel_that_patches_a_source_drops_the_source_snippet() {
    // The other half of the same rule. A `sources:` patch is applied to the
    // parsed config before compile, so the compiler is working on a document
    // `pipe.yaml` no longer describes. Line numbering is untouched, which is
    // exactly what makes this dangerous: every line still resolves, so a
    // snippet would quote stale content with nothing to signal it.
    let tmp =
        write_channel_workspace("sources:\n  src:\n    schema:\n      amount: { type: float }\n");

    let output = run_channel_in(&tmp);
    assert!(!output.status.success(), "the record_path gate must fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    let flat = flatten_report(&stderr);

    assert!(flat.contains("E363"), "got:\n{stderr}");
    assert!(
        !quotes_a_snippet_from(&flat, "pipe.yaml"),
        "a patched config must not be quoted from the unpatched file; \
         got:\n{stderr}"
    );
    // The diagnostic still arrives whole, and still names the pipeline.
    assert!(flat.contains("pipe.yaml"), "got:\n{stderr}");

    let _ = std::fs::remove_dir_all(&tmp);
}

/// Create an ephemeral per-test temp directory under the system
/// temp root. Avoids adding a `tempfile` dev-dep.
fn tempdir_path() -> std::path::PathBuf {
    // A per-process atomic counter guarantees a distinct directory per call.
    // pid + timestamp alone can collide on a platform whose clock resolution
    // is coarser than the nanosecond unit (macOS), letting two concurrent
    // tests share one directory and clobber each other's fixtures.
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let mut base = std::env::temp_dir();
    let name = format!(
        "clinker-miette-test-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0),
        SEQ.fetch_add(1, Ordering::Relaxed)
    );
    base.push(name);
    std::fs::create_dir_all(&base).expect("create tempdir");
    base
}

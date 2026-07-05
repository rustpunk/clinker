//! End-to-end coverage for the channel `sources:` config-patch block.
//!
//! A channel's per-target overlay patches a source's parsed config (schema
//! column ops, `array_paths` ops, scalar `options`) before the pipeline is
//! validated and compiled, so a run behaves as if the source YAML had been
//! hand-edited. These tests shell out to the built `clinker` binary and drive
//! the channel by tenant id (`--channel <id>`), resolving the tenant folder
//! under the workspace `channel/` root. They assert the patch takes effect on
//! the normal run path, is reflected by `--explain`, and that a bad patch fails
//! at compile time with the documented diagnostic.

use std::path::Path;
use std::process::Command;

fn clinker_bin() -> &'static str {
    env!("CARGO_BIN_EXE_clinker")
}

/// Write `contents` to `dir/name`.
fn write(dir: &Path, name: &str, contents: &str) {
    std::fs::write(dir.join(name), contents).expect("write fixture file");
}

/// Write a per-target overlay for tenant `id` overlaying `pipe.yaml`, at the
/// computed path `channel/<id>/pipe.channel.yaml`. `body` is the overlay YAML
/// below the authoritative `channel.target` header (e.g. a `sources:` block).
fn write_channel(dir: &Path, id: &str, body: &str) {
    let tenant = dir.join("channel").join(id);
    std::fs::create_dir_all(&tenant).expect("create tenant dir");
    let yaml = format!("channel:\n  target: ../../pipe.yaml\n{body}");
    std::fs::write(tenant.join("pipe.channel.yaml"), yaml).expect("write overlay");
}

/// Run `clinker run pipe.yaml` (plus any extra args) inside `dir`. With
/// `pipe.yaml` at the workspace root, the workspace root resolves to `dir`, so
/// `channel/` folders resolve there.
fn run_in(dir: &Path, extra: &[&str]) -> std::process::Output {
    Command::new(clinker_bin())
        .arg("run")
        .arg("pipe.yaml")
        .args(extra)
        .current_dir(dir)
        .output()
        .expect("spawn clinker")
}

/// A CSV pipeline whose transform reads the POST-patch names (`customer_id`
/// aliased from the physical `cust_id`, added `region`) and a numeric `amount`,
/// so the base pipeline fails to compile and only the channel patch — rename
/// `cust_id`→`customer_id`, retype `amount` to int, add `region` — makes it
/// valid.
const CSV_PIPELINE: &str = "\
pipeline:
  name: csv_patch
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: id, type: string }
        - { name: cust_id, type: string }
        - { name: amount, type: string }
  - type: transform
    name: calc
    input: src
    config:
      cxl: |
        emit doubled = amount * 2
        emit customer = customer_id
        emit zone = region
  - type: output
    name: out
    input: calc
    config:
      name: out
      type: csv
      path: out.csv
";

const CSV_CHANNEL: &str = "\
sources:
  src:
    schema:
      cust_id: { rename: customer_id }
      amount:  { type: int }
      region:  { add: { type: string } }
";

#[test]
fn csv_schema_patch_enables_run_and_is_visible_downstream() {
    let dir = tempfile::tempdir().expect("tempdir");
    let p = dir.path();
    write(
        p,
        "in.csv",
        "id,cust_id,amount,region\n1,alice,100,west\n2,bob,250,east\n",
    );
    write(p, "pipe.yaml", CSV_PIPELINE);
    write_channel(p, "fix", CSV_CHANNEL);

    // Base pipeline: `customer_id` / `region` are not declared and `amount` is
    // a string, so it fails to compile.
    let base = run_in(p, &[]);
    assert!(
        !base.status.success(),
        "base pipeline should fail without the channel patch"
    );

    // Channel patch makes it valid; output proves the physical→logical mapping.
    let patched = run_in(p, &["--channel", "fix"]);
    assert!(
        patched.status.success(),
        "patched run failed:\n{}",
        String::from_utf8_lossy(&patched.stderr)
    );
    let out = std::fs::read_to_string(p.join("out.csv")).expect("read out.csv");
    let header = out.lines().next().unwrap_or_default();
    assert!(header.contains("customer_id"), "header: {header}");
    assert!(header.contains("doubled"), "header: {header}");
    assert!(header.contains("zone"), "header: {header}");
    // The renamed column is a real alias: the physical `cust_id` field's data
    // lands under the exposed `customer_id` name, NOT in `$widened`. If rename
    // were a bare relabel, `customer_id` would be empty and `cust_id` would be
    // re-emitted as a widened column.
    assert!(
        !header.contains("cust_id"),
        "physical `cust_id` must be consumed as declared, not re-emitted: {header}"
    );
    // amount retyped to int and doubled; region added and echoed as zone;
    // customer_id carries the real cust_id data.
    assert!(
        out.contains("1,alice,100,west,200,alice,west"),
        "output:\n{out}"
    );
    assert!(
        out.contains("2,bob,250,east,500,bob,east"),
        "output:\n{out}"
    );
}

#[test]
fn base_schema_source_name_alias_maps_physical_to_logical() {
    // A hand-written base schema (no channel) can declare `source_name` to read
    // a differently-named physical column — the same alias the channel rename
    // op produces.
    let dir = tempfile::tempdir().expect("tempdir");
    let p = dir.path();
    write(p, "in.csv", "id,cust_id\n1,alice\n2,bob\n");
    write(
        p,
        "pipe.yaml",
        "\
pipeline:
  name: alias_base
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: id, type: string }
        - { name: customer_id, type: string, source_name: cust_id }
  - type: transform
    name: calc
    input: src
    config:
      cxl: |
        emit customer = customer_id
  - type: output
    name: out
    input: calc
    config:
      name: out
      type: csv
      path: out.csv
",
    );
    let run = run_in(p, &[]);
    assert!(
        run.status.success(),
        "base-schema alias run failed:\n{}",
        String::from_utf8_lossy(&run.stderr)
    );
    let out = std::fs::read_to_string(p.join("out.csv")).expect("read out.csv");
    let header = out.lines().next().unwrap_or_default();
    assert!(header.contains("customer_id"), "header: {header}");
    assert!(
        !header.contains("cust_id"),
        "physical name must not leak: {header}"
    );
    assert!(out.contains("1,alice"), "output:\n{out}");
    assert!(out.contains("2,bob"), "output:\n{out}");
}

#[test]
fn csv_schema_patch_is_reflected_by_explain() {
    let dir = tempfile::tempdir().expect("tempdir");
    let p = dir.path();
    write(p, "in.csv", "id,cust_id,amount,region\n1,alice,100,west\n");
    write(p, "pipe.yaml", CSV_PIPELINE);
    write_channel(p, "fix", CSV_CHANNEL);

    // --explain compiles the same patched config: base fails, channel succeeds.
    let base = run_in(p, &["--explain"]);
    assert!(
        !base.status.success(),
        "base --explain should fail without the channel patch"
    );
    let patched = run_in(p, &["--channel", "fix", "--explain"]);
    assert!(
        patched.status.success(),
        "patched --explain failed:\n{}",
        String::from_utf8_lossy(&patched.stderr)
    );
}

#[test]
fn json_options_and_array_paths_patch_changes_run_output() {
    let dir = tempfile::tempdir().expect("tempdir");
    let p = dir.path();
    write(
        p,
        "in.json",
        "{ \"small\": { \"rows\": [ {\"id\":\"1\",\"tags\":[\"x\",\"y\"]} ] }, \
           \"big\": { \"rows\": [ {\"id\":\"1\",\"tags\":[\"x\",\"y\"]}, \
                                   {\"id\":\"2\",\"tags\":[\"z\"]}, \
                                   {\"id\":\"3\",\"tags\":[\"w\"]} ] } }\n",
    );
    write(
        p,
        "pipe.yaml",
        "\
pipeline:
  name: json_patch
nodes:
  - type: source
    name: src
    config:
      name: src
      type: json
      path: in.json
      options:
        record_path: small.rows
      schema:
        - { name: id, type: string }
        - { name: tags, type: array }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
",
    );
    write_channel(
        p,
        "jfix",
        "\
sources:
  src:
    options:
      record_path: big.rows
    array_paths:
      tags: { mode: explode }
",
    );

    // Base: record_path points at the single-row array; no explosion.
    let base = run_in(p, &[]);
    assert!(base.status.success(), "base json run failed");
    let base_out = std::fs::read_to_string(p.join("out.csv")).expect("read out.csv");
    let base_rows = base_out.lines().skip(1).filter(|l| !l.is_empty()).count();
    assert_eq!(base_rows, 1, "base output:\n{base_out}");

    // Channel: record_path override selects the three-row array, and the
    // array_paths explode fans each record out per tag → 4 rows.
    let patched = run_in(p, &["--channel", "jfix"]);
    assert!(
        patched.status.success(),
        "patched json run failed:\n{}",
        String::from_utf8_lossy(&patched.stderr)
    );
    let out = std::fs::read_to_string(p.join("out.csv")).expect("read out.csv");
    let rows = out.lines().skip(1).filter(|l| !l.is_empty()).count();
    assert_eq!(rows, 4, "expected 4 exploded rows, got:\n{out}");
    for tag in ["x", "y", "z", "w"] {
        assert!(out.contains(tag), "missing exploded tag {tag}:\n{out}");
    }
}

#[test]
fn unknown_source_name_fails_at_compile() {
    let dir = tempfile::tempdir().expect("tempdir");
    let p = dir.path();
    write(p, "in.csv", "id,amount,region\n1,100,west\n");
    write(p, "pipe.yaml", CSV_PIPELINE);
    write_channel(
        p,
        "bad",
        "\
sources:
  ghost:
    schema:
      id: remove
",
    );
    let out = run_in(p, &["--channel", "bad"]);
    assert!(!out.status.success(), "run with unknown source should fail");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("E230"), "stderr:\n{stderr}");
}

#[test]
fn unknown_column_op_fails_at_compile() {
    let dir = tempfile::tempdir().expect("tempdir");
    let p = dir.path();
    write(p, "in.csv", "id,amount,region\n1,100,west\n");
    write(p, "pipe.yaml", CSV_PIPELINE);
    write_channel(
        p,
        "bad",
        "\
sources:
  src:
    schema:
      not_a_column: { type: int }
",
    );
    let out = run_in(p, &["--channel", "bad"]);
    assert!(!out.status.success(), "run with unknown column should fail");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("E231"), "stderr:\n{stderr}");
}

/// Pipeline whose output path carries the `{pipeline_hash}` token, so the
/// effective pipeline identity is observable as the output filename.
const HASH_PIPELINE: &str = "\
pipeline:
  name: hash_demo
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: id, type: string }
        - { name: amount, type: string }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: \"out_{pipeline_hash}.csv\"
";

#[test]
fn channel_patch_changes_pipeline_hash_and_empty_patch_does_not() {
    let dir = tempfile::tempdir().expect("tempdir");
    let p = dir.path();
    write(p, "in.csv", "id,amount\n1,100\n");
    write(p, "pipe.yaml", HASH_PIPELINE);
    write_channel(
        p,
        "a",
        "sources:\n  src:\n    schema:\n      amount: { type: int }\n",
    );
    write_channel(
        p,
        "b",
        "sources:\n  src:\n    schema:\n      amount: { type: float }\n",
    );
    // An overlay that declares no `sources:` block contributes no patch.
    write_channel(p, "e", "");

    // Extract the `{pipeline_hash}` token from the single out_*.csv the run
    // wrote, then clear it before the next run.
    let hash_after = |args: &[&str]| -> String {
        let out = run_in(p, args);
        assert!(
            out.status.success(),
            "run failed:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
        let mut found = None;
        for entry in std::fs::read_dir(p).unwrap() {
            let name = entry.unwrap().file_name().into_string().unwrap();
            if let Some(hash) = name
                .strip_prefix("out_")
                .and_then(|rest| rest.strip_suffix(".csv"))
            {
                found = Some(hash.to_string());
                std::fs::remove_file(p.join(&name)).unwrap();
            }
        }
        found.expect("an out_<hash>.csv file")
    };

    let base = hash_after(&[]);
    let empty = hash_after(&["--channel", "e"]);
    let patch_a = hash_after(&["--channel", "a"]);
    let patch_b = hash_after(&["--channel", "b"]);

    // An overlay with no source patches leaves the pipeline hash byte-identical
    // to the base.
    assert_eq!(
        empty, base,
        "an empty-patch channel must not change the pipeline hash"
    );
    // A patched run has a distinct identity from the base...
    assert_ne!(patch_a, base, "patched run must differ from base");
    // ...and two different patches produce different identities.
    assert_ne!(patch_a, patch_b, "two different patches must differ");
}

/// The `clinker explain` subcommand applies channel source-patches before
/// compile: an unknown-source patch surfaces E230, proving the channel is
/// loaded and applied on that path too (not silently ignored).
#[test]
fn explain_subcommand_applies_channel_source_patch() {
    let dir = tempfile::tempdir().expect("tempdir");
    let p = dir.path();
    write(p, "in.csv", "id,amount\n1,100\n");
    write(
        p,
        "pipe.yaml",
        "\
pipeline:
  name: explain_patch
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: id, type: string }
        - { name: amount, type: string }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
",
    );
    write_channel(
        p,
        "bad",
        "sources:\n  ghost:\n    schema:\n      id: remove\n",
    );
    let out = Command::new(clinker_bin())
        .arg("explain")
        .arg("pipe.yaml")
        .arg("--field")
        .arg("out.name")
        .arg("--channel")
        .arg("bad")
        .current_dir(p)
        .output()
        .expect("spawn clinker explain");
    assert!(
        !out.status.success(),
        "explain with an unknown-source patch should fail"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("E230"), "stderr:\n{stderr}");
}

// ── Composition-body source patches ─────────────────────────────────────────
//
// A channel `sources:` key qualified `<composition>.<source>` patches a source
// declared inside that composition's body. The body is expanded only during
// compile, so the patch is deferred and applied when the body is re-read. A
// body-declared source binds (its schema seeds the body) but does not run
// today, so these drive the compile-only `--explain` path — which flows through
// the same channel resolution and body-source patch application a run would.

/// The invoking pipeline: a composition `enrich` whose body reads its own
/// declared source `ref`.
const COMP_PIPELINE: &str = "\
pipeline:
  name: body_source_patch
nodes:
  - type: source
    name: drv
    config:
      name: drv
      type: csv
      path: drv.csv
      schema:
        - { name: x, type: int }
  - type: composition
    name: enrich
    input: drv
    use: ./compositions/with_ref.comp.yaml
    inputs:
      driver: drv
  - type: output
    name: out
    input: enrich
    config:
      name: out
      type: csv
      path: out.csv
";

/// Write a `with_ref.comp.yaml` under `dir/compositions/` whose body source
/// `ref` declares `ref_schema`. The body transform emits `label = code`, so the
/// body binds only when the source carries a `code` column.
fn write_comp(dir: &Path, ref_schema: &str) {
    let comp_dir = dir.join("compositions");
    std::fs::create_dir_all(&comp_dir).expect("create compositions dir");
    let yaml = format!(
        "\
_compose:
  name: with_ref
  inputs:
    driver:
      schema:
        - {{ name: x, type: int }}
  outputs:
    out: shape
  config_schema: {{}}

nodes:
  - type: source
    name: ref
    config:
      name: ref
      type: csv
      path: ref.csv
      schema:
{ref_schema}
  - type: transform
    name: shape
    input: ref
    config:
      cxl: |
        emit label = code
"
    );
    std::fs::write(comp_dir.join("with_ref.comp.yaml"), yaml).expect("write comp");
}

/// A channel `sources:` patch qualified `enrich.ref` reaches the source inside
/// composition `enrich`'s body: the body fails to compile without it (the
/// source lacks `code`) and compiles with it (the patch renames `raw` -> `code`).
#[test]
fn channel_patches_composition_body_source() {
    let dir = tempfile::tempdir().expect("tempdir");
    let p = dir.path();
    write(p, "pipe.yaml", COMP_PIPELINE);
    write_comp(p, "        - { name: raw, type: string }");
    write_channel(
        p,
        "fix",
        "sources:\n  enrich.ref:\n    schema:\n      raw: { rename: code }\n",
    );

    // Base compile fails: the body source has no `code` column for `shape`.
    let base = run_in(p, &["--explain"]);
    assert!(
        !base.status.success(),
        "base --explain should fail before the body source is patched"
    );

    // The qualified channel patch renames the body source column, so the body
    // binds and the effective plan compiles.
    let patched = run_in(p, &["--channel", "fix", "--explain"]);
    assert!(
        patched.status.success(),
        "patched --explain failed:\n{}",
        String::from_utf8_lossy(&patched.stderr)
    );
}

/// A channel patch whose composition exists but whose inner source name does
/// not fails compile with E230, naming the missing source — not silently
/// ignored.
#[test]
fn channel_patch_unknown_body_source_fails_with_e230() {
    let dir = tempfile::tempdir().expect("tempdir");
    let p = dir.path();
    write(p, "pipe.yaml", COMP_PIPELINE);
    // A valid body source, so the only error is the unknown patch target.
    write_comp(p, "        - { name: code, type: string }");
    write_channel(
        p,
        "bad",
        "sources:\n  enrich.ghost:\n    schema:\n      code: remove\n",
    );

    let out = run_in(p, &["--channel", "bad", "--explain"]);
    assert!(
        !out.status.success(),
        "a patch on an unknown body source should fail compile"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("E230"), "stderr:\n{stderr}");
    assert!(
        stderr.contains("ghost"),
        "E230 must name the missing source:\n{stderr}"
    );
}

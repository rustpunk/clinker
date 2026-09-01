//! Executes every committed composition fragment through the real CLI.
//!
//! The recursive inventory comes from the production composition loader.
//! Corpus keys must exactly equal that inventory before any case runs.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};
use std::process::Command;

use clinker_plan::config::composition::scan_workspace_signatures;
use serde::Deserialize;
use serde::de::{Error as _, MapAccess, Visitor};

const COMPOSITION_ROOT: &str = "examples/pipelines/compositions";
const CORPUS_MANIFEST: &str = "examples/pipelines/compositions/corpus-cases.json";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CorpusCase {
    input: String,
    expected: String,
    counters: Counters,
    #[serde(default)]
    config: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Counters {
    total: u64,
    ok: u64,
    written: u64,
    dlq: u64,
}

#[derive(Debug)]
struct CorpusCases(BTreeMap<String, CorpusCase>);

impl<'de> Deserialize<'de> for CorpusCases {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct CorpusVisitor;

        impl<'de> Visitor<'de> for CorpusVisitor {
            type Value = CorpusCases;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a composition corpus object keyed by fragment path")
            }

            fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mut cases = BTreeMap::new();
                while let Some((key, case)) = map.next_entry::<String, CorpusCase>()? {
                    if cases.insert(key.clone(), case).is_some() {
                        return Err(M::Error::custom(format!("duplicate corpus key: {key}")));
                    }
                }
                Ok(CorpusCases(cases))
            }
        }

        deserializer.deserialize_map(CorpusVisitor)
    }
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root above crates/clinker")
        .to_path_buf()
}

fn normalize_fragment_key(key: &str) -> Result<String, String> {
    if key.contains('\\')
        || Path::new(key).is_absolute()
        || Path::new(key)
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(format!(
            "path-escaping corpus key {key:?}: expected a normalized repository-relative path"
        ));
    }
    let prefix = format!("{COMPOSITION_ROOT}/");
    if !key.starts_with(&prefix) || !key.ends_with(".comp.yaml") {
        return Err(format!(
            "path-escaping corpus key {key:?}: expected {COMPOSITION_ROOT}/**/*.comp.yaml"
        ));
    }
    Ok(key.to_owned())
}

fn discover_composition_fragments(repo: &Path) -> Result<Vec<String>, String> {
    let root = repo.join(COMPOSITION_ROOT);
    if !root.is_dir() {
        return Err(format!(
            "composition inventory root {COMPOSITION_ROOT} is missing"
        ));
    }
    let mut fragments = Vec::new();
    discover_under(repo, &root, &mut fragments)?;
    fragments.sort();
    if fragments.is_empty() {
        return Err(format!(
            "composition inventory under {COMPOSITION_ROOT} is empty"
        ));
    }
    Ok(fragments)
}

fn discover_under(
    repo: &Path,
    directory: &Path,
    fragments: &mut Vec<String>,
) -> Result<(), String> {
    let mut entries = std::fs::read_dir(directory)
        .map_err(|error| format!("cannot read composition inventory: {error}"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("cannot read composition inventory entry: {error}"))?;
    entries.sort_by_key(std::fs::DirEntry::file_name);
    for entry in entries {
        let file_type = entry
            .file_type()
            .map_err(|error| format!("cannot inspect composition inventory entry: {error}"))?;
        let path = entry.path();
        let relative = path
            .strip_prefix(repo)
            .ok()
            .and_then(Path::to_str)
            .ok_or_else(|| "composition inventory contains a non-UTF-8 path escape".to_owned())?;
        if file_type.is_symlink() {
            return Err(format!(
                "path-escaping composition inventory entry {relative:?}: symlinks are not admitted"
            ));
        }
        if file_type.is_dir() {
            discover_under(repo, &path, fragments)?;
        } else if file_type.is_file() && relative.ends_with(".comp.yaml") {
            fragments.push(normalize_fragment_key(relative)?);
        }
    }
    Ok(())
}

fn load_corpus_cases(path: &Path) -> Result<CorpusCases, String> {
    let raw = std::fs::read_to_string(path)
        .map_err(|error| format!("cannot read {CORPUS_MANIFEST}: {error}"))?;
    let cases: CorpusCases = serde_json::from_str(&raw)
        .map_err(|error| format!("invalid {CORPUS_MANIFEST}: {error}"))?;
    for key in cases.0.keys() {
        normalize_fragment_key(key)?;
    }
    Ok(cases)
}

fn require_exact_case_set(fragments: &[String], cases: &CorpusCases) -> Result<(), String> {
    let fragments = fragments.iter().cloned().collect::<BTreeSet<_>>();
    let cases = cases.0.keys().cloned().collect::<BTreeSet<_>>();
    let missing = fragments.difference(&cases).cloned().collect::<Vec<_>>();
    if !missing.is_empty() {
        return Err(format!("missing corpus cases for: {}", missing.join(", ")));
    }
    let extra = cases.difference(&fragments).cloned().collect::<Vec<_>>();
    if !extra.is_empty() {
        return Err(format!("extra corpus cases for: {}", extra.join(", ")));
    }
    Ok(())
}

fn indent_yaml(yaml: &str, spaces: usize) -> String {
    let padding = " ".repeat(spaces);
    yaml.lines()
        .map(|line| format!("{padding}{line}\n"))
        .collect()
}

fn loader_codes(diagnostics: &[clinker_core_types::Diagnostic]) -> String {
    let mut codes = diagnostics
        .iter()
        .map(|diagnostic| diagnostic.code.as_str())
        .collect::<Vec<_>>();
    codes.sort_unstable();
    codes.dedup();
    codes.join(", ")
}

fn materialize_composition_case(
    repo: &Path,
    fragment_key: &str,
    case: &CorpusCase,
) -> Result<tempfile::TempDir, String> {
    let workspace = tempfile::tempdir()
        .map_err(|error| format!("{fragment_key}: cannot create case workspace: {error}"))?;
    let destination = workspace.path().join(fragment_key);
    std::fs::create_dir_all(
        destination
            .parent()
            .ok_or_else(|| format!("{fragment_key}: destination has no parent"))?,
    )
    .map_err(|error| format!("{fragment_key}: cannot create destination: {error}"))?;
    std::fs::copy(repo.join(fragment_key), destination)
        .map_err(|error| format!("{fragment_key}: cannot copy fragment: {error}"))?;

    let root = workspace.path().join(COMPOSITION_ROOT);
    let signatures = scan_workspace_signatures(&root).map_err(|diagnostics| {
        format!(
            "{fragment_key}: production loader rejected materialized fragments: {}",
            loader_codes(&diagnostics)
        )
    })?;
    let relative = fragment_key
        .strip_prefix(&format!("{COMPOSITION_ROOT}/"))
        .ok_or_else(|| format!("{fragment_key}: fragment is outside the composition root"))?;
    let signature = signatures
        .get(Path::new(relative))
        .ok_or_else(|| format!("{fragment_key}: production loader omitted the fragment"))?;
    if signature.inputs.len() != 1 || signature.outputs.len() != 1 {
        return Err(format!(
            "{fragment_key}: wrapper requires one input and one output, found {} and {}",
            signature.inputs.len(),
            signature.outputs.len()
        ));
    }
    let (input_port, input) = signature.inputs.first().expect("one input was established");
    let output_port = &signature
        .outputs
        .first()
        .expect("one output was established")
        .0;
    let schema = input.schema.as_ref().ok_or_else(|| {
        format!("{fragment_key}: corpus wrapper requires a declared input schema")
    })?;
    let schema = clinker_plan::yaml::to_string(schema)
        .map_err(|error| format!("{fragment_key}: cannot render input schema: {error}"))?;
    let config = if case.config.is_empty() {
        String::new()
    } else {
        let yaml = clinker_plan::yaml::to_string(&case.config)
            .map_err(|error| format!("{fragment_key}: cannot render case config: {error}"))?;
        format!("    config:\n{}", indent_yaml(&yaml, 6))
    };
    let pipeline = format!(
        "pipeline:\n  name: composition_corpus_case\nnodes:\n  - type: source\n    name: source\n    config:\n      name: source\n      type: csv\n      path: input.csv\n      options:\n        has_header: true\n      schema:\n{}  - type: composition\n    name: composed\n    input: source\n    use: {fragment_key}\n    inputs:\n      {input_port}: source\n{config}  - type: sink\n    name: result\n    input: composed.{output_port}\n    config:\n      name: result\n      type: csv\n      path: output.csv\n",
        indent_yaml(&schema, 8)
    );
    std::fs::write(workspace.path().join("pipeline.yaml"), pipeline)
        .map_err(|error| format!("{fragment_key}: cannot write wrapper: {error}"))?;
    std::fs::write(workspace.path().join("input.csv"), case.input.as_bytes())
        .map_err(|error| format!("{fragment_key}: cannot write input: {error}"))?;
    Ok(workspace)
}

fn parse_counters(stdout: &str) -> Option<Counters> {
    let tail = stdout
        .lines()
        .find(|line| line.contains("Pipeline complete:"))?
        .split("Pipeline complete:")
        .nth(1)?;
    let number = |label: &str| {
        tail.split(',')
            .find(|part| part.trim_end().ends_with(label))?
            .split_whitespace()
            .next()?
            .parse()
            .ok()
    };
    Some(Counters {
        total: number("total")?,
        ok: number("ok")?,
        written: number("written")?,
        dlq: number("dlq")?,
    })
}

fn sanitize(text: &[u8], workspace: &Path) -> String {
    String::from_utf8_lossy(text).replace(workspace.to_string_lossy().as_ref(), "<case-workspace>")
}

fn first_difference(expected: &[u8], actual: &[u8]) -> String {
    let expected = String::from_utf8_lossy(expected);
    let actual = String::from_utf8_lossy(actual);
    for (index, (left, right)) in expected.lines().zip(actual.lines()).enumerate() {
        if left != right {
            return format!(
                "first difference at line {}: expected {left:?}, actual {right:?}",
                index + 1
            );
        }
    }
    format!(
        "line counts differ: expected {}, actual {}",
        expected.lines().count(),
        actual.lines().count()
    )
}

fn run_composition_case(repo: &Path, fragment_key: &str, case: &CorpusCase) -> Result<(), String> {
    let workspace = materialize_composition_case(repo, fragment_key, case)?;
    let output = Command::new(env!("CARGO_BIN_EXE_clinker"))
        .current_dir(workspace.path())
        .args([
            "run",
            "pipeline.yaml",
            "--batch-id",
            "composition-corpus-fixed-batch",
            "--force",
        ])
        .output()
        .map_err(|error| format!("{fragment_key}: cannot start clinker: {error}"))?;
    let stdout = sanitize(&output.stdout, workspace.path());
    let stderr = sanitize(&output.stderr, workspace.path());
    let expected_exit = if case.counters.dlq == 0 { 0 } else { 2 };
    if output.status.code() != Some(expected_exit) {
        return Err(format!(
            "{fragment_key}: exit {:?}, expected {expected_exit}\nstdout:\n{stdout}\nstderr:\n{stderr}",
            output.status.code()
        ));
    }
    let counters = parse_counters(&stdout)
        .ok_or_else(|| format!("{fragment_key}: missing counters\n{stdout}\n{stderr}"))?;
    if counters != case.counters {
        return Err(format!(
            "{fragment_key}: counters {counters:?}, expected {:?}\n{stdout}\n{stderr}",
            case.counters
        ));
    }
    let actual = std::fs::read(workspace.path().join("output.csv"))
        .map_err(|error| format!("{fragment_key}: output.csv was not written: {error}"))?;
    if actual != case.expected.as_bytes() {
        return Err(format!(
            "{fragment_key}: output bytes differ: {}",
            first_difference(case.expected.as_bytes(), &actual)
        ));
    }
    Ok(())
}

fn load_inventory_and_cases() -> Result<(PathBuf, Vec<String>, CorpusCases), String> {
    let repo = repo_root();
    let fragments = discover_composition_fragments(&repo)?;
    let cases = load_corpus_cases(&repo.join(CORPUS_MANIFEST))?;
    require_exact_case_set(&fragments, &cases)?;
    Ok((repo, fragments, cases))
}

#[test]
fn clean_names_executes_exact_bytes() {
    let key = "examples/pipelines/compositions/clean_names.comp.yaml";
    let (repo, _fragments, cases) =
        load_inventory_and_cases().unwrap_or_else(|error| panic!("{error}"));
    let case = cases
        .0
        .get(key)
        .expect("clean_names case present after exact-set validation");
    run_composition_case(&repo, key, case).unwrap_or_else(|error| panic!("{error}"));
}

#[test]
fn every_composition_fragment_executes_exact_bytes() {
    let (repo, fragments, cases) =
        load_inventory_and_cases().unwrap_or_else(|error| panic!("{error}"));
    let failures = fragments
        .iter()
        .filter_map(|key| {
            let case = cases
                .0
                .get(key)
                .expect("case present after exact-set validation");
            run_composition_case(&repo, key, case).err()
        })
        .collect::<Vec<_>>();
    assert!(
        failures.is_empty(),
        "composition corpus failures:\n\n{}",
        failures.join("\n\n")
    );
}

#[test]
fn duplicate_corpus_keys_are_rejected() {
    let key = "examples/pipelines/compositions/clean_names.comp.yaml";
    let value = r#"{"input":"","expected":"","counters":{"total":0,"ok":0,"written":0,"dlq":0}}"#;
    let raw = format!(r#"{{"{key}":{value},"{key}":{value}}}"#);
    let error = serde_json::from_str::<CorpusCases>(&raw)
        .expect_err("duplicate corpus keys must fail")
        .to_string();
    assert!(error.contains("duplicate corpus key"), "{error}");
}

#[test]
fn path_escaping_corpus_keys_are_rejected() {
    for key in [
        "../clean_names.comp.yaml",
        "/examples/pipelines/compositions/clean_names.comp.yaml",
        "examples/pipelines/compositions/../clean_names.comp.yaml",
        "examples\\pipelines\\compositions\\clean_names.comp.yaml",
    ] {
        let error = normalize_fragment_key(key).expect_err("escaping key must fail");
        assert!(error.contains("path-escaping corpus key"), "{error}");
    }
}

#[test]
fn empty_and_missing_inventories_fail_distinctly() {
    let missing = tempfile::tempdir().expect("missing-inventory workspace");
    let missing_error = discover_composition_fragments(missing.path())
        .expect_err("missing inventory root must fail");
    assert!(missing_error.contains("is missing"), "{missing_error}");

    let empty = tempfile::tempdir().expect("empty-inventory workspace");
    std::fs::create_dir_all(empty.path().join(COMPOSITION_ROOT))
        .expect("create empty composition root");
    let empty_error =
        discover_composition_fragments(empty.path()).expect_err("empty inventory must fail");
    assert!(empty_error.contains("is empty"), "{empty_error}");
}

#[test]
fn missing_and_extra_case_keys_fail_distinctly() {
    fn case() -> CorpusCase {
        CorpusCase {
            input: String::new(),
            expected: String::new(),
            counters: Counters {
                total: 0,
                ok: 0,
                written: 0,
                dlq: 0,
            },
            config: BTreeMap::new(),
        }
    }
    let fragments = vec![
        "examples/pipelines/compositions/a.comp.yaml".to_owned(),
        "examples/pipelines/compositions/b.comp.yaml".to_owned(),
    ];
    let missing = CorpusCases(BTreeMap::from([(
        "examples/pipelines/compositions/a.comp.yaml".to_owned(),
        case(),
    )]));
    let error = require_exact_case_set(&fragments, &missing).expect_err("missing case must fail");
    assert!(error.starts_with("missing corpus cases for:"), "{error}");
    assert!(!error.contains("extra corpus cases for:"), "{error}");

    let extra = CorpusCases(BTreeMap::from([
        (
            "examples/pipelines/compositions/a.comp.yaml".to_owned(),
            case(),
        ),
        (
            "examples/pipelines/compositions/b.comp.yaml".to_owned(),
            case(),
        ),
        (
            "examples/pipelines/compositions/c.comp.yaml".to_owned(),
            case(),
        ),
    ]));
    let error = require_exact_case_set(&fragments, &extra).expect_err("extra case must fail");
    assert!(error.starts_with("extra corpus cases for:"), "{error}");
    assert!(!error.contains("missing corpus cases for:"), "{error}");
}

#[test]
fn exact_set_validation_is_order_independent() {
    let mut fragments = vec![
        "examples/pipelines/compositions/b.comp.yaml".to_owned(),
        "examples/pipelines/compositions/a.comp.yaml".to_owned(),
    ];
    let cases = CorpusCases(BTreeMap::from([
        (
            "examples/pipelines/compositions/a.comp.yaml".to_owned(),
            CorpusCase {
                input: String::new(),
                expected: String::new(),
                counters: Counters {
                    total: 0,
                    ok: 0,
                    written: 0,
                    dlq: 0,
                },
                config: BTreeMap::new(),
            },
        ),
        (
            "examples/pipelines/compositions/b.comp.yaml".to_owned(),
            CorpusCase {
                input: String::new(),
                expected: String::new(),
                counters: Counters {
                    total: 0,
                    ok: 0,
                    written: 0,
                    dlq: 0,
                },
                config: BTreeMap::new(),
            },
        ),
    ]));
    require_exact_case_set(&fragments, &cases).expect("forward order must match");
    fragments.reverse();
    require_exact_case_set(&fragments, &cases).expect("reverse order must match");
}

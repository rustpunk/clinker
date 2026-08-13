use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use serde_json::Value as JsonValue;
use toml::Value as TomlValue;

use crate::sha256::digest_hex;
use crate::{BoundaryError, BoundaryResult, Scope};

const CORE_CRATE: &str = "clinker-core-types";
const ALLOWED_SHARED_TYPES: [&str; 3] = ["FailureCategory", "FailureClassification", "RetryAdvice"];
const INTERNAL_CRATES: [&str; 15] = [
    "clinker",
    "clinker-bench-support",
    "clinker-benchmarks",
    "clinker-channel",
    "clinker-core-types",
    "clinker-exec",
    "clinker-format",
    "clinker-lineage",
    "clinker-net",
    "clinker-plan",
    "clinker-record",
    "clinker-scenarios",
    "clinker-schema",
    "cxl",
    "cxl-cli",
];
const CORE_DEPENDENCIES: [&str; 3] = ["miette", "petgraph", "serde-saphyr"];
const NETWORK_DEPENDENCIES: [&str; 9] = [
    "clinker-core-types",
    "clinker-exec",
    "clinker-format",
    "clinker-plan",
    "clinker-record",
    "indexmap",
    "serde_json",
    "tracing",
    "ureq",
];
const NETWORK_DEV_DEPENDENCIES: [&str; 2] = ["clinker-bench-support", "clinker-exec"];
const LINEAGE_DEPENDENCIES: [&str; 7] = [
    "clinker-core-types",
    "clinker-plan",
    "clinker-record",
    "cxl",
    "petgraph",
    "serde",
    "serde_json",
];
const NETWORK_METADATA_DEPENDENCIES: [ExpectedMetadataDependency; 11] = [
    ExpectedMetadataDependency::normal("clinker-core-types", &[], true),
    ExpectedMetadataDependency::normal("clinker-exec", &[], true),
    ExpectedMetadataDependency::normal("clinker-format", &[], true),
    ExpectedMetadataDependency::normal("clinker-plan", &[], true),
    ExpectedMetadataDependency::normal("clinker-record", &[], true),
    ExpectedMetadataDependency::normal("indexmap", &["serde"], true),
    ExpectedMetadataDependency::normal("serde_json", &["preserve_order"], true),
    ExpectedMetadataDependency::normal("tracing", &[], true),
    ExpectedMetadataDependency::normal("ureq", &["rustls"], false),
    ExpectedMetadataDependency::development("clinker-bench-support", &[]),
    ExpectedMetadataDependency::development("clinker-exec", &["test-utils"]),
];
const LINEAGE_METADATA_DEPENDENCIES: [ExpectedMetadataDependency; 7] = [
    ExpectedMetadataDependency::normal("clinker-core-types", &[], true),
    ExpectedMetadataDependency::normal("clinker-plan", &[], true),
    ExpectedMetadataDependency::normal("clinker-record", &[], true),
    ExpectedMetadataDependency::normal("cxl", &[], true),
    ExpectedMetadataDependency::normal("petgraph", &[], true),
    ExpectedMetadataDependency::normal("serde", &["derive", "rc"], true),
    ExpectedMetadataDependency::normal("serde_json", &["preserve_order"], true),
];
/// The resolved graph this policy has been shown, as a count and a digest over
/// every locked package's name, version, source and checksum.
///
/// It says nothing about which packages those are on purpose: naming them here
/// would be a second list to keep true beside `Cargo.toml`, and the two would
/// disagree. What each third-party edge is for, and why it rather than an
/// alternative, is recorded next to its declaration in the workspace manifest;
/// this pair only refuses a graph nobody has looked at.
///
/// Both move together, and moving them is the recorded act of approving what
/// changed — so change them only for an addition that has actually been
/// approved, never to make a red gate green. The values are whatever
/// [`check_lock_membership`] computes for the approved `Cargo.lock`, which is
/// what its own failure reports as `found`.
const LOCK_PACKAGE_COUNT: usize = 308;
pub const LOCK_PACKAGE_DIGEST: &str =
    "a6a8aa1a76d44e16dc67e954ec61581fa06d276e1fcf9e0fc6829bddad4666c2";

#[derive(Clone, Copy)]
struct ExpectedMetadataDependency {
    name: &'static str,
    kind: Option<&'static str>,
    features: &'static [&'static str],
    uses_default_features: bool,
}

impl ExpectedMetadataDependency {
    const fn normal(
        name: &'static str,
        features: &'static [&'static str],
        uses_default_features: bool,
    ) -> Self {
        Self {
            name,
            kind: None,
            features,
            uses_default_features,
        }
    }

    const fn development(name: &'static str, features: &'static [&'static str]) -> Self {
        Self {
            name,
            kind: Some("dev"),
            features,
            uses_default_features: true,
        }
    }
}

pub(crate) fn check_core(root: &Path) -> BoundaryResult<()> {
    let manifest = read_manifest(root, CORE_CRATE)?;
    reject_build_or_feature_expansion(root, CORE_CRATE, &manifest)?;
    for dependency in dependency_keys(&manifest, "dependencies")? {
        if INTERNAL_CRATES.contains(&dependency.as_str()) {
            return Err(BoundaryError::new(format!(
                "{CORE_CRATE} must not add internal workspace dependency {dependency}"
            )));
        }
    }
    require_exact_dependencies(CORE_CRATE, &manifest, "dependencies", &CORE_DEPENDENCIES)?;
    require_exact_dependencies(CORE_CRATE, &manifest, "dev-dependencies", &[])?;
    require_exact_workspace_declarations(CORE_CRATE, &manifest, "dependencies", None)?;
    Ok(())
}

pub(crate) fn check_consumer(root: &Path, crate_name: &str) -> BoundaryResult<()> {
    let manifest = read_manifest(root, crate_name)?;
    reject_build_or_feature_expansion(root, crate_name, &manifest)?;
    check_exact_core_edge(crate_name, &manifest)?;
    match crate_name {
        "clinker-net" => {
            require_exact_dependencies(
                crate_name,
                &manifest,
                "dependencies",
                &NETWORK_DEPENDENCIES,
            )?;
            require_exact_dependencies(
                crate_name,
                &manifest,
                "dev-dependencies",
                &NETWORK_DEV_DEPENDENCIES,
            )?;
            require_exact_workspace_declarations(crate_name, &manifest, "dependencies", None)?;
            require_exact_workspace_declarations(
                crate_name,
                &manifest,
                "dev-dependencies",
                Some(("clinker-exec", &["test-utils"])),
            )
        }
        "clinker-lineage" => {
            require_exact_dependencies(
                crate_name,
                &manifest,
                "dependencies",
                &LINEAGE_DEPENDENCIES,
            )?;
            require_exact_dependencies(crate_name, &manifest, "dev-dependencies", &[])?;
            require_exact_workspace_declarations(crate_name, &manifest, "dependencies", None)
        }
        _ => Err(BoundaryError::new(format!(
            "unsupported dependency policy consumer {crate_name}"
        ))),
    }
}

fn require_exact_workspace_declarations(
    crate_name: &str,
    manifest: &TomlValue,
    section: &str,
    featureful: Option<(&str, &[&str])>,
) -> BoundaryResult<()> {
    let Some(dependencies) = table(manifest, section)? else {
        return Ok(());
    };
    for (dependency, declaration) in dependencies {
        let declaration = declaration.as_table().ok_or_else(|| {
            BoundaryError::new(format!(
                "{crate_name} [{section}] {dependency} must be an inherited workspace dependency"
            ))
        })?;
        let exact = match featureful {
            Some((featureful_dependency, expected_features))
                if dependency == featureful_dependency =>
            {
                let actual_features: Option<Vec<&str>> = declaration
                    .get("features")
                    .and_then(TomlValue::as_array)
                    .map(|features| features.iter().filter_map(TomlValue::as_str).collect());
                declaration.len() == 2
                    && declaration.get("workspace") == Some(&TomlValue::Boolean(true))
                    && actual_features.as_deref() == Some(expected_features)
            }
            _ => {
                declaration.len() == 1
                    && declaration.get("workspace") == Some(&TomlValue::Boolean(true))
            }
        };
        if !exact {
            return Err(BoundaryError::new(format!(
                "{crate_name} [{section}] {dependency} must retain its exact preapproved workspace declaration"
            )));
        }
    }
    Ok(())
}

fn read_manifest(root: &Path, crate_name: &str) -> BoundaryResult<TomlValue> {
    let path = root.join("crates").join(crate_name).join("Cargo.toml");
    let text = fs::read_to_string(&path)
        .map_err(|error| BoundaryError::new(format!("cannot read {}: {error}", path.display())))?;
    toml::from_str(&text)
        .map_err(|error| BoundaryError::new(format!("cannot parse {}: {error}", path.display())))
}

fn table<'a>(value: &'a TomlValue, key: &str) -> BoundaryResult<Option<&'a toml::Table>> {
    match value.get(key) {
        None => Ok(None),
        Some(TomlValue::Table(table)) => Ok(Some(table)),
        Some(_) => Err(BoundaryError::new(format!(
            "Cargo manifest [{key}] must be a table"
        ))),
    }
}

fn dependency_keys(manifest: &TomlValue, section: &str) -> BoundaryResult<BTreeSet<String>> {
    Ok(table(manifest, section)?
        .into_iter()
        .flat_map(|table| table.keys().cloned())
        .collect())
}

fn require_exact_dependencies(
    crate_name: &str,
    manifest: &TomlValue,
    section: &str,
    expected: &[&str],
) -> BoundaryResult<()> {
    let actual = dependency_keys(manifest, section)?;
    let expected: BTreeSet<String> = expected.iter().map(|value| (*value).to_owned()).collect();
    if actual != expected {
        return Err(BoundaryError::new(format!(
            "{crate_name} [{section}] must contain exactly the preapproved dependencies; expected={expected:?}, actual={actual:?}"
        )));
    }
    Ok(())
}

fn reject_build_or_feature_expansion(
    root: &Path,
    crate_name: &str,
    manifest: &TomlValue,
) -> BoundaryResult<()> {
    reject_target_dependency_expansion(crate_name, manifest)?;
    for target_table in ["lib", "bin", "example", "bench"] {
        if manifest.get(target_table).is_some() {
            return Err(BoundaryError::new(format!(
                "{crate_name} must not add an explicit {target_table} target under dependency policy"
            )));
        }
    }
    if manifest
        .get("package")
        .and_then(TomlValue::as_table)
        .is_some_and(|package| package.contains_key("build"))
        || root
            .join("crates")
            .join(crate_name)
            .join("build.rs")
            .exists()
    {
        return Err(BoundaryError::new(format!(
            "{crate_name} must not add a build script under dependency policy"
        )));
    }
    if manifest.get("features").is_some() {
        return Err(BoundaryError::new(format!(
            "{crate_name} must not add or expand a feature table under dependency policy"
        )));
    }
    if !dependency_keys(manifest, "build-dependencies")?.is_empty() {
        return Err(BoundaryError::new(format!(
            "{crate_name} must not add build-dependencies under dependency policy"
        )));
    }
    Ok(())
}

fn reject_target_dependency_expansion(
    crate_name: &str,
    manifest: &TomlValue,
) -> BoundaryResult<()> {
    let Some(targets) = table(manifest, "target")? else {
        return Ok(());
    };
    for (selector, target) in targets {
        let target = target.as_table().ok_or_else(|| {
            BoundaryError::new(format!(
                "{crate_name} Cargo manifest target {selector:?} must be a table"
            ))
        })?;
        for section in ["dependencies", "dev-dependencies", "build-dependencies"] {
            let dependencies = match target.get(section) {
                None => continue,
                Some(TomlValue::Table(dependencies)) => dependencies,
                Some(_) => {
                    return Err(BoundaryError::new(format!(
                        "{crate_name} target {selector:?} [{section}] must be a table"
                    )));
                }
            };
            if !dependencies.is_empty() {
                return Err(BoundaryError::new(format!(
                    "{crate_name} must not add target-specific {section} under dependency policy; target={selector:?}, dependencies={:?}",
                    dependencies.keys().collect::<Vec<_>>()
                )));
            }
        }
    }
    Ok(())
}

fn check_exact_core_edge(crate_name: &str, manifest: &TomlValue) -> BoundaryResult<()> {
    let dependencies = table(manifest, "dependencies")?
        .ok_or_else(|| BoundaryError::new(format!("{crate_name} has no [dependencies] table")))?;
    let declaration = dependencies.get(CORE_CRATE).ok_or_else(|| {
        BoundaryError::new(format!(
            "{crate_name} must have one normal dependency on {CORE_CRATE}"
        ))
    })?;
    let inherited = declaration.as_table().is_some_and(|table| {
        table.len() == 1 && table.get("workspace") == Some(&TomlValue::Boolean(true))
    });
    if !inherited {
        return Err(BoundaryError::new(format!(
            "{crate_name} -> {CORE_CRATE} must be declared exactly as {{ workspace = true }}"
        )));
    }
    if dependency_keys(manifest, "dev-dependencies")?.contains(CORE_CRATE) {
        return Err(BoundaryError::new(format!(
            "{crate_name} must not retain a second dev-only {CORE_CRATE} edge"
        )));
    }
    Ok(())
}

pub(crate) fn check_lock_membership(root: &Path) -> BoundaryResult<()> {
    let path = root.join("Cargo.lock");
    let text = fs::read_to_string(&path)
        .map_err(|error| BoundaryError::new(format!("cannot read {}: {error}", path.display())))?;
    let lock: TomlValue = toml::from_str(&text)
        .map_err(|error| BoundaryError::new(format!("cannot parse {}: {error}", path.display())))?;
    let packages = lock
        .get("package")
        .and_then(TomlValue::as_array)
        .ok_or_else(|| BoundaryError::new("Cargo.lock package table is malformed"))?;
    let mut rows = Vec::with_capacity(packages.len());
    for package in packages {
        let package = package
            .as_table()
            .ok_or_else(|| BoundaryError::new("Cargo.lock package entry is not a table"))?;
        let value = |key: &str| package.get(key).and_then(TomlValue::as_str).unwrap_or("");
        rows.push(format!(
            "{}\t{}\t{}\t{}",
            value("name"),
            value("version"),
            value("source"),
            value("checksum")
        ));
    }
    rows.sort_unstable();
    let payload = rows.join("\n");
    let digest = digest_hex(payload.as_bytes());
    if rows.len() != LOCK_PACKAGE_COUNT || digest != LOCK_PACKAGE_DIGEST {
        return Err(BoundaryError::new(format!(
            "Cargo.lock package membership changed outside dependency policy; expected count={LOCK_PACKAGE_COUNT} digest={LOCK_PACKAGE_DIGEST}, found count={} digest={digest}",
            rows.len()
        )));
    }
    Ok(())
}

pub(crate) fn check_metadata(
    root: &Path,
    metadata: &JsonValue,
    scope: Scope,
) -> BoundaryResult<()> {
    let core = metadata_package(metadata, CORE_CRATE)?;
    check_package_target(root, CORE_CRATE, core)?;
    check_core_metadata(core)?;
    let mut consumers = Vec::new();
    if matches!(scope, Scope::ClinkerNet | Scope::Final) {
        consumers.push("clinker-net");
    }
    if matches!(scope, Scope::ClinkerLineage | Scope::Final) {
        consumers.push("clinker-lineage");
    }
    for crate_name in consumers {
        let package = metadata_package(metadata, crate_name)?;
        check_package_target(root, crate_name, package)?;
        let expected = match crate_name {
            "clinker-net" => &NETWORK_METADATA_DEPENDENCIES[..],
            "clinker-lineage" => &LINEAGE_METADATA_DEPENDENCIES[..],
            _ => {
                return Err(BoundaryError::new(format!(
                    "unsupported dependency policy metadata consumer {crate_name}"
                )));
            }
        };
        check_exact_metadata_dependencies(root, package, crate_name, expected)?;
    }
    Ok(())
}

fn check_exact_metadata_dependencies(
    root: &Path,
    package: &JsonValue,
    crate_name: &str,
    expected: &[ExpectedMetadataDependency],
) -> BoundaryResult<()> {
    let dependencies = package
        .get("dependencies")
        .and_then(JsonValue::as_array)
        .ok_or_else(|| {
            BoundaryError::new(format!("metadata package {crate_name} lacks dependencies"))
        })?;
    if dependencies.len() != expected.len() {
        return Err(BoundaryError::new(format!(
            "cargo metadata shows dependency expansion in {crate_name}"
        )));
    }
    for expected_edge in expected {
        let matches: Vec<&JsonValue> = dependencies
            .iter()
            .filter(|dependency| {
                dependency.get("name").and_then(JsonValue::as_str) == Some(expected_edge.name)
                    && metadata_dependency_kind(dependency) == expected_edge.kind
            })
            .collect();
        if matches.len() != 1 {
            return Err(BoundaryError::new(format!(
                "cargo metadata must contain exactly one {crate_name} -> {} edge of kind {:?}",
                expected_edge.name, expected_edge.kind
            )));
        }
        let edge = matches[0];
        if INTERNAL_CRATES.contains(&expected_edge.name) {
            let expected_path = root.join("crates").join(expected_edge.name);
            let actual_path = edge.get("path").and_then(JsonValue::as_str);
            let source_is_local = edge.get("source").is_none_or(JsonValue::is_null);
            if actual_path.map(Path::new) != Some(expected_path.as_path()) || !source_is_local {
                return Err(BoundaryError::new(format!(
                    "cargo metadata routes {crate_name} -> {} through an unapproved package source; expected {}",
                    expected_edge.name,
                    expected_path.display()
                )));
            }
        } else if edge.get("path").is_some_and(|path| !path.is_null()) {
            return Err(BoundaryError::new(format!(
                "cargo metadata routes external dependency {crate_name} -> {} through an unapproved local path",
                expected_edge.name
            )));
        }
        let actual_features: BTreeSet<&str> = edge
            .get("features")
            .and_then(JsonValue::as_array)
            .into_iter()
            .flatten()
            .filter_map(JsonValue::as_str)
            .collect();
        let expected_features: BTreeSet<&str> = expected_edge.features.iter().copied().collect();
        let unrenamed = edge.get("rename").is_none_or(JsonValue::is_null);
        let required = !edge
            .get("optional")
            .and_then(JsonValue::as_bool)
            .unwrap_or(false);
        let uses_default = edge
            .get("uses_default_features")
            .and_then(JsonValue::as_bool)
            .unwrap_or(true);
        let untargeted = edge.get("target").is_none_or(JsonValue::is_null);
        if actual_features != expected_features
            || !unrenamed
            || !required
            || uses_default != expected_edge.uses_default_features
            || !untargeted
        {
            return Err(BoundaryError::new(format!(
                "cargo metadata shows feature, rename, optional, default-feature, or target expansion on {crate_name} -> {}",
                expected_edge.name
            )));
        }
    }
    Ok(())
}

fn metadata_dependency_kind(dependency: &JsonValue) -> Option<&str> {
    dependency.get("kind").and_then(JsonValue::as_str)
}

fn check_package_target(root: &Path, crate_name: &str, package: &JsonValue) -> BoundaryResult<()> {
    let crate_root = root.join("crates").join(crate_name);
    let expected_manifest = crate_root.join("Cargo.toml");
    let manifest_path = package
        .get("manifest_path")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| {
            BoundaryError::new(format!(
                "metadata package {crate_name} lacks a manifest_path"
            ))
        })?;
    if Path::new(manifest_path) != expected_manifest {
        return Err(BoundaryError::new(format!(
            "cargo metadata routes {crate_name} through unexpected manifest {manifest_path}; expected {}",
            expected_manifest.display()
        )));
    }

    let targets = package
        .get("targets")
        .and_then(JsonValue::as_array)
        .ok_or_else(|| {
            BoundaryError::new(format!("metadata package {crate_name} lacks targets"))
        })?;
    if targets
        .iter()
        .any(|target| target_has_kind(target, "custom-build"))
    {
        return Err(BoundaryError::new(format!(
            "cargo metadata shows a forbidden custom-build target for {crate_name}"
        )));
    }
    if targets.iter().any(|target| target_has_kind(target, "bin")) {
        return Err(BoundaryError::new(format!(
            "cargo metadata shows an unapproved binary target for {crate_name}"
        )));
    }

    let libraries: Vec<&JsonValue> = targets
        .iter()
        .filter(|target| target_has_kind(target, "lib"))
        .collect();
    if libraries.len() != 1 {
        return Err(BoundaryError::new(format!(
            "cargo metadata must show exactly one library target for {crate_name}"
        )));
    }
    let library = libraries[0];
    let expected_source = crate_root.join("src/lib.rs");
    let source_path = library
        .get("src_path")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| {
            BoundaryError::new(format!(
                "metadata library target for {crate_name} lacks src_path"
            ))
        })?;
    let crate_types = library
        .get("crate_types")
        .and_then(JsonValue::as_array)
        .ok_or_else(|| {
            BoundaryError::new(format!(
                "metadata library target for {crate_name} lacks crate_types"
            ))
        })?;
    let exact_crate_type =
        crate_types.len() == 1 && crate_types.first().and_then(JsonValue::as_str) == Some("lib");
    if Path::new(source_path) != expected_source || !exact_crate_type {
        return Err(BoundaryError::new(format!(
            "cargo metadata routes {crate_name} library through an unapproved source or crate type; expected {} as crate type lib",
            expected_source.display()
        )));
    }
    Ok(())
}

fn target_has_kind(target: &JsonValue, expected: &str) -> bool {
    target
        .get("kind")
        .and_then(JsonValue::as_array)
        .is_some_and(|kinds| kinds.iter().any(|kind| kind.as_str() == Some(expected)))
}

fn check_core_metadata(package: &JsonValue) -> BoundaryResult<()> {
    let dependencies = package
        .get("dependencies")
        .and_then(JsonValue::as_array)
        .ok_or_else(|| {
            BoundaryError::new("metadata package clinker-core-types lacks dependencies")
        })?;
    let expected = [
        ("miette", &["fancy"][..]),
        ("petgraph", &[][..]),
        ("serde-saphyr", &["miette"][..]),
    ];
    if dependencies.len() != expected.len() {
        return Err(BoundaryError::new(
            "cargo metadata shows dependency expansion in clinker-core-types",
        ));
    }
    for (name, features) in expected {
        let matches: Vec<&JsonValue> = dependencies
            .iter()
            .filter(|dependency| dependency.get("name").and_then(JsonValue::as_str) == Some(name))
            .collect();
        if matches.len() != 1 {
            return Err(BoundaryError::new(format!(
                "cargo metadata must contain exactly one clinker-core-types -> {name} edge"
            )));
        }
        let edge = matches[0];
        if edge.get("path").is_some_and(|path| !path.is_null()) {
            return Err(BoundaryError::new(format!(
                "cargo metadata routes clinker-core-types -> {name} through an unapproved local path"
            )));
        }
        let actual_features: Option<Vec<&str>> = edge
            .get("features")
            .and_then(JsonValue::as_array)
            .map(|values| values.iter().filter_map(JsonValue::as_str).collect());
        let normal = edge.get("kind").is_none_or(JsonValue::is_null);
        let unrenamed = edge.get("rename").is_none_or(JsonValue::is_null);
        let required = !edge
            .get("optional")
            .and_then(JsonValue::as_bool)
            .unwrap_or(false);
        let uses_default = edge
            .get("uses_default_features")
            .and_then(JsonValue::as_bool)
            .unwrap_or(true);
        let untargeted = edge.get("target").is_none_or(JsonValue::is_null);
        if actual_features.as_deref() != Some(features)
            || !normal
            || !unrenamed
            || !required
            || !uses_default
            || !untargeted
        {
            return Err(BoundaryError::new(format!(
                "cargo metadata shows feature, kind, rename, optional, default-feature, or target expansion on clinker-core-types -> {name}"
            )));
        }
    }
    Ok(())
}

fn metadata_package<'a>(metadata: &'a JsonValue, name: &str) -> BoundaryResult<&'a JsonValue> {
    let packages = metadata
        .get("packages")
        .and_then(JsonValue::as_array)
        .ok_or_else(|| BoundaryError::new("cargo metadata packages field is malformed"))?;
    let matches: Vec<&JsonValue> = packages
        .iter()
        .filter(|package| package.get("name").and_then(JsonValue::as_str) == Some(name))
        .collect();
    if matches.len() != 1 {
        return Err(BoundaryError::new(format!(
            "cargo metadata must contain exactly one {name} package"
        )));
    }
    Ok(matches[0])
}

pub(crate) fn check_final_crate_map(root: &Path) -> BoundaryResult<()> {
    let path = root.join("docs/ai/20_CRATE_MAP.md");
    let text = fs::read_to_string(&path)
        .map_err(|error| BoundaryError::new(format!("cannot read {}: {error}", path.display())))?
        .to_lowercase();
    let required = [
        "clinker-core-types::failure",
        "serialization-neutral",
        "clinker-net -> clinker-core-types",
        "clinker-lineage -> clinker-core-types",
        "failureclassification",
        "failurecategory",
        "retryadvice",
        "do not re-export",
        "semantic plan identity remains",
        "dataset identity remains",
    ];
    let missing: Vec<&str> = required
        .into_iter()
        .filter(|term| !text.contains(term))
        .collect();
    if !missing.is_empty() {
        return Err(BoundaryError::new(format!(
            "release crate map classification is incomplete; missing {missing:?}"
        )));
    }
    Ok(())
}

pub(crate) fn allowed_shared_types() -> &'static [&'static str] {
    &ALLOWED_SHARED_TYPES
}

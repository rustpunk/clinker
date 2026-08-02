//! Strict release inventory admission.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Component, Path, PathBuf};

use serde::Serialize;

use crate::error::GateError;

const INVENTORY_SCHEMA: &str = "clinker.release-inventory/v1";
const VERSION_SOURCE: &str = "Cargo.toml:workspace.package.version";
const TARGETS: [(&str, &str, &str); 4] = [
    ("x86_64-unknown-linux-gnu", "tar.gz", ""),
    ("x86_64-apple-darwin", "tar.gz", ""),
    ("aarch64-apple-darwin", "tar.gz", ""),
    ("x86_64-pc-windows-msvc", "zip", ".exe"),
];
const MEMBERS: [&str; 5] = [
    "clinker",
    "cxl",
    "README.md",
    "LICENSE",
    "release-manifest.json",
];

/// Fully validated, materialized release inventory.
#[derive(Debug, Clone, Serialize)]
pub struct ReleaseInventory {
    /// Versioned inventory schema.
    pub schema: String,
    /// Workspace release version.
    pub version: String,
    /// Workspace SPDX license.
    pub license: String,
    /// Repository-relative license file.
    pub license_file: String,
    /// Complete ordered binary declarations.
    pub binaries: Vec<BinarySpec>,
    /// Complete ordered target declarations.
    pub targets: Vec<TargetSpec>,
}

/// One executable in every suite archive.
#[derive(Debug, Clone, Serialize)]
pub struct BinarySpec {
    /// Cargo package name.
    pub package: String,
    /// Public executable name.
    pub name: String,
    /// Non-mutating smoke arguments.
    pub smoke_args: Vec<String>,
}

/// One materialized native release target.
#[derive(Debug, Clone, Serialize)]
pub struct TargetSpec {
    /// Rust target triple.
    pub target: String,
    /// Archive encoding.
    pub archive_format: String,
    /// Platform executable suffix.
    pub binary_suffix: String,
    /// Version-materialized archive filename.
    pub archive_name: String,
    /// Version-materialized archive root.
    pub root_name: String,
    /// Complete ordered archive member paths.
    pub members: Vec<String>,
}

/// Validated repository and inventory paths.
#[derive(Debug, Clone)]
pub struct InventoryPaths {
    /// Canonical repository root.
    pub repo_root: PathBuf,
    /// Canonical inventory path.
    pub inventory: PathBuf,
}

#[derive(Debug, Default)]
struct RawInventory {
    root: BTreeMap<String, TomlValue>,
    binaries: Vec<BTreeMap<String, TomlValue>>,
    targets: Vec<BTreeMap<String, TomlValue>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TomlValue {
    String(String),
    Strings(Vec<String>),
}

/// Load and validate the strict v1 inventory beneath an admitted repository root.
pub fn load(
    repo_root: &Path,
    inventory: Option<&Path>,
) -> Result<(InventoryPaths, ReleaseInventory), GateError> {
    let repo_root = fs::canonicalize(repo_root)
        .map_err(|error| GateError::io("resolve repository root", &error))?;
    if !repo_root.is_dir() {
        return Err(policy("repository root is not a directory"));
    }
    let requested = inventory
        .map(Path::to_path_buf)
        .unwrap_or_else(|| repo_root.join("release/inventory.toml"));
    let requested = if requested.is_absolute() {
        requested
    } else {
        repo_root.join(requested)
    };
    reject_parent_components(&requested)?;
    let inventory_path = fs::canonicalize(&requested)
        .map_err(|error| GateError::io("resolve release inventory", &error))?;
    if !inventory_path.starts_with(&repo_root) {
        return Err(policy(
            "release inventory must remain contained beneath the repository root",
        ));
    }
    let metadata = fs::symlink_metadata(&requested)
        .map_err(|error| GateError::io("inspect release inventory", &error))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(policy(
            "release inventory must be a regular non-symlink file",
        ));
    }
    let bytes = fs::read(&inventory_path)
        .map_err(|error| GateError::io("read release inventory", &error))?;
    let source =
        std::str::from_utf8(&bytes).map_err(|_| policy("release inventory must be UTF-8"))?;
    let raw = parse_inventory(source)?;
    let inventory = validate_inventory(&repo_root, raw)?;
    Ok((
        InventoryPaths {
            repo_root,
            inventory: inventory_path,
        },
        inventory,
    ))
}

/// Render the stable machine-readable inventory result.
pub fn render_json(inventory: &ReleaseInventory) -> Result<String, GateError> {
    let mut output = serde_json::to_string(inventory).map_err(|_| {
        GateError::internal(
            "inventory.serialize",
            "release inventory serialization failed",
        )
    })?;
    output.push('\n');
    Ok(output)
}

fn parse_inventory(source: &str) -> Result<RawInventory, GateError> {
    enum Section {
        Root,
        Binary(usize),
        Target(usize),
    }
    let mut raw = RawInventory::default();
    let mut section = Section::Root;
    for (index, original) in source.lines().enumerate() {
        let line_number = index + 1;
        let line = strip_comment(original).trim();
        if line.is_empty() {
            continue;
        }
        match line {
            "[[binaries]]" => {
                raw.binaries.push(BTreeMap::new());
                section = Section::Binary(raw.binaries.len() - 1);
                continue;
            }
            "[[targets]]" => {
                raw.targets.push(BTreeMap::new());
                section = Section::Target(raw.targets.len() - 1);
                continue;
            }
            _ if line.starts_with('[') => {
                return Err(policy(format!(
                    "inventory line {line_number} contains an unsupported table"
                )));
            }
            _ => {}
        }
        let (key, raw_value) = line.split_once('=').ok_or_else(|| {
            policy(format!(
                "inventory line {line_number} is not a key/value assignment"
            ))
        })?;
        let key = key.trim();
        if key.is_empty()
            || !key
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte == b'_')
        {
            return Err(policy(format!(
                "inventory line {line_number} contains an invalid key"
            )));
        }
        let value = parse_value(raw_value.trim(), line_number)?;
        let table = match section {
            Section::Root => &mut raw.root,
            Section::Binary(index) => &mut raw.binaries[index],
            Section::Target(index) => &mut raw.targets[index],
        };
        if table.insert(key.to_owned(), value).is_some() {
            return Err(policy(format!(
                "inventory line {line_number} duplicates field {key}"
            )));
        }
    }
    Ok(raw)
}

fn strip_comment(line: &str) -> &str {
    let mut quoted = false;
    let mut escaped = false;
    for (index, character) in line.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match character {
            '\\' if quoted => escaped = true,
            '"' => quoted = !quoted,
            '#' if !quoted => return &line[..index],
            _ => {}
        }
    }
    line
}

fn parse_value(value: &str, line: usize) -> Result<TomlValue, GateError> {
    if value.starts_with('"') {
        return serde_json::from_str::<String>(value)
            .map(TomlValue::String)
            .map_err(|_| policy(format!("inventory line {line} contains an invalid string")));
    }
    if value.starts_with('[') {
        return serde_json::from_str::<Vec<String>>(value)
            .map(TomlValue::Strings)
            .map_err(|_| {
                policy(format!(
                    "inventory line {line} contains an invalid string array"
                ))
            });
    }
    Err(policy(format!(
        "inventory line {line} uses an unsupported value type"
    )))
}

fn validate_inventory(
    repo_root: &Path,
    mut raw: RawInventory,
) -> Result<ReleaseInventory, GateError> {
    exact_keys(
        &raw.root,
        &[
            "schema",
            "version_source",
            "license",
            "license_file",
            "archive_prefix",
            "required_members",
        ],
        "release inventory",
    )?;
    expect_string(&mut raw.root, "schema", INVENTORY_SCHEMA)?;
    expect_string(&mut raw.root, "version_source", VERSION_SOURCE)?;
    expect_string(&mut raw.root, "license", "MIT")?;
    expect_string(&mut raw.root, "license_file", "LICENSE")?;
    expect_string(&mut raw.root, "archive_prefix", "clinker")?;
    let members = take_strings(&mut raw.root, "required_members")?;
    if members != MEMBERS {
        return Err(policy(
            "required_members must contain exactly clinker, cxl, README.md, LICENSE, release-manifest.json",
        ));
    }

    let (version, license) = workspace_package(repo_root)?;
    if license != "MIT" {
        return Err(policy(
            "workspace license must remain MIT for inventory schema v1",
        ));
    }
    validate_license(repo_root)?;
    validate_cxl_manifest(repo_root)?;

    if raw.binaries.len() != 2 {
        return Err(policy(
            "binary inventory must contain exactly clinker and cxl",
        ));
    }
    let mut binaries = Vec::with_capacity(2);
    for (index, mut entry) in raw.binaries.into_iter().enumerate() {
        exact_keys(&entry, &["package", "name", "smoke_args"], "binary entry")?;
        let package = take_string(&mut entry, "package")?;
        let name = take_string(&mut entry, "name")?;
        let smoke_args = take_strings(&mut entry, "smoke_args")?;
        let expected = if index == 0 {
            ("clinker", "clinker")
        } else {
            ("cxl-cli", "cxl")
        };
        if (package.as_str(), name.as_str()) != expected || smoke_args != ["--version"] {
            return Err(policy(
                "binary inventory must contain ordered clinker and cxl --version records",
            ));
        }
        binaries.push(BinarySpec {
            package,
            name,
            smoke_args,
        });
    }

    if raw.targets.len() != TARGETS.len() {
        return Err(policy("expected exactly four release targets"));
    }
    let mut seen_archives = BTreeSet::new();
    let mut targets = Vec::with_capacity(TARGETS.len());
    for (index, mut entry) in raw.targets.into_iter().enumerate() {
        exact_keys(
            &entry,
            &[
                "target",
                "archive_format",
                "binary_suffix",
                "archive_name",
                "root_name",
            ],
            "target entry",
        )?;
        let target = take_string(&mut entry, "target")?;
        let archive_format = take_string(&mut entry, "archive_format")?;
        let binary_suffix = take_string(&mut entry, "binary_suffix")?;
        let archive_template = take_string(&mut entry, "archive_name")?;
        let root_template = take_string(&mut entry, "root_name")?;
        let expected = TARGETS[index];
        if (
            target.as_str(),
            archive_format.as_str(),
            binary_suffix.as_str(),
        ) != expected
        {
            return Err(policy(
                "release targets must match the exact ordered four-target contract",
            ));
        }
        let expected_root = format!("clinker-v{{version}}-{target}");
        let expected_archive = format!(
            "{expected_root}.{}",
            if archive_format == "zip" {
                "zip"
            } else {
                "tar.gz"
            }
        );
        if root_template != expected_root || archive_template != expected_archive {
            return Err(policy(
                "target archive_name and root_name must use the canonical version template",
            ));
        }
        let root_name = root_template.replace("{version}", &version);
        let archive_name = archive_template.replace("{version}", &version);
        if !seen_archives.insert(archive_name.clone()) {
            return Err(policy("release archive names must be unique"));
        }
        let members = MEMBERS
            .iter()
            .map(|member| {
                let suffix = if matches!(*member, "clinker" | "cxl") {
                    binary_suffix.as_str()
                } else {
                    ""
                };
                format!("{root_name}/{member}{suffix}")
            })
            .collect();
        targets.push(TargetSpec {
            target,
            archive_format,
            binary_suffix,
            archive_name,
            root_name,
            members,
        });
    }

    Ok(ReleaseInventory {
        schema: INVENTORY_SCHEMA.to_owned(),
        version,
        license,
        license_file: "LICENSE".to_owned(),
        binaries,
        targets,
    })
}

fn exact_keys(
    table: &BTreeMap<String, TomlValue>,
    expected: &[&str],
    label: &str,
) -> Result<(), GateError> {
    let expected = expected.iter().copied().collect::<BTreeSet<_>>();
    let actual = table.keys().map(String::as_str).collect::<BTreeSet<_>>();
    let unknown = actual.difference(&expected).copied().collect::<Vec<_>>();
    let missing = expected.difference(&actual).copied().collect::<Vec<_>>();
    if !unknown.is_empty() {
        return Err(policy(format!(
            "{label} contains unknown fields: {}",
            unknown.join(", ")
        )));
    }
    if !missing.is_empty() {
        return Err(policy(format!(
            "{label} is missing fields: {}",
            missing.join(", ")
        )));
    }
    Ok(())
}

fn expect_string(
    table: &mut BTreeMap<String, TomlValue>,
    key: &str,
    expected: &str,
) -> Result<(), GateError> {
    if take_string(table, key)? != expected {
        return Err(policy(format!("{key} must be {expected}")));
    }
    Ok(())
}

fn take_string(table: &mut BTreeMap<String, TomlValue>, key: &str) -> Result<String, GateError> {
    match table.remove(key) {
        Some(TomlValue::String(value)) => Ok(value),
        _ => Err(policy(format!("{key} must be a string"))),
    }
}

fn take_strings(
    table: &mut BTreeMap<String, TomlValue>,
    key: &str,
) -> Result<Vec<String>, GateError> {
    match table.remove(key) {
        Some(TomlValue::Strings(value)) => Ok(value),
        _ => Err(policy(format!("{key} must be a string array"))),
    }
}

fn workspace_package(root: &Path) -> Result<(String, String), GateError> {
    let source = fs::read_to_string(root.join("Cargo.toml"))
        .map_err(|error| GateError::io("read workspace manifest", &error))?;
    let table = parse_named_table(&source, "workspace.package")?;
    let version = table
        .get("version")
        .cloned()
        .ok_or_else(|| policy("workspace.package.version is missing"))?;
    let license = table
        .get("license")
        .cloned()
        .ok_or_else(|| policy("workspace.package.license is missing"))?;
    if version.is_empty() {
        return Err(policy("workspace.package.version must not be empty"));
    }
    Ok((version, license))
}

fn validate_cxl_manifest(root: &Path) -> Result<(), GateError> {
    let source = fs::read_to_string(root.join("crates/cxl-cli/Cargo.toml"))
        .map_err(|error| GateError::io("read cxl-cli manifest", &error))?;
    let package = parse_named_table(&source, "package")?;
    if package.get("name").map(String::as_str) != Some("cxl-cli") {
        return Err(policy("cxl-cli package identity changed"));
    }
    let binary = parse_named_table(&source, "[bin]")?;
    if binary.get("name").map(String::as_str) != Some("cxl")
        || binary.get("path").map(String::as_str) != Some("src/main.rs")
    {
        return Err(policy(
            "cxl-cli must expose exactly the public cxl executable",
        ));
    }
    Ok(())
}

fn parse_named_table(source: &str, name: &str) -> Result<BTreeMap<String, String>, GateError> {
    let header = if name == "[bin]" {
        "[[bin]]".to_owned()
    } else {
        format!("[{name}]")
    };
    let mut active = false;
    let mut found = false;
    let mut result = BTreeMap::new();
    for original in source.lines() {
        let line = strip_comment(original).trim();
        if line.starts_with('[') {
            if active {
                break;
            }
            active = line == header;
            found |= active;
            continue;
        }
        if !active || line.is_empty() {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        if let Ok(value) = serde_json::from_str::<String>(value.trim()) {
            result.insert(key.trim().to_owned(), value);
        }
    }
    if !found {
        return Err(policy(format!(
            "required manifest table {header} is missing"
        )));
    }
    Ok(result)
}

fn validate_license(root: &Path) -> Result<(), GateError> {
    let text = fs::read_to_string(root.join("LICENSE"))
        .map_err(|error| GateError::io("read license", &error))?;
    for required in [
        "MIT License",
        "Permission is hereby granted, free of charge",
        "THE SOFTWARE IS PROVIDED \"AS IS\"",
    ] {
        if !text.contains(required) {
            return Err(policy(
                "LICENSE does not contain the canonical MIT grant and disclaimer",
            ));
        }
    }
    Ok(())
}

fn reject_parent_components(path: &Path) -> Result<(), GateError> {
    if path
        .components()
        .any(|component| component == Component::ParentDir)
    {
        return Err(policy(
            "release inventory path must not contain parent traversal",
        ));
    }
    Ok(())
}

fn policy(detail: impl Into<String>) -> GateError {
    GateError::policy("inventory.invalid", detail)
}

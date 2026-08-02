//! Deterministic release bundle and provenance generation.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::{Value, json};

use crate::child::{self, ChildSpec, Termination};
use crate::digest::sha256_hex;
use crate::error::GateError;
use crate::inventory::{BinarySpec, ReleaseInventory, TargetSpec};

const BUILD_REPOSITORY: &str = "rustpunk/clinker";
const BUILD_WORKFLOW: &str = ".github/workflows/release.yml";
const BUILDER_ID: &str = "https://github.com/actions/runner/github-hosted";
const BUILD_TYPE: &str = "https://github.com/Attestations/GitHubActionsWorkflow@v1";
const STATEMENT_TYPE: &str = "https://in-toto.io/Statement/v1";
const PREDICATE_TYPE: &str = "https://slsa.dev/provenance/v1";

/// Exact bundle-builder request.
#[derive(Debug, Clone)]
pub struct BuildRequest {
    /// Supported target triple.
    pub target: String,
    /// Lowercase full source commit.
    pub source_sha: String,
    /// Destination for the archive and sidecars.
    pub output_dir: PathBuf,
}

/// Exact assembly-verification request.
#[derive(Debug, Clone)]
pub struct AssemblyRequest {
    /// Directory containing all four archives and sidecars.
    pub asset_dir: PathBuf,
    /// Optional independently downloaded draft bytes.
    pub draft_dir: Option<PathBuf>,
    /// Exact repository identity.
    pub repository: String,
    /// Exact build workflow path.
    pub workflow: String,
    /// Exact protected tag ref.
    pub release_ref: String,
    /// Exact source commit.
    pub source_sha: String,
}

#[derive(Debug, Clone)]
struct ArchiveEntry {
    name: String,
    bytes: Vec<u8>,
    mode: u32,
}

/// Build one deterministic suite archive plus checksum and SLSA statement.
pub fn build(
    repo_root: &Path,
    inventory: &ReleaseInventory,
    request: &BuildRequest,
) -> Result<String, GateError> {
    validate_sha40(&request.source_sha, "--source-sha")?;
    let target = inventory
        .targets
        .iter()
        .find(|target| target.target == request.target)
        .ok_or_else(|| policy("target is not in release/inventory.toml"))?;
    let binary_dir = repo_root
        .join("target")
        .join(&target.target)
        .join("release");
    let mut entries = Vec::with_capacity(5);
    for binary in &inventory.binaries {
        entries.push(read_binary(
            binary,
            target,
            &binary_dir,
            &inventory.version,
        )?);
    }
    for (name, mode) in [
        ("README.md", 0o644),
        (inventory.license_file.as_str(), 0o644),
    ] {
        let path = repo_root.join(name);
        let bytes = read_regular(&path, "read release member")?;
        entries.push(ArchiveEntry {
            name: name.to_owned(),
            bytes,
            mode,
        });
    }
    entries.sort_by(|left, right| left.name.cmp(&right.name));
    let manifest = release_manifest(inventory, target, &request.source_sha, &entries)?;
    entries.push(ArchiveEntry {
        name: "release-manifest.json".to_owned(),
        bytes: manifest,
        mode: 0o644,
    });
    entries.sort_by(|left, right| left.name.cmp(&right.name));

    let members = entries
        .iter()
        .map(|entry| format!("{}/{}", target.root_name, entry.name))
        .collect::<BTreeSet<_>>();
    if members != target.members.iter().cloned().collect() {
        return Err(policy(
            "staged release members do not match release/inventory.toml",
        ));
    }
    let archive = if target.archive_format == "tar.gz" {
        gzip_store(&tar_archive(&target.root_name, &entries)?)
    } else {
        zip_archive(&target.root_name, &entries)?
    };
    let digest = sha256_hex(&archive);
    let sidecar = format!("{digest}  {}\n", target.archive_name).into_bytes();
    let provenance =
        provenance_statement(target, &inventory.version, &request.source_sha, &digest)?;

    admit_output_directory(&request.output_dir)?;
    write_exact(&request.output_dir.join(&target.archive_name), &archive)?;
    write_exact(
        &request
            .output_dir
            .join(format!("{}.sha256", target.archive_name)),
        &sidecar,
    )?;
    write_exact(
        &request
            .output_dir
            .join(format!("{}.intoto.jsonl", target.archive_name)),
        &provenance,
    )?;
    Ok(format!(
        "{}\n",
        request.output_dir.join(&target.archive_name).display()
    ))
}

/// Verify all archive bytes, sidecars, manifests, provenance, and optional draft reread.
pub fn verify_assembly(
    inventory: &ReleaseInventory,
    request: &AssemblyRequest,
) -> Result<String, GateError> {
    validate_sha40(&request.source_sha, "--source-sha")?;
    if request.repository != BUILD_REPOSITORY || request.workflow != BUILD_WORKFLOW {
        return Err(policy(
            "release provenance origin must be rustpunk/clinker via .github/workflows/release.yml",
        ));
    }
    if request.release_ref != format!("refs/tags/v{}", inventory.version) {
        return Err(policy(
            "release provenance ref does not match the inventory version",
        ));
    }
    if !request.asset_dir.is_dir() {
        return Err(policy("release asset set is empty"));
    }
    let expected = expected_assets(inventory);
    let actual = regular_file_names(&request.asset_dir)?;
    let required = expected
        .iter()
        .filter(|name| name.as_str() != "SHA256SUMS")
        .cloned()
        .collect::<BTreeSet<_>>();
    let missing = required.difference(&actual).cloned().collect::<Vec<_>>();
    let extra = actual.difference(&expected).cloned().collect::<Vec<_>>();
    if !missing.is_empty() {
        return Err(policy(format!(
            "missing release assets: {}",
            missing.join(", ")
        )));
    }
    if !extra.is_empty() {
        return Err(policy(format!(
            "extra release assets: {}",
            extra.join(", ")
        )));
    }

    let mut checksum_lines = Vec::with_capacity(inventory.targets.len());
    for target in &inventory.targets {
        let archive_path = request.asset_dir.join(&target.archive_name);
        let archive = read_regular(&archive_path, "read release archive")?;
        let digest = sha256_hex(&archive);
        let sidecar = read_regular(
            &request
                .asset_dir
                .join(format!("{}.sha256", target.archive_name)),
            "read release checksum",
        )?;
        let expected_sidecar = format!("{digest}  {}\n", target.archive_name);
        if sidecar != expected_sidecar.as_bytes() {
            return Err(policy(format!(
                "checksum sidecar does not match archive: {}",
                target.archive_name
            )));
        }
        verify_provenance(
            &request
                .asset_dir
                .join(format!("{}.intoto.jsonl", target.archive_name)),
            target,
            request,
            &digest,
        )?;
        verify_archive(inventory, target, &archive, &request.source_sha)?;
        checksum_lines.push(expected_sidecar);
    }
    checksum_lines.sort();
    let checksums = checksum_lines.concat().into_bytes();
    let checksum_path = request.asset_dir.join("SHA256SUMS");
    if checksum_path.exists() {
        if read_regular(&checksum_path, "read unified checksums")? != checksums {
            return Err(policy(
                "existing SHA256SUMS does not match verified archives",
            ));
        }
    } else {
        write_exact(&checksum_path, &checksums)?;
    }

    if let Some(draft_dir) = &request.draft_dir {
        if regular_file_names(draft_dir)? != expected {
            return Err(policy(
                "draft asset inventory differs from verified inventory",
            ));
        }
        for name in &expected {
            if read_regular(&request.asset_dir.join(name), "read verified asset")?
                != read_regular(&draft_dir.join(name), "read draft asset")?
            {
                return Err(policy(format!(
                    "draft asset differs from verified inventory: {name}"
                )));
            }
        }
    }
    Ok("Release archive integrity and structured provenance metadata verified.\nNOTE: digest and metadata checks do not cryptographically authenticate artifact origin.\n".to_owned())
}

fn read_binary(
    binary: &BinarySpec,
    target: &TargetSpec,
    binary_dir: &Path,
    version: &str,
) -> Result<ArchiveEntry, GateError> {
    let name = format!("{}{}", binary.name, target.binary_suffix);
    let path = binary_dir.join(&name);
    let bytes = read_regular(&path, "read release binary")?;
    let result = child::run(ChildSpec {
        program: path,
        arguments: binary.smoke_args.iter().map(OsString::from).collect(),
        environment: BTreeMap::new(),
        timeout: Duration::from_secs(15),
        output_limit: 16 * 1024,
    })?;
    if result.termination != Termination::Exited(Some(0))
        || result.stdout_truncated
        || result.stderr_truncated
    {
        return Err(policy(format!("{name} smoke check failed")));
    }
    let mut combined = result.stdout;
    combined.extend(result.stderr);
    if !String::from_utf8_lossy(&combined).contains(version) {
        return Err(policy(format!(
            "{name} smoke check did not report version {version}"
        )));
    }
    Ok(ArchiveEntry {
        name,
        bytes,
        mode: 0o755,
    })
}

fn release_manifest(
    inventory: &ReleaseInventory,
    target: &TargetSpec,
    source_sha: &str,
    entries: &[ArchiveEntry],
) -> Result<Vec<u8>, GateError> {
    let files = entries
        .iter()
        .map(|entry| {
            json!({
                "mode": format!("{:04o}", entry.mode),
                "name": entry.name,
                "sha256": sha256_hex(&entry.bytes),
            })
        })
        .collect::<Vec<_>>();
    json_line(&json!({
        "files": files,
        "schema": "clinker.release-manifest/v1",
        "source_sha": source_sha,
        "target": target.target,
        "version": inventory.version,
    }))
}

fn provenance_statement(
    target: &TargetSpec,
    version: &str,
    source_sha: &str,
    digest: &str,
) -> Result<Vec<u8>, GateError> {
    json_line(&json!({
        "_type": STATEMENT_TYPE,
        "predicate": {
            "buildDefinition": {
                "buildType": BUILD_TYPE,
                "externalParameters": {
                    "ref": format!("refs/tags/v{version}"),
                    "repository": BUILD_REPOSITORY,
                    "sourceSha": source_sha,
                    "workflow": BUILD_WORKFLOW,
                },
                "internalParameters": {},
                "resolvedDependencies": [],
            },
            "runDetails": {
                "builder": {"id": BUILDER_ID},
                "byproducts": [{"content": "github-hosted", "name": "runner_environment"}],
                "metadata": {"invocationId": format!("offline-build:{source_sha}:{}", target.target)},
            },
        },
        "predicateType": PREDICATE_TYPE,
        "subject": [{"digest": {"sha256": digest}, "name": target.archive_name}],
    }))
}

fn verify_provenance(
    path: &Path,
    target: &TargetSpec,
    request: &AssemblyRequest,
    digest: &str,
) -> Result<(), GateError> {
    let bytes = read_regular(path, "read release provenance")?;
    let value: Value = serde_json::from_slice(&bytes).map_err(|_| {
        policy(format!(
            "provenance is malformed for {}",
            target.archive_name
        ))
    })?;
    let object = exact_object(
        &value,
        &["_type", "subject", "predicateType", "predicate"],
        "attestation",
    )?;
    expect_value(
        object.get("_type"),
        STATEMENT_TYPE,
        "attestation statement type",
    )?;
    expect_value(
        object.get("predicateType"),
        PREDICATE_TYPE,
        "attestation predicate type",
    )?;
    let subjects = object
        .get("subject")
        .and_then(Value::as_array)
        .ok_or_else(|| policy("attestation subject must be an array"))?;
    if subjects.len() != 1 {
        return Err(policy("attestation must contain exactly one subject"));
    }
    let subject = exact_object(&subjects[0], &["name", "digest"], "attestation subject")?;
    expect_value(
        subject.get("name"),
        &target.archive_name,
        "attestation subject name",
    )?;
    let digest_object = exact_object(
        subject.get("digest").unwrap_or(&Value::Null),
        &["sha256"],
        "attestation digest",
    )?;
    expect_value(
        digest_object.get("sha256"),
        digest,
        "attestation subject digest",
    )?;
    let predicate = exact_object(
        object.get("predicate").unwrap_or(&Value::Null),
        &["buildDefinition", "runDetails"],
        "attestation predicate",
    )?;
    let definition = exact_object(
        predicate.get("buildDefinition").unwrap_or(&Value::Null),
        &[
            "buildType",
            "externalParameters",
            "internalParameters",
            "resolvedDependencies",
        ],
        "attestation build definition",
    )?;
    expect_value(
        definition.get("buildType"),
        BUILD_TYPE,
        "attestation build type",
    )?;
    let parameters = exact_object(
        definition.get("externalParameters").unwrap_or(&Value::Null),
        &["repository", "workflow", "ref", "sourceSha"],
        "attestation parameters",
    )?;
    for (field, expected) in [
        ("repository", request.repository.as_str()),
        ("workflow", request.workflow.as_str()),
        ("ref", request.release_ref.as_str()),
        ("sourceSha", request.source_sha.as_str()),
    ] {
        expect_value(
            parameters.get(field),
            expected,
            "attestation external parameter",
        )?;
    }
    let details = exact_object(
        predicate.get("runDetails").unwrap_or(&Value::Null),
        &["builder", "metadata", "byproducts"],
        "attestation run details",
    )?;
    let builder = exact_object(
        details.get("builder").unwrap_or(&Value::Null),
        &["id"],
        "attestation builder",
    )?;
    expect_value(builder.get("id"), BUILDER_ID, "attestation builder")?;
    let metadata = exact_object(
        details.get("metadata").unwrap_or(&Value::Null),
        &["invocationId"],
        "attestation metadata",
    )?;
    if metadata
        .get("invocationId")
        .and_then(Value::as_str)
        .is_none_or(str::is_empty)
    {
        return Err(policy("attestation invocation identity must not be empty"));
    }
    if details.get("byproducts")
        != Some(&json!([{"content": "github-hosted", "name": "runner_environment"}]))
    {
        return Err(policy("attestation runner environment does not match"));
    }
    Ok(())
}

fn verify_archive(
    inventory: &ReleaseInventory,
    target: &TargetSpec,
    archive: &[u8],
    source_sha: &str,
) -> Result<(), GateError> {
    let entries = if target.archive_format == "tar.gz" {
        read_tar(&gunzip_store(archive)?)?
    } else {
        read_zip(archive)?
    };
    let names = entries.keys().cloned().collect::<BTreeSet<_>>();
    if names != target.members.iter().cloned().collect() {
        return Err(policy(format!(
            "archive members do not match inventory: {}",
            target.archive_name
        )));
    }
    let manifest_name = format!("{}/release-manifest.json", target.root_name);
    let manifest: Value = serde_json::from_slice(
        entries
            .get(&manifest_name)
            .ok_or_else(|| policy("release manifest is absent"))?,
    )
    .map_err(|_| policy("release manifest is malformed"))?;
    let object = exact_object(
        &manifest,
        &["schema", "version", "target", "source_sha", "files"],
        "release manifest",
    )?;
    for (field, expected) in [
        ("schema", "clinker.release-manifest/v1"),
        ("version", inventory.version.as_str()),
        ("target", target.target.as_str()),
        ("source_sha", source_sha),
    ] {
        expect_value(object.get(field), expected, "release manifest identity")?;
    }
    let files = object
        .get("files")
        .and_then(Value::as_array)
        .ok_or_else(|| policy("release manifest files must be an array"))?;
    let mut observed = BTreeSet::new();
    for value in files {
        let entry = exact_object(value, &["name", "sha256", "mode"], "release manifest file")?;
        let name = entry
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| policy("release manifest name must be a string"))?;
        if !observed.insert(name.to_owned()) {
            return Err(policy(format!(
                "release manifest contains duplicate file: {name}"
            )));
        }
        let member_name = format!("{}/{name}", target.root_name);
        let bytes = entries.get(&member_name).ok_or_else(|| {
            policy(format!(
                "release manifest names missing archive member: {name}"
            ))
        })?;
        expect_value(
            entry.get("sha256"),
            &sha256_hex(bytes),
            "release manifest digest",
        )?;
        let expected_mode = if name == format!("clinker{}", target.binary_suffix)
            || name == format!("cxl{}", target.binary_suffix)
        {
            "0755"
        } else {
            "0644"
        };
        expect_value(entry.get("mode"), expected_mode, "release manifest mode")?;
    }
    let expected_names = target
        .members
        .iter()
        .filter_map(|name| Path::new(name).file_name()?.to_str().map(str::to_owned))
        .filter(|name| name != "release-manifest.json")
        .collect::<BTreeSet<_>>();
    if observed != expected_names {
        return Err(policy(
            "release manifest file inventory does not match archive",
        ));
    }
    Ok(())
}

fn tar_archive(root: &str, entries: &[ArchiveEntry]) -> Result<Vec<u8>, GateError> {
    let mut output = Vec::new();
    for entry in entries {
        let name = format!("{root}/{}", entry.name);
        if name.len() > 100 {
            return Err(policy(
                "release archive member name exceeds the v1 tar limit",
            ));
        }
        let mut header = [0_u8; 512];
        header[..name.len()].copy_from_slice(name.as_bytes());
        write_octal(&mut header[100..108], u64::from(entry.mode));
        write_octal(&mut header[108..116], 0);
        write_octal(&mut header[116..124], 0);
        write_octal(&mut header[124..136], entry.bytes.len() as u64);
        write_octal(&mut header[136..148], 0);
        header[148..156].fill(b' ');
        header[156] = b'0';
        header[257..263].copy_from_slice(b"ustar\0");
        header[263..265].copy_from_slice(b"00");
        let checksum = header.iter().map(|byte| u64::from(*byte)).sum();
        write_checksum(&mut header[148..156], checksum);
        output.extend(header);
        output.extend(&entry.bytes);
        output.resize(output.len().next_multiple_of(512), 0);
    }
    output.resize(output.len() + 1024, 0);
    Ok(output)
}

fn read_tar(bytes: &[u8]) -> Result<BTreeMap<String, Vec<u8>>, GateError> {
    let mut offset = 0_usize;
    let mut entries = BTreeMap::new();
    while offset + 512 <= bytes.len() {
        let header = &bytes[offset..offset + 512];
        if header.iter().all(|byte| *byte == 0) {
            break;
        }
        let name = nul_string(&header[..100])?;
        safe_member(&name)?;
        let size = parse_octal(&header[124..136])?;
        let start = offset + 512;
        let end = start
            .checked_add(size)
            .ok_or_else(|| policy("tar member size overflow"))?;
        if end > bytes.len() || entries.insert(name, bytes[start..end].to_vec()).is_some() {
            return Err(policy(
                "tar archive is truncated or contains duplicate members",
            ));
        }
        offset = end.next_multiple_of(512);
    }
    Ok(entries)
}

fn gzip_store(input: &[u8]) -> Vec<u8> {
    let mut output = vec![0x1f, 0x8b, 8, 0, 0, 0, 0, 0, 0, 255];
    if input.is_empty() {
        output.extend([1, 0, 0, 255, 255]);
    } else {
        let chunks = input.chunks(u16::MAX as usize).collect::<Vec<_>>();
        for (index, chunk) in chunks.iter().enumerate() {
            output.push(u8::from(index + 1 == chunks.len()));
            let length = chunk.len() as u16;
            output.extend(length.to_le_bytes());
            output.extend((!length).to_le_bytes());
            output.extend(*chunk);
        }
    }
    output.extend(crc32(input).to_le_bytes());
    output.extend((input.len() as u32).to_le_bytes());
    output
}

fn gunzip_store(bytes: &[u8]) -> Result<Vec<u8>, GateError> {
    if bytes.len() < 18 || bytes[..10] != [0x1f, 0x8b, 8, 0, 0, 0, 0, 0, 0, 255] {
        return Err(policy("release tar.gz header is invalid"));
    }
    let mut offset = 10_usize;
    let mut output = Vec::new();
    loop {
        if offset + 5 > bytes.len() - 8 {
            return Err(policy("release tar.gz deflate stream is truncated"));
        }
        let flags = bytes[offset];
        if flags & 0b1111_1110 != 0 {
            return Err(policy(
                "release tar.gz uses a non-deterministic compression block",
            ));
        }
        let final_block = flags & 1 == 1;
        let length = u16::from_le_bytes([bytes[offset + 1], bytes[offset + 2]]) as usize;
        let inverse = u16::from_le_bytes([bytes[offset + 3], bytes[offset + 4]]);
        if inverse != !(length as u16) {
            return Err(policy("release tar.gz stored block length is invalid"));
        }
        offset += 5;
        let end = offset
            .checked_add(length)
            .ok_or_else(|| policy("release tar.gz length overflow"))?;
        if end > bytes.len() - 8 {
            return Err(policy("release tar.gz stored block is truncated"));
        }
        output.extend(&bytes[offset..end]);
        offset = end;
        if final_block {
            break;
        }
    }
    let trailer = &bytes[bytes.len() - 8..];
    if u32::from_le_bytes(trailer[..4].try_into().unwrap_or([0; 4])) != crc32(&output)
        || u32::from_le_bytes(trailer[4..].try_into().unwrap_or([0; 4])) != output.len() as u32
    {
        return Err(policy("release tar.gz checksum does not match"));
    }
    Ok(output)
}

fn zip_archive(root: &str, entries: &[ArchiveEntry]) -> Result<Vec<u8>, GateError> {
    let mut output = Vec::new();
    let mut central = Vec::new();
    for entry in entries {
        let name = format!("{root}/{}", entry.name).into_bytes();
        let crc = crc32(&entry.bytes);
        let offset = output.len() as u32;
        output.extend(0x0403_4b50_u32.to_le_bytes());
        output.extend(20_u16.to_le_bytes());
        output.extend(0_u16.to_le_bytes());
        output.extend(0_u16.to_le_bytes());
        output.extend(0_u16.to_le_bytes());
        output.extend(0_u16.to_le_bytes());
        output.extend(crc.to_le_bytes());
        output.extend((entry.bytes.len() as u32).to_le_bytes());
        output.extend((entry.bytes.len() as u32).to_le_bytes());
        output.extend((name.len() as u16).to_le_bytes());
        output.extend(0_u16.to_le_bytes());
        output.extend(&name);
        output.extend(&entry.bytes);

        central.extend(0x0201_4b50_u32.to_le_bytes());
        central.extend(0x0314_u16.to_le_bytes());
        central.extend(20_u16.to_le_bytes());
        central.extend(0_u16.to_le_bytes());
        central.extend(0_u16.to_le_bytes());
        central.extend(0_u16.to_le_bytes());
        central.extend(0_u16.to_le_bytes());
        central.extend(crc.to_le_bytes());
        central.extend((entry.bytes.len() as u32).to_le_bytes());
        central.extend((entry.bytes.len() as u32).to_le_bytes());
        central.extend((name.len() as u16).to_le_bytes());
        central.extend(0_u16.to_le_bytes());
        central.extend(0_u16.to_le_bytes());
        central.extend(0_u16.to_le_bytes());
        central.extend(0_u16.to_le_bytes());
        central.extend(((0o100000 | entry.mode) << 16).to_le_bytes());
        central.extend(offset.to_le_bytes());
        central.extend(&name);
    }
    let central_offset = output.len() as u32;
    let central_size = central.len() as u32;
    output.extend(central);
    output.extend(0x0605_4b50_u32.to_le_bytes());
    output.extend(0_u16.to_le_bytes());
    output.extend(0_u16.to_le_bytes());
    output.extend((entries.len() as u16).to_le_bytes());
    output.extend((entries.len() as u16).to_le_bytes());
    output.extend(central_size.to_le_bytes());
    output.extend(central_offset.to_le_bytes());
    output.extend(0_u16.to_le_bytes());
    Ok(output)
}

fn read_zip(bytes: &[u8]) -> Result<BTreeMap<String, Vec<u8>>, GateError> {
    let mut offset = 0_usize;
    let mut entries = BTreeMap::new();
    while offset + 4 <= bytes.len() && bytes[offset..offset + 4] == 0x0403_4b50_u32.to_le_bytes() {
        if offset + 30 > bytes.len() {
            return Err(policy("zip local header is truncated"));
        }
        if le16(bytes, offset + 8)? != 0 {
            return Err(policy("zip entry uses an unsupported compression method"));
        }
        let crc = le32(bytes, offset + 14)?;
        let compressed = le32(bytes, offset + 18)? as usize;
        let uncompressed = le32(bytes, offset + 22)? as usize;
        let name_len = le16(bytes, offset + 26)? as usize;
        let extra_len = le16(bytes, offset + 28)? as usize;
        if compressed != uncompressed {
            return Err(policy("zip stored entry length does not match"));
        }
        let name_start = offset + 30;
        let data_start = name_start + name_len + extra_len;
        let data_end = data_start
            .checked_add(uncompressed)
            .ok_or_else(|| policy("zip entry length overflow"))?;
        if data_end > bytes.len() {
            return Err(policy("zip entry is truncated"));
        }
        let name = std::str::from_utf8(&bytes[name_start..name_start + name_len])
            .map_err(|_| policy("zip member name is not UTF-8"))?
            .to_owned();
        safe_member(&name)?;
        let data = bytes[data_start..data_end].to_vec();
        if crc32(&data) != crc || entries.insert(name, data).is_some() {
            return Err(policy("zip entry checksum failed or member is duplicated"));
        }
        offset = data_end;
    }
    if offset + 4 > bytes.len() || bytes[offset..offset + 4] != 0x0201_4b50_u32.to_le_bytes() {
        return Err(policy("zip central directory is missing"));
    }
    Ok(entries)
}

fn write_octal(field: &mut [u8], value: u64) {
    field.fill(b'0');
    let text = format!("{value:o}");
    let start = field.len() - 1 - text.len();
    field[start..start + text.len()].copy_from_slice(text.as_bytes());
    field[field.len() - 1] = 0;
}

fn write_checksum(field: &mut [u8], value: u64) {
    field.fill(b'0');
    let text = format!("{value:06o}");
    field[..6].copy_from_slice(text.as_bytes());
    field[6] = 0;
    field[7] = b' ';
}

fn parse_octal(field: &[u8]) -> Result<usize, GateError> {
    let text = std::str::from_utf8(field).map_err(|_| policy("tar numeric field is invalid"))?;
    usize::from_str_radix(text.trim_matches(['\0', ' ']), 8)
        .map_err(|_| policy("tar numeric field is invalid"))
}

fn nul_string(field: &[u8]) -> Result<String, GateError> {
    let end = field
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(field.len());
    std::str::from_utf8(&field[..end])
        .map(str::to_owned)
        .map_err(|_| policy("archive member name is not UTF-8"))
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = !0_u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xedb8_8320 & 0_u32.wrapping_sub(crc & 1));
        }
    }
    !crc
}

fn le16(bytes: &[u8], offset: usize) -> Result<u16, GateError> {
    let value = bytes
        .get(offset..offset + 2)
        .ok_or_else(|| policy("zip header is truncated"))?;
    Ok(u16::from_le_bytes([value[0], value[1]]))
}

fn le32(bytes: &[u8], offset: usize) -> Result<u32, GateError> {
    let value = bytes
        .get(offset..offset + 4)
        .ok_or_else(|| policy("zip header is truncated"))?;
    Ok(u32::from_le_bytes([value[0], value[1], value[2], value[3]]))
}

fn safe_member(name: &str) -> Result<(), GateError> {
    let path = Path::new(name);
    if path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return Err(policy("archive contains an unsafe member path"));
    }
    Ok(())
}

fn exact_object<'a>(
    value: &'a Value,
    keys: &[&str],
    label: &str,
) -> Result<&'a serde_json::Map<String, Value>, GateError> {
    let object = value
        .as_object()
        .ok_or_else(|| policy(format!("{label} must be an object")))?;
    let actual = object.keys().map(String::as_str).collect::<BTreeSet<_>>();
    let expected = keys.iter().copied().collect::<BTreeSet<_>>();
    if actual != expected {
        return Err(policy(format!(
            "{label} fields do not match the strict contract"
        )));
    }
    Ok(object)
}

fn expect_value(value: Option<&Value>, expected: &str, label: &str) -> Result<(), GateError> {
    if value.and_then(Value::as_str) != Some(expected) {
        return Err(policy(format!("{label} does not match")));
    }
    Ok(())
}

fn json_line(value: &Value) -> Result<Vec<u8>, GateError> {
    let mut bytes = serde_json::to_vec(value).map_err(|_| {
        GateError::internal("release.serialize", "release JSON serialization failed")
    })?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn read_regular(path: &Path, operation: &'static str) -> Result<Vec<u8>, GateError> {
    let metadata = fs::symlink_metadata(path).map_err(|error| GateError::io(operation, &error))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(policy("release input must be a regular non-symlink file"));
    }
    fs::read(path).map_err(|error| GateError::io(operation, &error))
}

fn admit_output_directory(path: &Path) -> Result<(), GateError> {
    fs::create_dir_all(path)
        .map_err(|error| GateError::io("create release output directory", &error))?;
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| GateError::io("inspect release output directory", &error))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(policy(
            "release output directory must be a non-symlink directory",
        ));
    }
    Ok(())
}

fn write_exact(path: &Path, bytes: &[u8]) -> Result<(), GateError> {
    if path.exists() {
        if read_regular(path, "read existing release output")? == bytes {
            return Ok(());
        }
        return Err(policy("release output conflicts with existing bytes"));
    }
    let parent = path
        .parent()
        .ok_or_else(|| policy("release output has no parent directory"))?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)
        .map_err(|error| GateError::io("create release temporary", &error))?;
    temporary
        .write_all(bytes)
        .map_err(|error| GateError::io("write release temporary", &error))?;
    temporary
        .as_file()
        .sync_all()
        .map_err(|error| GateError::io("sync release temporary", &error))?;
    temporary
        .persist_noclobber(path)
        .map_err(|error| GateError::io("install release output", &error.error))?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| GateError::io("sync release output directory", &error))?;
    Ok(())
}

fn expected_assets(inventory: &ReleaseInventory) -> BTreeSet<String> {
    let mut expected = BTreeSet::from(["SHA256SUMS".to_owned()]);
    for target in &inventory.targets {
        expected.insert(target.archive_name.clone());
        expected.insert(format!("{}.sha256", target.archive_name));
        expected.insert(format!("{}.intoto.jsonl", target.archive_name));
    }
    expected
}

fn regular_file_names(directory: &Path) -> Result<BTreeSet<String>, GateError> {
    let mut names = BTreeSet::new();
    let entries = fs::read_dir(directory)
        .map_err(|error| GateError::io("read release asset directory", &error))?;
    for entry in entries {
        let entry = entry.map_err(|error| GateError::io("read release asset entry", &error))?;
        let metadata = entry
            .file_type()
            .map_err(|error| GateError::io("inspect release asset", &error))?;
        if metadata.is_symlink() || !metadata.is_file() {
            return Err(policy(
                "release asset directory contains a non-regular entry",
            ));
        }
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| policy("release asset name is not UTF-8"))?;
        if !names.insert(name) {
            return Err(policy("release asset directory contains duplicate names"));
        }
    }
    Ok(names)
}

fn validate_sha40(value: &str, label: &str) -> Result<(), GateError> {
    if value.len() != 40
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(GateError::usage(format!(
            "{label} must be a lowercase 40-character commit SHA"
        )));
    }
    Ok(())
}

fn policy(detail: impl Into<String>) -> GateError {
    GateError::policy("release.invalid", detail)
}

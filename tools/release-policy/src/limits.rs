//! Fixed admission limits shared by parsers, evidence I/O, and child execution.

use std::ffi::OsStr;
use std::fs::File;
use std::io::Read;
use std::path::Path;

use crate::error::GateError;

/// Largest decision, authorization, or evidence record admitted by the gate.
pub const MAX_INPUT_BYTES: usize = 1024 * 1024;
/// Largest schema document admitted by the gate.
pub const MAX_SCHEMA_BYTES: usize = 2 * 1024 * 1024;
/// Maximum canonical value nesting depth.
pub const MAX_JSON_DEPTH: usize = 64;
/// Maximum canonical scalar/container node count.
pub const MAX_JSON_NODES: usize = 100_000;
/// Maximum number of decision records in one invocation.
pub const MAX_DECISION_RECORDS: usize = 16;
/// Maximum public diagnostic size.
pub const MAX_DIAGNOSTIC_BYTES: usize = 512;
/// Maximum explicit child argument count.
pub const MAX_CHILD_ARGUMENTS: usize = 256;
/// Maximum aggregate child argument bytes.
pub const MAX_CHILD_ARGUMENT_BYTES: usize = 64 * 1024;
/// Maximum explicit child environment entries.
pub const MAX_CHILD_ENVIRONMENT: usize = 64;
/// Maximum aggregate child environment bytes.
pub const MAX_CHILD_ENVIRONMENT_BYTES: usize = 64 * 1024;
/// Default maximum retained bytes in each child output lane.
pub const DEFAULT_CHILD_OUTPUT_BYTES: usize = 256 * 1024;
/// Hard ceiling for each child output lane.
pub const MAX_CHILD_OUTPUT_BYTES: usize = 1024 * 1024;

/// Read a regular file through an explicit byte ceiling.
pub fn read_bounded(
    path: &Path,
    operation: &'static str,
    maximum: usize,
) -> Result<Vec<u8>, GateError> {
    let file = File::open(path).map_err(|error| GateError::io(operation, &error))?;
    let metadata = file
        .metadata()
        .map_err(|error| GateError::io(operation, &error))?;
    if !metadata.is_file() {
        return Err(GateError::policy(
            "input.not_regular_file",
            format!("{operation} must reference a regular file"),
        ));
    }
    if metadata.len() > maximum as u64 {
        return Err(GateError::policy(
            "input.too_large",
            format!("{operation} exceeds the {maximum}-byte limit"),
        ));
    }

    let mut bytes = Vec::with_capacity((metadata.len() as usize).min(maximum));
    file.take(maximum as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| GateError::io(operation, &error))?;
    if bytes.len() > maximum {
        return Err(GateError::policy(
            "input.too_large",
            format!("{operation} exceeds the {maximum}-byte limit"),
        ));
    }
    Ok(bytes)
}

/// Count platform string bytes without lossy conversion.
#[must_use]
pub fn os_len(value: &OsStr) -> usize {
    value.as_encoded_bytes().len()
}

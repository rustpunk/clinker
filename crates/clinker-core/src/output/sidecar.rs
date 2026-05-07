//! Provenance sidecar JSON writer.
//!
//! When `OutputConfig.write_meta` is `true`, a JSON file at
//! `<resolved_path>.meta.json` is emitted after the main stream is
//! flushed. The sidecar answers "which pipeline produced this file?"
//! without requiring the user to embed a hash in the filename.

use std::collections::BTreeMap;
use std::fs::File;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::PipelineError;

/// Provenance metadata written alongside an output file.
///
/// Maps keyed by deterministic `BTreeMap` for stable JSON output (so
/// snapshot tests do not drift on iteration order). Optional fields use
/// `skip_serializing_if` so the sidecar stays compact when sections do
/// not apply (no channel, no DLQ, no upstream router).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OutputSidecar {
    pub pipeline_path: String,
    /// Full 64 hex chars of BLAKE3 over the post-env-var-interpolated
    /// source YAML.
    pub pipeline_hash: String,
    /// First 8 hex chars of `pipeline_hash`. Stored explicitly so
    /// downstream consumers do not need to know the truncation rule.
    pub pipeline_hash_short: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    pub clinker_version: String,
    pub run_started_at: String,
    pub run_finished_at: String,
    pub elapsed_total_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_id: Option<String>,
    pub output_name: String,
    pub resolved_path: String,
    pub record_count: u64,
    pub bytes_written: u64,
    /// DLQ record counts bucketed by classification reason. Empty when
    /// the output has no DLQ branch wired.
    #[serde(skip_serializing_if = "BTreeMap::is_empty", default)]
    pub dlq_counts: BTreeMap<String, u64>,
    /// Per-route record counts keyed by `<route_node_name>.<route_output_name>`
    /// so multiple route nodes upstream of one output do not collide.
    #[serde(skip_serializing_if = "BTreeMap::is_empty", default)]
    pub route_counts: BTreeMap<String, u64>,
    /// Wall-clock elapsed time per pipeline node in milliseconds.
    #[serde(skip_serializing_if = "BTreeMap::is_empty", default)]
    pub node_timings_ms: BTreeMap<String, u64>,
}

impl OutputSidecar {
    /// Filename for the sidecar JSON: `<output>.meta.json`.
    pub fn sidecar_path(output_path: &Path) -> PathBuf {
        let mut p = output_path.as_os_str().to_owned();
        p.push(".meta.json");
        PathBuf::from(p)
    }
}

/// Convert a 32-byte hash to lowercase hex.
pub fn hash_to_hex(bytes: &[u8; 32]) -> String {
    let mut s = String::with_capacity(64);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// Write the sidecar JSON next to the output file.
///
/// Pretty-prints for human readability since sidecars are typically
/// inspected by hand. Atomic write is not required — the sidecar is
/// auxiliary to the main file, not part of the data set.
pub fn write_sidecar(output_path: &Path, sidecar: &OutputSidecar) -> Result<(), PipelineError> {
    let path = OutputSidecar::sidecar_path(output_path);
    let f = File::create(&path).map_err(PipelineError::Io)?;
    serde_json::to_writer_pretty(f, sidecar).map_err(|e| {
        PipelineError::Io(std::io::Error::other(format!(
            "failed to serialize sidecar JSON for {}: {e}",
            path.display()
        )))
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn sample() -> OutputSidecar {
        OutputSidecar {
            pipeline_path: "pipelines/customer_etl.yaml".into(),
            pipeline_hash: "00".repeat(32),
            pipeline_hash_short: "00000000".into(),
            channel: Some("acme".into()),
            clinker_version: "0.1.0".into(),
            run_started_at: "2026-05-06T14:23:01Z".into(),
            run_finished_at: "2026-05-06T14:23:04Z".into(),
            elapsed_total_ms: 3214,
            execution_id: Some("exec-1".into()),
            batch_id: Some("batch-1".into()),
            output_name: "results".into(),
            resolved_path: "./output/customers-acme.csv".into(),
            record_count: 1234,
            bytes_written: 56789,
            dlq_counts: BTreeMap::from([("schema_mismatch".into(), 3u64)]),
            route_counts: BTreeMap::from([("router_main.high_value".into(), 80u64)]),
            node_timings_ms: BTreeMap::from([("source1".into(), 12u64)]),
        }
    }

    #[test]
    fn sidecar_path_appends_meta_json() {
        assert_eq!(
            OutputSidecar::sidecar_path(Path::new("/tmp/out.csv")),
            Path::new("/tmp/out.csv.meta.json")
        );
    }

    #[test]
    fn write_round_trip() {
        let dir = tempdir().unwrap();
        let out = dir.path().join("results.csv");
        std::fs::write(&out, "row\n").unwrap();
        let s = sample();
        write_sidecar(&out, &s).unwrap();
        let json = std::fs::read_to_string(out.with_extension("csv.meta.json")).unwrap();
        let parsed: OutputSidecar = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, s);
    }

    #[test]
    fn empty_maps_omitted_in_json() {
        let dir = tempdir().unwrap();
        let out = dir.path().join("results.csv");
        std::fs::write(&out, "").unwrap();
        let mut s = sample();
        s.dlq_counts.clear();
        s.route_counts.clear();
        s.node_timings_ms.clear();
        write_sidecar(&out, &s).unwrap();
        let json = std::fs::read_to_string(out.with_extension("csv.meta.json")).unwrap();
        assert!(!json.contains("dlq_counts"));
        assert!(!json.contains("route_counts"));
        assert!(!json.contains("node_timings_ms"));
    }

    #[test]
    fn hash_to_hex_pads_with_leading_zeros() {
        let mut bytes = [0u8; 32];
        bytes[0] = 0x0a;
        let hex = hash_to_hex(&bytes);
        assert_eq!(&hex[..2], "0a");
        assert_eq!(hex.len(), 64);
    }
}

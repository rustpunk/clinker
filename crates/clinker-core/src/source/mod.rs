//! Source-side discovery and multi-file ingestion.
//!
//! [`discovery`] resolves a [`crate::config::SourceConfig`] matcher
//! (`path` / `glob` / `regex` / `paths`) into a concrete file set,
//! applying the post-discovery filters (`exclude`, `modified_after/before`,
//! `min_size`/`max_size`, `files.sort_by`/`take_first|last`,
//! `files.recursive`, `files.on_no_match`).
//!
//! Multi-file format streaming (header skipping across files, per-file
//! `RecordProvenance` Arc swaps) lives alongside in §3.

pub mod discovery;
pub mod multi_file;

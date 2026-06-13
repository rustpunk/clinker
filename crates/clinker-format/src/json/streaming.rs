//! Streaming envelope pre-scan for JSON sources.
//!
//! Walks a JSON document exactly once through serde's `DeserializeSeed`
//! API, deserializing *only* the subtrees a source's declared envelope
//! sections point at and skipping every other key with `IgnoredAny` (a
//! zero-allocation parse-and-discard). This replaces a full
//! `serde_json::from_slice` of the whole document — which materialized the
//! entire tree just to resolve a handful of pointers — so the pre-scan's
//! retained memory scales with the declared sections rather than the
//! document size.
//!
//! Blocking, bounded: the pre-scan runs before any body record streams and
//! returns the matched section subtrees as a map. The caller charges each
//! retained subtree against the document index cap and coerces it to the
//! section's typed field schema.

use std::io::{BufReader, Read};

use indexmap::IndexMap;
use serde::Deserialize;
use serde::de::{DeserializeSeed, Deserializer, IgnoredAny, MapAccess, Visitor};

use crate::error::FormatError;

/// One declared section's RFC 6901 pointer, decoded to path segments, plus
/// its author-chosen name.
///
/// `segments` is empty only for the whole-document pointer `""`, which
/// matches the root object itself.
pub(crate) struct SectionTarget {
    /// Author-chosen section name the matched subtree is recorded under.
    pub name: String,
    /// RFC 6901 pointer segments, `~1`/`~0` already decoded to `/`/`~`.
    pub segments: Vec<String>,
}

impl SectionTarget {
    /// Decode an RFC 6901 JSON pointer into path segments.
    ///
    /// A leading `/` introduces each segment; `~1` decodes to `/` and `~0`
    /// to `~`, matching `serde_json::Value::pointer`. The empty pointer
    /// `""` yields no segments (it names the root).
    pub(crate) fn new(name: String, pointer: &str) -> Self {
        let segments = if pointer.is_empty() {
            Vec::new()
        } else {
            pointer
                .split('/')
                .skip(1)
                .map(|s| s.replace("~1", "/").replace("~0", "~"))
                .collect()
        };
        SectionTarget { name, segments }
    }
}

/// Walk a JSON document once, retaining only the subtrees named by
/// `targets`.
///
/// Each matched section's subtree is deserialized as a single
/// `serde_json::Value` and returned keyed by section name; every other key
/// is skipped via `IgnoredAny`. A target whose pointer does not resolve is
/// simply absent from the result — the caller treats a missing section the
/// same as the prior full-materialization path did.
///
/// # Errors
///
/// Returns [`FormatError::Json`] on any JSON parse failure (malformed
/// input, or a pointer descending through a non-object node).
pub(crate) fn extract_sections<R: Read>(
    reader: &mut BufReader<R>,
    targets: &[SectionTarget],
) -> Result<IndexMap<String, serde_json::Value>, FormatError> {
    let live: Vec<&SectionTarget> = targets.iter().collect();
    let mut matched: IndexMap<String, serde_json::Value> = IndexMap::new();
    let mut de = serde_json::Deserializer::from_reader(reader);
    NodeSeed {
        // The whole document is the depth-0 node; descent advances one
        // segment per object level.
        depth: 0,
        targets: &live,
        matched: &mut matched,
    }
    .deserialize(&mut de)
    .map_err(|e| FormatError::Json(format!("envelope pre-scan: {e}")))?;
    Ok(matched)
}

/// Seed driving the walk of one JSON node `depth` segments from the
/// document root, against the targets still live at this node.
///
/// A node is a matched section when some live target's pointer has exactly
/// `depth` segments — every earlier segment matched the keys descended so
/// far. A matched node is deserialized whole as a `serde_json::Value`;
/// otherwise the seed descends into the object, following only keys some
/// live target's next segment selects and skipping the rest with
/// `IgnoredAny`.
struct NodeSeed<'a, 't> {
    depth: usize,
    targets: &'t [&'t SectionTarget],
    matched: &'a mut IndexMap<String, serde_json::Value>,
}

impl<'de, 'a, 't> DeserializeSeed<'de> for NodeSeed<'a, 't> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        for target in self.targets {
            if target.segments.len() == self.depth {
                let value = serde_json::Value::deserialize(deserializer)?;
                self.matched.insert(target.name.clone(), value);
                return Ok(());
            }
        }
        deserializer.deserialize_map(ObjectDescent {
            depth: self.depth,
            targets: self.targets,
            matched: self.matched,
        })
    }
}

/// Visitor that descends into a JSON object, recursing into keys a live
/// target selects and skipping the rest without allocation.
struct ObjectDescent<'a, 't> {
    depth: usize,
    targets: &'t [&'t SectionTarget],
    matched: &'a mut IndexMap<String, serde_json::Value>,
}

impl<'de, 'a, 't> Visitor<'de> for ObjectDescent<'a, 't> {
    type Value = ();

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("a JSON object to descend for envelope sections")
    }

    fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<(), M::Error> {
        while let Some(key) = map.next_key::<String>()? {
            // Targets whose next segment is this key stay live one level
            // deeper; every other target ignores this key's value.
            let live: Vec<&SectionTarget> = self
                .targets
                .iter()
                .copied()
                .filter(|t| t.segments.get(self.depth).is_some_and(|seg| seg == &key))
                .collect();
            if live.is_empty() {
                map.next_value::<IgnoredAny>()?;
                continue;
            }
            map.next_value_seed(NodeSeed {
                depth: self.depth + 1,
                targets: &live,
                matched: self.matched,
            })?;
        }
        Ok(())
    }
}

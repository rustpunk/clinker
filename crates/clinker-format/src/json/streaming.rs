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
//! returns the matched section subtrees as a map. A declared section is
//! built through a byte-counting visitor that charges each constructed
//! string/key/element against a shared cap and aborts the parse the moment
//! the cumulative retained total crosses `max_index_bytes` — so even a
//! single oversized declared section fails loud at ~the cap, before the
//! whole subtree materializes, not after.

use std::io::{BufReader, Read};

use indexmap::IndexMap;
use serde::de::{DeserializeSeed, Deserializer, IgnoredAny, MapAccess, SeqAccess, Visitor};

use crate::error::FormatError;

/// One declared section's RFC 6901 pointer, decoded to path segments, plus
/// its author-chosen name.
///
/// `segments` is empty only for the whole-document pointer `""`, which
/// matches the root node itself.
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
    ///
    /// Callers must pre-validate the pointer grammar (empty or leading `/`):
    /// this constructor is infallible and a slashless non-empty pointer would
    /// decode to zero segments, silently aliasing the whole-document pointer.
    /// The reader rejects that case before building a target.
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

/// Walk a JSON document once, building only the subtrees named by `targets`
/// and charging their retained bytes against `max_index_bytes`.
///
/// Each matched section's subtree is constructed as a single
/// `serde_json::Value` through a byte-counting visitor; every other key is
/// skipped via `IgnoredAny`. The cap is charged *as the subtree is built*,
/// so an oversized declared section aborts the parse mid-build rather than
/// after the whole subtree materializes. A target whose pointer cannot be
/// reached through the streamed structure — a non-object/array intermediate,
/// a top-level scalar/array document, or an array-index segment that no
/// element matches — yields no section (a graceful miss), matching
/// `serde_json::Value::pointer` returning `None`.
///
/// # Errors
///
/// Returns [`FormatError::Json`] on a JSON parse failure, or naming the
/// offending section and the cap when a declared section's retained bytes
/// cross `max_index_bytes`.
pub(crate) fn extract_sections<R: Read>(
    reader: &mut BufReader<R>,
    targets: &[SectionTarget],
    max_index_bytes: Option<usize>,
) -> Result<IndexMap<String, serde_json::Value>, FormatError> {
    let live: Vec<&SectionTarget> = targets.iter().collect();
    let mut state = WalkState {
        matched: IndexMap::new(),
        retained_bytes: 0,
        max_index_bytes,
        cap_error: None,
    };
    let mut de = serde_json::Deserializer::from_reader(reader);
    let outcome = NodeSeed {
        // The whole document is the depth-0 node; descent advances one
        // segment per object/array level.
        depth: 0,
        targets: &live,
        state: &mut state,
    }
    .deserialize(&mut de);

    // A cap trip is signalled through `cap_error` (carried out-of-band so it
    // is not swallowed by serde's generic parse-error surface); surface it
    // ahead of any serde error the aborted parse also produced.
    if let Some(err) = state.cap_error {
        return Err(err);
    }
    outcome.map_err(|e| FormatError::Json(format!("envelope pre-scan: {e}")))?;
    Ok(state.matched)
}

/// Mutable walk state threaded through the recursion: the sections built so
/// far, the running retained-byte total, the cap, and an out-of-band slot
/// for a cap-exceeded error.
struct WalkState {
    matched: IndexMap<String, serde_json::Value>,
    retained_bytes: usize,
    max_index_bytes: Option<usize>,
    /// Set when a section build crosses the cap. Carried out-of-band of
    /// serde's `D::Error` so the precise cap message survives the aborted
    /// parse; checked by `extract_sections` before any serde error.
    cap_error: Option<FormatError>,
}

impl WalkState {
    /// Charge `bytes` against the cap; record a cap error and signal abort
    /// (`Err(())`) if the running total would cross it. The caller turns the
    /// `Err(())` into a serde `D::Error` to unwind the parse.
    fn charge(&mut self, bytes: usize, section: &str) -> Result<(), ()> {
        let projected = self.retained_bytes.saturating_add(bytes);
        if let Some(cap) = self.max_index_bytes
            && projected > cap
        {
            self.cap_error.get_or_insert_with(|| {
                FormatError::Json(format!(
                    "envelope document-index cap exceeded: building section {section:?} \
                     crossed the {cap}-byte `max_index_bytes` cap mid-parse. Raise \
                     `max_index_bytes` on the source, or narrow the `$doc.*` paths the \
                     pipeline reads (declare paths over envelope metadata, not bulk arrays)."
                ))
            });
            return Err(());
        }
        self.retained_bytes = projected;
        Ok(())
    }
}

/// Seed driving the walk of one JSON node `depth` segments from the document
/// root, against the targets still live at this node.
///
/// A node is a matched section when some live target's pointer has exactly
/// `depth` segments — every earlier segment matched the structure descended
/// so far. A matched node is built whole (byte-counted) as a
/// `serde_json::Value`; otherwise the seed descends into the node, following
/// only the keys/indices a live target's next segment selects and skipping
/// the rest. A node that is neither map nor sequence (a scalar leaf reached
/// before a target's pointer terminates) is a graceful miss for those
/// targets.
struct NodeSeed<'a, 't> {
    depth: usize,
    targets: &'t [&'t SectionTarget],
    state: &'a mut WalkState,
}

impl<'de, 'a, 't> DeserializeSeed<'de> for NodeSeed<'a, 't> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        // Collect every target whose pointer terminates at this node. Two or
        // more sections may alias one pointer (distinct names projecting
        // different `fields` from a single header), so each terminating name
        // must be recorded — not just the first.
        let terminating: Vec<&SectionTarget> = self
            .targets
            .iter()
            .copied()
            .filter(|t| t.segments.len() == self.depth)
            .collect();
        if !terminating.is_empty() {
            // Build the shared subtree once, charging its bytes a single time
            // (the cap message names the first-declared alias), then record it
            // under every terminating name in declaration order. The common
            // single-section case moves the built value; extra aliases clone.
            let value = CountingValueSeed {
                section: &terminating[0].name,
                state: self.state,
            }
            .deserialize(deserializer)?;
            let (last, init) = terminating.split_last().expect("non-empty checked above");
            for target in init {
                self.state
                    .matched
                    .insert(target.name.clone(), value.clone());
            }
            self.state.matched.insert(last.name.clone(), value);
            return Ok(());
        }
        // No target terminates here; descend. `deserialize_any` lets the
        // node be a map, a sequence, or a scalar — a scalar reached before a
        // target's pointer terminates is a graceful miss (no section, no
        // error), matching `serde_json::Value::pointer` -> None.
        deserializer.deserialize_any(StructureDescent {
            depth: self.depth,
            targets: self.targets,
            state: self.state,
        })
    }
}

/// Visitor that descends one structural level toward live targets, matching
/// object keys and array indices and skipping everything else, and treats a
/// scalar node as a graceful miss.
struct StructureDescent<'a, 't> {
    depth: usize,
    targets: &'t [&'t SectionTarget],
    state: &'a mut WalkState,
}

impl<'de, 'a, 't> Visitor<'de> for StructureDescent<'a, 't> {
    type Value = ();

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("any JSON value to descend for envelope sections")
    }

    fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<(), M::Error> {
        while let Some(key) = map.next_key::<String>()? {
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
                state: self.state,
            })?;
        }
        Ok(())
    }

    fn visit_seq<S: SeqAccess<'de>>(self, mut seq: S) -> Result<(), S::Error> {
        // RFC 6901 array-index segments: a numeric segment selects the Nth
        // element. Targets with a non-numeric next segment cannot match an
        // array element and miss.
        let mut idx = 0usize;
        loop {
            let live: Vec<&SectionTarget> = self
                .targets
                .iter()
                .copied()
                .filter(|t| {
                    t.segments
                        .get(self.depth)
                        .and_then(|seg| seg.parse::<usize>().ok())
                        .is_some_and(|n| n == idx)
                })
                .collect();
            let proceeded = if live.is_empty() {
                seq.next_element::<IgnoredAny>()?.is_some()
            } else {
                seq.next_element_seed(NodeSeed {
                    depth: self.depth + 1,
                    targets: &live,
                    state: self.state,
                })?
                .is_some()
            };
            if !proceeded {
                return Ok(());
            }
            idx += 1;
        }
    }

    // A scalar node reached before any live target's pointer terminates is a
    // graceful miss: the target's deeper segments cannot be satisfied, so it
    // records no section and raises no error. `serde` calls the matching
    // scalar visitor; each returns `Ok(())` without recording anything.
    fn visit_unit<E>(self) -> Result<(), E> {
        Ok(())
    }
    fn visit_bool<E>(self, _v: bool) -> Result<(), E> {
        Ok(())
    }
    fn visit_i64<E>(self, _v: i64) -> Result<(), E> {
        Ok(())
    }
    fn visit_u64<E>(self, _v: u64) -> Result<(), E> {
        Ok(())
    }
    fn visit_f64<E>(self, _v: f64) -> Result<(), E> {
        Ok(())
    }
    fn visit_str<E>(self, _v: &str) -> Result<(), E> {
        Ok(())
    }
}

/// Seed that builds a `serde_json::Value` while charging its retained bytes
/// incrementally against the cap, aborting mid-build the instant the
/// cumulative total crosses it.
struct CountingValueSeed<'a, 's> {
    section: &'s str,
    state: &'a mut WalkState,
}

impl<'de, 'a, 's> DeserializeSeed<'de> for CountingValueSeed<'a, 's> {
    type Value = serde_json::Value;

    fn deserialize<D: Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<serde_json::Value, D::Error> {
        deserializer.deserialize_any(CountingValueVisitor {
            section: self.section,
            state: self.state,
        })
    }
}

/// Visitor building a `serde_json::Value` node-by-node, charging each owned
/// string, key, and container element against the cap as it is constructed.
struct CountingValueVisitor<'a, 's> {
    section: &'s str,
    state: &'a mut WalkState,
}

impl<'a, 's> CountingValueVisitor<'a, 's> {
    /// Charge `bytes`, mapping a cap trip to a serde error so the parse
    /// unwinds. The precise cap message is stashed in `state.cap_error`.
    fn charge<E: serde::de::Error>(&mut self, bytes: usize) -> Result<(), E> {
        self.state
            .charge(bytes, self.section)
            .map_err(|()| E::custom("envelope document-index cap exceeded"))
    }
}

impl<'de, 'a, 's> Visitor<'de> for CountingValueVisitor<'a, 's> {
    type Value = serde_json::Value;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("a JSON value retained as an envelope section subtree")
    }

    fn visit_unit<E: serde::de::Error>(self) -> Result<serde_json::Value, E> {
        Ok(serde_json::Value::Null)
    }

    fn visit_bool<E: serde::de::Error>(self, v: bool) -> Result<serde_json::Value, E> {
        Ok(serde_json::Value::Bool(v))
    }

    fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<serde_json::Value, E> {
        Ok(serde_json::Value::Number(v.into()))
    }

    fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<serde_json::Value, E> {
        Ok(serde_json::Value::Number(v.into()))
    }

    fn visit_f64<E: serde::de::Error>(mut self, v: f64) -> Result<serde_json::Value, E> {
        match serde_json::Number::from_f64(v) {
            Some(n) => Ok(serde_json::Value::Number(n)),
            None => {
                // Non-finite floats have no JSON number; retain the string
                // form and charge its bytes.
                let s = v.to_string();
                self.charge(s.len())?;
                Ok(serde_json::Value::String(s))
            }
        }
    }

    fn visit_str<E: serde::de::Error>(mut self, v: &str) -> Result<serde_json::Value, E> {
        self.charge(v.len())?;
        Ok(serde_json::Value::String(v.to_owned()))
    }

    fn visit_string<E: serde::de::Error>(mut self, v: String) -> Result<serde_json::Value, E> {
        self.charge(v.len())?;
        Ok(serde_json::Value::String(v))
    }

    fn visit_seq<S: SeqAccess<'de>>(self, mut seq: S) -> Result<serde_json::Value, S::Error> {
        let mut out: Vec<serde_json::Value> = Vec::new();
        while let Some(elem) = seq.next_element_seed(CountingValueSeed {
            section: self.section,
            state: self.state,
        })? {
            out.push(elem);
        }
        Ok(serde_json::Value::Array(out))
    }

    fn visit_map<M: MapAccess<'de>>(mut self, mut map: M) -> Result<serde_json::Value, M::Error> {
        let mut out = serde_json::Map::new();
        while let Some(key) = map.next_key::<String>()? {
            // Charge the key bytes (the retained map owns the key string).
            self.charge(key.len())?;
            let value = map.next_value_seed(CountingValueSeed {
                section: self.section,
                state: self.state,
            })?;
            out.insert(key, value);
        }
        Ok(serde_json::Value::Object(out))
    }
}

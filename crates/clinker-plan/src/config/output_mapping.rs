//! An Output node's `mapping:` block: the ordered declaration of which
//! columns the file carries, in which order, under which names.
//!
//! ```yaml
//! mapping:
//!   - order_id                # emitted under its own name
//!   - sold_to: customer_id    # output column `sold_to` reads `customer_id`
//!   - contact_email: customer_email
//! ```
//!
//! Two item shapes, one concept. A bare scalar is a passthrough column; a
//! single-key pair renames. The pair reads **output name on the left, source
//! column on the right**, so the first token of an item names the output column
//! in both shapes. Identity-by-omission — a bare name means "unchanged" and no
//! placeholder token is spent on it — is the settled convention in query and
//! object-construction languages, and a sequence of bare scalars mixed with
//! single-key maps is the same shorthand the Source node's `split_values:` /
//! `join_values:` blocks and this crate's [`SortFieldSpec`](crate::config::SortFieldSpec)
//! already accept.
//!
//! Declaration order is the output column order. Columns the block does not
//! list are appended after the listed ones when `include_unmapped: true` (the
//! default) and dropped when it is `false`.
//!
//! Bounded memory: the block is per-schema config, sized by the author's column
//! count and never by input cardinality. [`OutputMapping`] resolves the
//! claimed-source lookup once, at parse time, so the per-record projection pass
//! adds no allocation and no per-column hashing beyond the one map probe the
//! rename already performed.

use std::collections::HashSet;

use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// One item of an Output node's `mapping:` sequence.
///
/// `output` is the column name the writer emits; `source` is the column read
/// from upstream. A bare-name item sets both to the same string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MappingEntry {
    /// Column name in the written output.
    pub output: String,
    /// Column name read from the record arriving at this Output.
    pub source: String,
}

impl MappingEntry {
    /// A passthrough entry — the column keeps its upstream name.
    pub fn passthrough(name: impl Into<String>) -> Self {
        let name = name.into();
        Self {
            output: name.clone(),
            source: name,
        }
    }

    /// A rename entry — `output` is written, `source` is read.
    pub fn rename(output: impl Into<String>, source: impl Into<String>) -> Self {
        Self {
            output: output.into(),
            source: source.into(),
        }
    }

    /// True when this entry carries the column through unrenamed.
    pub fn is_passthrough(&self) -> bool {
        self.output == self.source
    }
}

/// An Output node's ordered `mapping:` declaration.
///
/// Construct from an entry list with [`OutputMapping::new`]; every derived
/// index is computed once there and kept in step with `entries` because all
/// fields are private.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputMapping {
    entries: Vec<MappingEntry>,
    /// Every source column some entry reads. Lets the projection pass answer
    /// "has this column already been placed?" in O(1) per column with no
    /// per-record allocation, which is what keeps the unlisted-column append
    /// off the record-rate allocation path.
    claimed: HashSet<Box<str>>,
    /// Every name some entry writes. The unlisted-column append consults it so
    /// a passthrough column cannot land under a name the block already
    /// declared — two same-named columns on one schema resolve last-write-wins
    /// through `Record::get`, which would serve the passthrough's value under
    /// the renamed column's header.
    claimed_outputs: HashSet<Box<str>>,
    /// Per entry, whether it is the LAST one reading its source column. The
    /// projection pass moves the value out of the gathered field map for a last
    /// reader and clones only for the rare source feeding two output columns,
    /// so the common path copies no `Value` per record.
    ///
    /// Parallel to `entries`; indexed by entry position.
    last_reader: Vec<bool>,
    /// Capture slot for the superseded map form, read by the **E364** gate and
    /// by nothing else — the same device the retired `array_paths:` key uses.
    ///
    /// The map form is never honoured: a captured block declares no entries, so
    /// it renames nothing even if the gate were bypassed. It is parsed at all
    /// so the rejection can be a coded diagnostic with the node's source span
    /// and the author's own pairs echoed back in the sequence form, rather than
    /// a bare YAML type error.
    legacy_map: Vec<(String, String)>,
    /// Whether the block was written as a YAML map. Tracked separately from
    /// `legacy_map` being non-empty so an EMPTY map (`mapping: {}`) is still
    /// recognised as the superseded form rather than slipping through as a
    /// well-formed block that happens to declare nothing.
    map_form: bool,
}

impl OutputMapping {
    /// Build from an ordered entry list, deriving the claimed-source,
    /// claimed-output, and last-reader indexes.
    ///
    /// Accepts duplicate output names and an empty list: rejecting either is a
    /// plan-time diagnostic (**E364**) that carries the offending node's span,
    /// and a `Deserialize` impl has no span to attach.
    pub fn new(entries: Vec<MappingEntry>) -> Self {
        let claimed: HashSet<Box<str>> = entries
            .iter()
            .map(|e| Box::<str>::from(e.source.as_str()))
            .collect();
        let claimed_outputs: HashSet<Box<str>> = entries
            .iter()
            .map(|e| Box::<str>::from(e.output.as_str()))
            .collect();
        // Walk backwards so the FIRST time a source is seen from the end is its
        // last reader in declaration order.
        let mut seen: HashSet<&str> = HashSet::with_capacity(entries.len());
        let mut last_reader = vec![false; entries.len()];
        for (i, entry) in entries.iter().enumerate().rev() {
            last_reader[i] = seen.insert(entry.source.as_str());
        }
        Self {
            entries,
            claimed,
            claimed_outputs,
            last_reader,
            legacy_map: Vec::new(),
            map_form: false,
        }
    }

    /// The superseded map form's pairs, when the block was written as a map.
    /// Empty for every well-formed block, and also for an empty map — use
    /// [`OutputMapping::is_map_form`] to detect the shape itself.
    pub fn legacy_map_form(&self) -> &[(String, String)] {
        &self.legacy_map
    }

    /// Whether the block was written as a YAML map rather than a sequence.
    /// True for `mapping: {}` as well as for a populated map.
    pub fn is_map_form(&self) -> bool {
        self.map_form
    }

    /// The declared entries, in declaration order — which is output order.
    pub fn entries(&self) -> &[MappingEntry] {
        &self.entries
    }

    /// True when some entry reads `column` from upstream, so the projection
    /// pass has already placed it and must not append it a second time.
    pub fn claims_source(&self, column: &str) -> bool {
        self.claimed.contains(column)
    }

    /// True when some entry writes `column`, so an unlisted upstream column of
    /// that name must not be appended alongside it.
    pub fn claims_output(&self, column: &str) -> bool {
        self.claimed_outputs.contains(column)
    }

    /// Whether the entry at `index` is the last one reading its source column,
    /// and may therefore take the value rather than copy it.
    pub fn is_last_reader(&self, index: usize) -> bool {
        self.last_reader.get(index).copied().unwrap_or(true)
    }

    /// Output names that appear more than once, in first-duplicate order.
    /// Drives the **E364** plan-time gate; a writer cannot emit two columns
    /// under one header.
    pub fn duplicate_output_names(&self) -> Vec<&str> {
        let mut seen: HashSet<&str> = HashSet::with_capacity(self.entries.len());
        let mut dups: Vec<&str> = Vec::new();
        for entry in &self.entries {
            if !seen.insert(entry.output.as_str()) && !dups.contains(&entry.output.as_str()) {
                dups.push(entry.output.as_str());
            }
        }
        dups
    }
}

impl Serialize for OutputMapping {
    /// Round-trips to the authored shape: a bare scalar per passthrough entry,
    /// a single-key map per rename. `clinker config --resolved` prints this.
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.entries.len()))?;
        for entry in &self.entries {
            if entry.is_passthrough() {
                seq.serialize_element(&entry.output)?;
            } else {
                seq.serialize_element(&RenamePair(entry))?;
            }
        }
        seq.end()
    }
}

/// Serializes one rename entry as the single-key map `{output: source}`.
struct RenamePair<'a>(&'a MappingEntry);

impl Serialize for RenamePair<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry(&self.0.output, &self.0.source)?;
        map.end()
    }
}

/// The corrected form every `mapping:` diagnostic points at, so the message
/// carries something the author can paste.
const MAPPING_SHAPE_HELP: &str = "`mapping:` is a sequence, one item per output column. \
     Write a bare column name to carry a column through unchanged, or a single \
     `output_name: source_column` pair to rename it:\n  \
     mapping:\n    - order_id\n    - sold_to: customer_id";

impl<'de> Deserialize<'de> for OutputMapping {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct MappingVisitor;

        impl<'de> Visitor<'de> for MappingVisitor {
            type Value = OutputMapping;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str(
                    "a sequence of column names and single `output_name: source_column` pairs",
                )
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut entries = Vec::with_capacity(seq.size_hint().unwrap_or(0));
                while let Some(entry) = seq.next_element::<MappingEntry>()? {
                    entries.push(entry);
                }
                Ok(OutputMapping::new(entries))
            }

            /// Capture the superseded map form rather than failing here, so the
            /// **E364** gate can reject it with a source span and the author's
            /// own pairs rewritten into the sequence form. Nothing else reads
            /// the capture; a block that reaches this arm declares no entries.
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut legacy_map = Vec::new();
                while let Some(pair) = map.next_entry::<String, String>()? {
                    legacy_map.push(pair);
                }
                Ok(OutputMapping {
                    map_form: true,
                    legacy_map,
                    ..OutputMapping::new(Vec::new())
                })
            }
        }

        deserializer.deserialize_any(MappingVisitor)
    }
}

impl<'de> Deserialize<'de> for MappingEntry {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct EntryVisitor;

        impl<'de> Visitor<'de> for EntryVisitor {
            type Value = MappingEntry;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a column name, or a single `output_name: source_column` pair")
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                if v.is_empty() {
                    return Err(de::Error::custom(format!(
                        "a `mapping:` item may not be an empty column name. {MAPPING_SHAPE_HELP}"
                    )));
                }
                Ok(MappingEntry::passthrough(v))
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let Some((output, source)) = map.next_entry::<String, String>()? else {
                    return Err(de::Error::custom(format!(
                        "an empty `mapping:` item names no column. {MAPPING_SHAPE_HELP}"
                    )));
                };
                // A multi-key item is the map form leaking in one level down —
                // `- {a: x, b: y}` instead of two items. Name every key so the
                // author can see which ones to split out.
                let mut extra: Vec<String> = Vec::new();
                while let Some((k, _)) = map.next_entry::<String, de::IgnoredAny>()? {
                    extra.push(k);
                }
                if !extra.is_empty() {
                    let mut names = vec![output.clone()];
                    names.extend(extra);
                    let rewritten = names
                        .iter()
                        .map(|n| format!("    - {n}: <source_column>"))
                        .collect::<Vec<_>>()
                        .join("\n");
                    return Err(de::Error::custom(format!(
                        "a `mapping:` rename item carries exactly one \
                         `output_name: source_column` pair; this one carries {} ({}). \
                         Give each output column its own item:\n  mapping:\n{rewritten}",
                        names.len(),
                        names.join(", "),
                    )));
                }
                if output.is_empty() || source.is_empty() {
                    return Err(de::Error::custom(format!(
                        "a `mapping:` rename item needs a non-empty output name and \
                         source column; got `{output}: {source}`. {MAPPING_SHAPE_HELP}"
                    )));
                }
                Ok(MappingEntry::rename(output, source))
            }
        }

        deserializer.deserialize_any(EntryVisitor)
    }
}

/// Render a `'a', 'b'` list for a diagnostic.
fn quoted(names: &[&str]) -> String {
    names
        .iter()
        .map(|n| format!("'{n}'"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Every Output whose `mapping:` block is malformed or contradicts itself
/// (**E364**).
///
/// Four faults, all decidable from the Output node alone — no upstream schema
/// needed, which is why they live here rather than in the bind walk:
///
/// * the superseded map form. `mapping:` was a YAML map of column name to
///   column name; it is a sequence now. The rejection rewrites the author's own
///   pairs into the sequence form so the fix is a paste.
/// * a block that declares no columns — an empty sequence or an empty map.
///   Under `include_unmapped: false` that is a file with no columns at all, so
///   it is malformed rather than a valid request for an empty file.
/// * a repeated output name. A YAML map gave key uniqueness for free; a
///   sequence has to enforce it, and a writer cannot emit two columns under one
///   header.
/// * a column this same output's `exclude:` removes, named on either side of an
///   entry. `exclude:` runs against the incoming column names before `mapping:`
///   reads them, so the entry could only ever produce nothing (source side) or
///   the exclusion could never take effect (output side).
///
/// Takes a node LIST rather than the pipeline, for the same reason the
/// multi-value gates do: a composition body's nodes need the identical check
/// and never appear in the call-site pipeline's `nodes:`.
pub fn output_mapping_faults(
    nodes: &[crate::yaml::Spanned<crate::config::pipeline_node::PipelineNode>],
) -> Vec<crate::config::multi_value::NodeFault> {
    use crate::config::multi_value::NodeFault;
    use crate::config::pipeline_node::PipelineNode;

    let mut faults = Vec::new();
    for (node_index, spanned) in nodes.iter().enumerate() {
        let PipelineNode::Output {
            header,
            config: body,
        } = &spanned.value
        else {
            continue;
        };
        let output = &body.output;
        let Some(mapping) = output.mapping.as_ref() else {
            continue;
        };
        let out_name = header.name.as_str();

        // Keyed off the SHAPE, not off the capture having content, so an empty
        // map (`mapping: {}`) is recognised as the superseded form too instead
        // of reading as a well-formed block that happens to declare nothing.
        if mapping.is_map_form() {
            let legacy = mapping.legacy_map_form();
            // The pairs are SWAPPED into the new direction. The old executor
            // looked entries up by the incoming field name, so the map's key
            // was the SOURCE column — every block that actually renamed
            // anything was source-on-left, and lifting it verbatim would invert
            // it. The help names the one block this is wrong for: one written
            // to the old documentation, which renamed nothing at all.
            let rewritten = legacy
                .iter()
                .map(|(k, v)| {
                    if k == v {
                        format!("    - {k}")
                    } else {
                        format!("    - {v}: {k}")
                    }
                })
                .collect::<Vec<_>>()
                .join("\n");
            let help = if legacy.is_empty() {
                "each item is a bare column name (carried through under its own name) or a \
                 single `output_name: source_column` pair — the OUTPUT name is on the left, \
                 and declaration order is the output column order. This block also names no \
                 column at all; list the columns the file should carry:\n  mapping:\n    \
                 - order_id\n    - sold_to: customer_id"
                    .to_string()
            } else {
                format!(
                    "each item is a bare column name (carried through under its own name) or a \
                     single `output_name: source_column` pair — the OUTPUT name is on the left, \
                     and declaration order is the output column order. Rewrite the block as:\n  \
                     mapping:\n{rewritten}\n\nThe two sides of each pair are SWAPPED above, \
                     deliberately: the engine read the map's key as the SOURCE column, so this \
                     preserves what the pipeline actually wrote. If instead the block was \
                     written to follow the old documentation — which described the opposite \
                     direction and therefore renamed nothing — swap them back."
                )
            };
            faults.push(NodeFault {
                node_index,
                code: "E364",
                message: format!(
                    "output '{out_name}': `mapping:` is a sequence of output columns, not a map \
                     of column name to column name"
                ),
                help,
            });
            // Every other check reads `entries`, which a captured map form
            // leaves empty; there is nothing further to say about this block.
            continue;
        }

        if mapping.entries().is_empty() {
            faults.push(NodeFault {
                node_index,
                code: "E364",
                message: format!(
                    "output '{out_name}': `mapping:` declares no columns, so it states that the \
                     file carries none"
                ),
                help: "list the columns the file should carry, one item each — a bare column \
                       name to carry it through, or an `output_name: source_column` pair to \
                       rename it. To write every upstream column, remove the `mapping:` block \
                       entirely rather than leaving it empty."
                    .to_string(),
            });
            continue;
        }

        let dups = mapping.duplicate_output_names();
        if !dups.is_empty() {
            faults.push(NodeFault {
                node_index,
                code: "E364",
                message: format!(
                    "output '{out_name}': `mapping:` declares the output column(s) {listed} \
                     more than once; a written file cannot carry two columns under one name",
                    listed = quoted(&dups),
                ),
                help: format!(
                    "keep one item per output column and delete the rest. To write one upstream \
                     column into two output columns, give each its own name — \
                     `- {first}: <source_column>` and `- {first}_copy: <source_column>`",
                    first = dups[0],
                ),
            });
        }

        if let Some(exclude) = output.exclude.as_ref() {
            let excluded = |n: &str| exclude.iter().any(|x| x == n);
            let read_clashes: Vec<&str> = mapping
                .entries()
                .iter()
                .map(|e| e.source.as_str())
                .filter(|s| excluded(s))
                .collect();
            if !read_clashes.is_empty() {
                faults.push(NodeFault {
                    node_index,
                    code: "E364",
                    message: format!(
                        "output '{out_name}': `mapping:` reads the column(s) {listed}, which this \
                         output's own `exclude:` removes first — the entries can never produce a \
                         column",
                        listed = quoted(&read_clashes),
                    ),
                    help: format!(
                        "drop {listed} from `exclude:` if the mapping should write it, or drop \
                         the mapping item if the column should not be written. `exclude:` names \
                         incoming columns and runs before `mapping:` reads them",
                        listed = quoted(&read_clashes),
                    ),
                });
            }
            // Only the SOURCE side is a fault. `exclude:` operates on incoming
            // column names, full stop, and naming one that the mapping also
            // produces as an output name is not a mistake — it is the documented
            // resolution for a collision between a mapped column and a
            // passthrough of the same name, which the collision diagnostic's own
            // help tells the author to write.
        }
    }
    faults
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(yaml: &str) -> Result<OutputMapping, String> {
        crate::yaml::from_str::<OutputMapping>(yaml).map_err(|e| e.to_string())
    }

    #[test]
    fn bare_names_are_passthrough_entries() {
        let m = parse("- order_id\n- order_date\n").expect("parse");
        assert_eq!(
            m.entries(),
            &[
                MappingEntry::passthrough("order_id"),
                MappingEntry::passthrough("order_date"),
            ]
        );
        assert!(m.claims_source("order_id"));
        assert!(!m.claims_source("sku"));
    }

    #[test]
    fn pair_reads_output_name_on_the_left() {
        let m = parse("- sold_to: customer_id\n").expect("parse");
        assert_eq!(
            m.entries(),
            &[MappingEntry::rename("sold_to", "customer_id")]
        );
        // The direction contract, asserted rather than implied: the source
        // column is what the projection reads, the output name is what it writes.
        assert!(m.claims_source("customer_id"));
        assert!(!m.claims_source("sold_to"));
    }

    #[test]
    fn bare_and_pair_items_mix_in_one_sequence() {
        let m = parse("- order_id\n- sold_to: customer_id\n- sku\n").expect("parse");
        assert_eq!(m.entries().len(), 3);
        assert!(m.entries()[0].is_passthrough());
        assert!(!m.entries()[1].is_passthrough());
        assert!(m.entries()[2].is_passthrough());
    }

    /// The superseded map form parses into the capture slot and declares no
    /// entries, so it renames nothing even if the E364 gate were bypassed. The
    /// gate's message is asserted where the gate lives.
    #[test]
    fn map_form_is_captured_not_honoured() {
        let m = parse("sold_to: customer_id\nchannel: channel\n").expect("captured");
        assert!(m.entries().is_empty(), "a captured map declares no entries");
        assert_eq!(
            m.legacy_map_form(),
            &[
                ("sold_to".to_string(), "customer_id".to_string()),
                ("channel".to_string(), "channel".to_string()),
            ]
        );
    }

    #[test]
    fn multi_key_item_is_rejected_and_names_every_key() {
        let err = parse("- { a: x, b: y }\n").expect_err("multi-key item must fail");
        assert!(err.contains("exactly one"), "{err}");
        assert!(err.contains('a') && err.contains('b'), "{err}");
    }

    #[test]
    fn duplicate_output_names_are_reported() {
        let m = parse("- sku\n- sku: legacy_sku\n- qty\n").expect("parse");
        assert_eq!(m.duplicate_output_names(), vec!["sku"]);
    }

    #[test]
    fn one_source_column_may_feed_two_output_columns() {
        let m = parse("- sku\n- item_code: sku\n").expect("parse");
        assert!(m.duplicate_output_names().is_empty());
        assert_eq!(m.entries().len(), 2);
    }

    #[test]
    fn round_trips_through_yaml() {
        let original = parse("- order_id\n- sold_to: customer_id\n").expect("parse");
        let yaml = crate::yaml::to_string(&original).expect("serialize");
        let reparsed = parse(&yaml).unwrap_or_else(|e| panic!("reparse {yaml:?}: {e}"));
        assert_eq!(original, reparsed);
        assert!(
            !yaml.contains("order_id: order_id"),
            "a passthrough entry must serialize back to a bare name, got {yaml:?}"
        );
    }

    #[test]
    fn empty_item_is_rejected() {
        assert!(parse("- \"\"\n").is_err());
        assert!(parse("- {}\n").is_err());
    }

    /// The E364 gate's three faults, each with the corrected form the author
    /// can paste. Asserted against the message text because the message *is*
    /// the surface here.
    mod gate {
        use crate::config::pipeline::PipelineConfig;

        fn faults(nodes_yaml: &str) -> Vec<(String, String)> {
            let yaml = format!("pipeline:\n  name: t\nnodes:\n{nodes_yaml}");
            let config: PipelineConfig = crate::yaml::from_str(&yaml)
                .unwrap_or_else(|e| panic!("fixture must parse: {e}\n{yaml}"));
            super::super::output_mapping_faults(&config.nodes)
                .into_iter()
                .map(|f| (f.message, f.help))
                .collect()
        }

        /// `mapping_value` is spliced in directly after `mapping:`, so a caller
        /// can write an inline `{}` / `[]` as well as an indented block.
        fn output_with(mapping_value: &str, extra: &str) -> String {
            format!(
                "  - type: source\n    name: src\n    config:\n      name: src\n      \
                 type: csv\n      path: in.csv\n      schema:\n        - {{ name: sku, \
                 type: string }}\n        - {{ name: qty, type: int }}\n  - type: output\n    \
                 name: out\n    input: src\n    config:\n      name: out\n      type: csv\n      \
                 path: out.csv\n{extra}      mapping:{mapping_value}"
            )
        }

        /// An indented block value, the ordinary shape.
        fn block(lines: &str) -> String {
            format!("\n{lines}")
        }

        #[test]
        fn map_form_is_rejected_with_the_sequence_form_spelled_out() {
            let f = faults(&output_with(
                &block("        sku: sku\n        item: qty\n"),
                "",
            ));
            assert_eq!(f.len(), 1, "{f:?}");
            assert!(f[0].0.contains("not a map"), "{}", f[0].0);
            // `item: qty` meant "rename the source column `item` to `qty`" — the
            // engine looked entries up by the incoming field name. The sequence
            // spelling of that is `- qty: item`; emitting `- item: qty` would
            // hand back a block that inverts the rename.
            assert!(
                f[0].1.contains("- sku\n") && f[0].1.contains("- qty: item"),
                "help must rewrite the pairs into the new direction, identity entries \
                 collapsed to a bare name: {}",
                f[0].1
            );
            assert!(
                f[0].1.contains("OUTPUT name is on the left"),
                "help must state the pair direction: {}",
                f[0].1
            );
        }

        /// An empty map is still the superseded shape — the gate keys off the
        /// form, not off the capture having content, so it cannot slip through
        /// as a well-formed block that happens to declare nothing.
        #[test]
        fn an_empty_map_is_rejected_as_the_map_form() {
            let f = faults(&output_with(" {}\n", ""));
            assert_eq!(f.len(), 1, "{f:?}");
            assert!(f[0].0.contains("not a map"), "{}", f[0].0);
            assert!(
                f[0].1.contains("names no column"),
                "help must also say the block declares nothing: {}",
                f[0].1
            );
        }

        /// An empty sequence declares no columns, which under
        /// `include_unmapped: false` is a file with no columns at all.
        #[test]
        fn an_empty_sequence_is_rejected() {
            let f = faults(&output_with(" []\n", ""));
            assert_eq!(f.len(), 1, "{f:?}");
            assert!(f[0].0.contains("declares no columns"), "{}", f[0].0);
        }

        #[test]
        fn duplicate_output_name_is_rejected() {
            let f = faults(&output_with(
                &block("        - sku\n        - sku: qty\n"),
                "",
            ));
            assert_eq!(f.len(), 1, "{f:?}");
            assert!(f[0].0.contains("'sku'"), "{}", f[0].0);
            assert!(f[0].0.contains("more than once"), "{}", f[0].0);
        }

        #[test]
        fn a_mapping_item_excluded_by_the_same_output_is_rejected() {
            let f = faults(&output_with(
                &block("        - sku\n        - amount: qty\n"),
                "      exclude: [qty]\n",
            ));
            assert_eq!(f.len(), 1, "{f:?}");
            assert!(f[0].0.contains("'qty'"), "{}", f[0].0);
            assert!(f[0].0.contains("exclude"), "{}", f[0].0);
        }

        /// `exclude:` naming a column the mapping also PRODUCES is not a fault.
        /// `exclude:` operates on incoming names, so it removes the upstream
        /// column of that name and leaves the mapped one — which is exactly the
        /// resolution the collision diagnostic tells the author to write. A gate
        /// here would reject the fix another diagnostic hands out.
        #[test]
        fn excluding_a_mapping_output_name_is_not_a_fault() {
            let f = faults(&output_with(
                &block("        - sku\n        - amount: qty\n"),
                "      exclude: [amount]\n",
            ));
            assert!(f.is_empty(), "{f:?}");
        }

        /// A passthrough entry names one column on both sides, so excluding it
        /// is the source-side clash and must be reported once, not twice.
        #[test]
        fn excluding_a_passthrough_entry_reports_the_source_side_only() {
            let f = faults(&output_with(
                &block("        - sku\n        - amount: qty\n"),
                "      exclude: [sku]\n",
            ));
            assert_eq!(f.len(), 1, "{f:?}");
            assert!(f[0].0.contains("removes first"), "{}", f[0].0);
        }

        #[test]
        fn a_well_formed_sequence_produces_no_fault() {
            let f = faults(&output_with(
                &block("        - sku\n        - amount: qty\n"),
                "",
            ));
            assert!(f.is_empty(), "{f:?}");
        }
    }
}

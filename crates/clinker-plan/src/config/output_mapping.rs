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
/// Construct from an entry list with [`OutputMapping::new`]; the claimed-source
/// index is derived once there and kept in step with `entries` because both
/// fields are private.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputMapping {
    entries: Vec<MappingEntry>,
    /// Every source column some entry reads. Lets the projection pass answer
    /// "has this column already been placed?" in O(1) per column with no
    /// per-record allocation, which is what keeps the unlisted-column append
    /// off the record-rate allocation path.
    claimed: HashSet<Box<str>>,
    /// Capture slot for the superseded map form, read by the **E364** gate and
    /// by nothing else — the same device the retired `array_paths:` key uses.
    ///
    /// The map form is never honoured: a captured block declares no entries, so
    /// it renames nothing even if the gate were bypassed. It is parsed at all
    /// so the rejection can be a coded diagnostic with the node's source span
    /// and the author's own pairs echoed back in the sequence form, rather than
    /// a bare YAML type error.
    legacy_map: Vec<(String, String)>,
}

impl OutputMapping {
    /// Build from an ordered entry list, deriving the claimed-source index.
    ///
    /// Accepts duplicate output names: rejecting them is a plan-time
    /// diagnostic (**E364**) that carries the offending node's span, and a
    /// `Deserialize` impl has no span to attach.
    pub fn new(entries: Vec<MappingEntry>) -> Self {
        let claimed = entries
            .iter()
            .map(|e| Box::<str>::from(e.source.as_str()))
            .collect();
        Self {
            entries,
            claimed,
            legacy_map: Vec::new(),
        }
    }

    /// The superseded map form's pairs, when the block was written as a map.
    /// Empty for every well-formed block. Drives the **E364** migration
    /// diagnostic.
    pub fn legacy_map_form(&self) -> &[(String, String)] {
        &self.legacy_map
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
                    entries: Vec::new(),
                    claimed: HashSet::new(),
                    legacy_map,
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

/// Every Output whose `mapping:` block is malformed or contradicts itself
/// (**E364**).
///
/// Three faults, all decidable from the Output node alone — no upstream schema
/// needed, which is why they live here rather than in the bind walk:
///
/// * the superseded map form. `mapping:` was a YAML map of column name to
///   column name; it is a sequence now. The rejection echoes the author's own
///   pairs back in the sequence form so the fix is a paste.
/// * a repeated output name. A YAML map gave key uniqueness for free; a
///   sequence has to enforce it, and a writer cannot emit two columns under one
///   header.
/// * a listed column this same output's `exclude:` removes. `exclude:` runs
///   against the incoming column names before `mapping:` reads them, so the
///   entry could only ever produce nothing.
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

        let legacy = mapping.legacy_map_form();
        if !legacy.is_empty() {
            // Echo the author's own pairs, unswapped. Under the contract the
            // key is already the output name, so a block written against the
            // documented direction lifts straight into the sequence; a block
            // written against the old executor behaviour needs the sides
            // swapped, which is why the help states the direction outright.
            let rewritten = legacy
                .iter()
                .map(|(k, v)| {
                    if k == v {
                        format!("    - {k}")
                    } else {
                        format!("    - {k}: {v}")
                    }
                })
                .collect::<Vec<_>>()
                .join("\n");
            faults.push(NodeFault {
                node_index,
                code: "E364",
                message: format!(
                    "output '{out_name}': `mapping:` is a sequence of output columns, not a map \
                     of column name to column name"
                ),
                help: format!(
                    "each item is a bare column name (carried through under its own name) or a \
                     single `output_name: source_column` pair — the OUTPUT name is on the left, \
                     and declaration order is the output column order. Rewrite the block as:\n  \
                     mapping:\n{rewritten}"
                ),
            });
            // Every other check reads `entries`, which a captured map form
            // leaves empty; there is nothing further to say about this block.
            continue;
        }

        let dups = mapping.duplicate_output_names();
        if !dups.is_empty() {
            let listed = dups
                .iter()
                .map(|d| format!("'{d}'"))
                .collect::<Vec<_>>()
                .join(", ");
            faults.push(NodeFault {
                node_index,
                code: "E364",
                message: format!(
                    "output '{out_name}': `mapping:` declares the output column(s) {listed} \
                     more than once; a written file cannot carry two columns under one name"
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
            let clashes: Vec<&str> = mapping
                .entries()
                .iter()
                .filter(|e| exclude.iter().any(|x| x == &e.source))
                .map(|e| e.source.as_str())
                .collect();
            if !clashes.is_empty() {
                let listed = clashes
                    .iter()
                    .map(|c| format!("'{c}'"))
                    .collect::<Vec<_>>()
                    .join(", ");
                faults.push(NodeFault {
                    node_index,
                    code: "E364",
                    message: format!(
                        "output '{out_name}': `mapping:` reads the column(s) {listed}, which this \
                         output's own `exclude:` removes first — the entries can never produce a \
                         column"
                    ),
                    help: format!(
                        "drop {listed} from `exclude:` if the mapping should write it, or drop \
                         the mapping item if the column should not be written. `exclude:` names \
                         incoming columns and runs before `mapping:` reads them"
                    ),
                });
            }
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

        fn output_with(mapping_block: &str, extra: &str) -> String {
            format!(
                "  - type: source\n    name: src\n    config:\n      name: src\n      \
                 type: csv\n      path: in.csv\n      schema:\n        - {{ name: sku, \
                 type: string }}\n        - {{ name: qty, type: int }}\n  - type: output\n    \
                 name: out\n    input: src\n    config:\n      name: out\n      type: csv\n      \
                 path: out.csv\n{extra}      mapping:\n{mapping_block}"
            )
        }

        #[test]
        fn map_form_is_rejected_with_the_sequence_form_spelled_out() {
            let f = faults(&output_with("        sku: sku\n        item: qty\n", ""));
            assert_eq!(f.len(), 1, "{f:?}");
            assert!(f[0].0.contains("not a map"), "{}", f[0].0);
            assert!(
                f[0].1.contains("- sku\n") && f[0].1.contains("- item: qty"),
                "help must echo the author's pairs in the sequence form, identity entries \
                 collapsed to a bare name: {}",
                f[0].1
            );
            assert!(
                f[0].1.contains("OUTPUT name is on the left"),
                "help must state the pair direction: {}",
                f[0].1
            );
        }

        #[test]
        fn duplicate_output_name_is_rejected() {
            let f = faults(&output_with("        - sku\n        - sku: qty\n", ""));
            assert_eq!(f.len(), 1, "{f:?}");
            assert!(f[0].0.contains("'sku'"), "{}", f[0].0);
            assert!(f[0].0.contains("more than once"), "{}", f[0].0);
        }

        #[test]
        fn a_mapping_item_excluded_by_the_same_output_is_rejected() {
            let f = faults(&output_with(
                "        - sku\n        - amount: qty\n",
                "      exclude: [qty]\n",
            ));
            assert_eq!(f.len(), 1, "{f:?}");
            assert!(f[0].0.contains("'qty'"), "{}", f[0].0);
            assert!(f[0].0.contains("exclude"), "{}", f[0].0);
        }

        #[test]
        fn a_well_formed_sequence_produces_no_fault() {
            let f = faults(&output_with("        - sku\n        - amount: qty\n", ""));
            assert!(f.is_empty(), "{f:?}");
        }
    }
}

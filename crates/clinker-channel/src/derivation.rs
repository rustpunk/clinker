//! Group derivation and priority ordering.
//!
//! A group's membership is *derived* from a channel's identity `labels` via its
//! selector, never enumerated in a members list. [`derive_groups`] evaluates
//! every group's [`match:`](crate::group::Group::selector) selector against a
//! channel's labels, collects the matches, and assigns each selected group its
//! place in the overlay stack — the [`Group`](LayerKind::Group) rung, ordered
//! by `priority` (higher wins) then declaration order.
//!
//! # Where groups sit in the stack
//!
//! Groups occupy the rungs *between* the pipeline default and the channel
//! layers in the fixed semantic order
//! `PipelineDefault < Group(s) by priority < ChannelWide < ChannelPerTarget`.
//! Each selected group becomes a distinct [`LayerKind::Group { priority, seq }`]
//! layer; [`apply_group_config`] clobbers its `config:` onto the plan's
//! provenance through the *same* [`apply_config_clobber`](crate::overlay) engine
//! the channel layers use — no bespoke resolution logic per layer.
//!
//! # Channel-agnostic: standalone application
//!
//! Group overrides never read channel labels (only the *selector* does, and
//! only during derivation). A group therefore runs standalone: invoked by name
//! (`--group`) with no channel present, its selector is not evaluated at all —
//! the caller applies its layer directly with [`apply_group_config`] at a
//! [`group_layer`] built from the group's own `priority`.
//!
//! # Transparent, never a silent non-match
//!
//! [`derive_groups`] records an outcome for *every* group — selected,
//! unmatched, explicit-only (no selector), or a selector error — so
//! `channels resolve` / `--explain` can report which groups matched and why. A
//! selector that references an unknown label (a typo) or is ill-typed surfaces
//! as [`SelectionOutcome::Error`], never as a quiet `false`.

use indexmap::IndexMap;
use serde_json::Value as JsonValue;

use clinker_core_types::Diagnostic;
use clinker_plan::config::composition::{LayerKind, ProvenanceDb};

use crate::dotted::DottedPath;
use crate::error::ChannelError;
use crate::group::Group;
use crate::selector::{LabelSelector, SelectorError};

/// Saturating conversion of a group's declaration index into the layer `seq`.
///
/// The group scan budget bounds group counts far below `u32::MAX`, so this
/// never saturates in practice; the fallback only guards against a pathological
/// input rather than silently truncating.
fn seq_of(index: usize) -> u32 {
    u32::try_from(index).unwrap_or(u32::MAX)
}

/// Saturating conversion of a group's authored `priority` (`i64`) into the
/// layer's `i32` priority.
///
/// [`Group::priority`] is authored as `i64`; [`LayerKind::Group`] carries `i32`.
/// A priority beyond `i32`'s range is absurd, but must not panic — it saturates
/// toward the matching extreme so ordering (higher wins) is preserved.
fn saturate_priority(priority: i64) -> i32 {
    i32::try_from(priority).unwrap_or(if priority > 0 { i32::MAX } else { i32::MIN })
}

/// The overlay layer a group occupies at declaration-order `seq`.
///
/// Shared by the derived path ([`derive_groups`]) and the standalone `--group`
/// path so a group lands on the *same* layer identity either way.
pub fn group_layer(group: &Group, seq: u32) -> LayerKind {
    LayerKind::Group {
        priority: saturate_priority(group.priority),
        seq,
    }
}

/// Why a group was or was not selected for a channel.
#[derive(Debug)]
pub enum SelectionOutcome {
    /// The group's selector matched the channel's labels; it is applied at
    /// `layer` (a [`LayerKind::Group`]).
    Selected {
        /// The overlay layer this group occupies.
        layer: LayerKind,
    },
    /// The group has a selector that evaluated to `false` for these labels.
    Unmatched,
    /// The group has no `match:` selector — explicit-only, never auto-selected
    /// by label derivation (it applies only when invoked by name via `--group`).
    ExplicitOnly,
    /// The group's selector failed to compile or evaluate (e.g. it references an
    /// unknown label, or is ill-typed). Surfaced here rather than treated as a
    /// silent non-match.
    Error(SelectorError),
}

/// One group's derivation record: the group, its declaration-order `seq`, and
/// the outcome (with the reason).
#[derive(Debug)]
pub struct GroupSelection<'a> {
    /// The group this record describes.
    pub group: &'a Group,
    /// Declaration-order source id — the group's index in the input slice.
    /// Breaks `priority` ties, later declaration winning.
    pub seq: u32,
    /// Whether (and why) the group was selected.
    pub outcome: SelectionOutcome,
}

impl GroupSelection<'_> {
    /// The overlay layer this group occupies, if it was selected.
    pub fn layer(&self) -> Option<LayerKind> {
        match &self.outcome {
            SelectionOutcome::Selected { layer } => Some(*layer),
            _ => None,
        }
    }

    /// Whether this group was selected for the channel.
    pub fn is_selected(&self) -> bool {
        matches!(self.outcome, SelectionOutcome::Selected { .. })
    }

    /// A one-line, human-readable reason — the "why" for `channels resolve` /
    /// `--explain` reporting.
    pub fn describe(&self) -> String {
        match &self.outcome {
            SelectionOutcome::Selected { .. } => {
                format!(
                    "selected (priority {})",
                    saturate_priority(self.group.priority)
                )
            }
            SelectionOutcome::Unmatched => "not selected (selector did not match)".to_string(),
            SelectionOutcome::ExplicitOnly => {
                "not selected (no selector; explicit-only via --group)".to_string()
            }
            SelectionOutcome::Error(e) => format!("selector error: {e}"),
        }
    }
}

/// The full result of deriving groups for one channel: a per-group record for
/// *every* input group, in declaration order.
///
/// Holds the transparent report ([`all`](Self::all)) and offers the
/// precedence-ordered selected set ([`selected`](Self::selected)) the overlay
/// applies, plus the surfaced selector errors ([`errors`](Self::errors)).
#[derive(Debug)]
pub struct GroupDerivation<'a> {
    selections: Vec<GroupSelection<'a>>,
}

impl<'a> GroupDerivation<'a> {
    /// Every group's record, in declaration (input) order — the transparent
    /// report for `channels resolve` / `--explain`.
    pub fn all(&self) -> &[GroupSelection<'a>] {
        &self.selections
    }

    /// The selected groups, ordered by overlay precedence (ascending): by
    /// `priority` (higher last), then declaration `seq` (later last). This is
    /// the low-to-high application order for the overlay stack.
    pub fn selected(&self) -> Vec<&GroupSelection<'a>> {
        let mut chosen: Vec<&GroupSelection<'a>> =
            self.selections.iter().filter(|s| s.is_selected()).collect();
        // `layer()` is `Some` for every selected record; ordering is by the
        // `LayerKind::Group { priority, seq }` total order.
        chosen.sort_by_key(|s| s.layer());
        chosen
    }

    /// Iterate the groups whose selector failed to compile or evaluate, paired
    /// with the error. Callers decide whether such a failure is fatal.
    pub fn errors(&self) -> impl Iterator<Item = (&Group, &SelectorError)> {
        self.selections.iter().filter_map(|s| match &s.outcome {
            SelectionOutcome::Error(e) => Some((s.group, e)),
            _ => None,
        })
    }

    /// Whether any group's selector failed.
    pub fn has_errors(&self) -> bool {
        self.selections
            .iter()
            .any(|s| matches!(s.outcome, SelectionOutcome::Error(_)))
    }
}

/// Evaluate every group's selector against a channel's `labels` and derive the
/// selected set.
///
/// Groups are supplied in declaration order; each group's index becomes its
/// `seq` (the priority-tie-breaker). For each group:
///
/// - no `match:` selector ⇒ [`SelectionOutcome::ExplicitOnly`] (never
///   auto-selected);
/// - selector matches ⇒ [`SelectionOutcome::Selected`] at
///   [`LayerKind::Group { priority, seq }`];
/// - selector evaluates `false` ⇒ [`SelectionOutcome::Unmatched`];
/// - selector fails to compile/evaluate ⇒ [`SelectionOutcome::Error`] (an
///   unknown label or ill-typed selector is surfaced, not a silent non-match).
///
/// A channel with no manifest has no labels; pass an empty map.
pub fn derive_groups<'a>(
    groups: &'a [Group],
    labels: &IndexMap<String, JsonValue>,
) -> GroupDerivation<'a> {
    let selections = groups
        .iter()
        .enumerate()
        .map(|(index, group)| {
            let seq = seq_of(index);
            let outcome = evaluate_group(group, seq, labels);
            GroupSelection {
                group,
                seq,
                outcome,
            }
        })
        .collect();
    GroupDerivation { selections }
}

/// Evaluate a single group's selector against a label set.
fn evaluate_group(
    group: &Group,
    seq: u32,
    labels: &IndexMap<String, JsonValue>,
) -> SelectionOutcome {
    let Some(source) = &group.selector else {
        return SelectionOutcome::ExplicitOnly;
    };
    let selector = match LabelSelector::compile(source) {
        Ok(sel) => sel,
        Err(e) => return SelectionOutcome::Error(e),
    };
    match selector.matches(labels) {
        Ok(true) => SelectionOutcome::Selected {
            layer: group_layer(group, seq),
        },
        Ok(false) => SelectionOutcome::Unmatched,
        Err(e) => SelectionOutcome::Error(e),
    }
}

/// Clobber a group's `config:` onto a plan's provenance at its group `layer`.
///
/// Reuses the shared [`apply_config_clobber`](crate::overlay) engine — the same
/// clobber the channel layers use — so a group layer resolves with no bespoke
/// logic. Group config keys are raw `alias.param` strings; they are validated
/// into [`DottedPath`] here (apply-time validation), and a malformed key is a
/// hard [`ChannelError::InvalidDottedPath`]. A well-formed key that matches no
/// plan parameter is reported as an `E113` diagnostic by the clobber, exactly as
/// for a channel layer.
///
/// Group `config:` has no `fixed`/`default` split (only the per-target overlay
/// does), so every group value applies non-fixed. This is the single apply used
/// by both the derived path and the standalone `--group` path.
pub fn apply_group_config(
    provenance: &mut ProvenanceDb,
    group: &Group,
    layer: LayerKind,
    diagnostics: &mut Vec<Diagnostic>,
) -> Result<(), ChannelError> {
    let config = validate_group_config_keys(&group.config)?;
    crate::overlay::apply_config_clobber(
        provenance,
        &config,
        layer,
        false,
        &group.name,
        diagnostics,
    );
    Ok(())
}

/// Validate a group's raw `config:` keys into [`DottedPath`]s, cloning the
/// values. The first malformed key fails the whole apply.
fn validate_group_config_keys(
    config: &IndexMap<String, JsonValue>,
) -> Result<IndexMap<DottedPath, JsonValue>, ChannelError> {
    config
        .iter()
        .map(|(key, value)| Ok((DottedPath::try_from(key.as_str())?, value.clone())))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    use clinker_core_types::Span;
    use clinker_plan::config::composition::ResolvedValue;

    /// Build a label set from `(name, json)` pairs.
    fn labels(pairs: &[(&str, JsonValue)]) -> IndexMap<String, JsonValue> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect()
    }

    /// Build a group with a selector, priority, and a single `config` entry.
    fn group_with(
        name: &str,
        selector: Option<&str>,
        priority: i64,
        config: &[(&str, JsonValue)],
    ) -> Group {
        Group {
            name: name.to_string(),
            selector: selector.map(|s| s.to_string()),
            priority,
            config: config
                .iter()
                .map(|(k, v)| ((*k).to_string(), v.clone()))
                .collect(),
            vars: Default::default(),
            overrides: Vec::new(),
        }
    }

    // ── Derivation: which groups match a given label set ─────────────────

    #[test]
    fn selects_only_groups_whose_selector_matches() {
        let groups = vec![
            group_with("enterprise", Some(r#"tier == "enterprise""#), 20, &[]),
            group_with("west", Some(r#"region == "west""#), 10, &[]),
            group_with("east", Some(r#"region == "east""#), 10, &[]),
        ];
        let ls = labels(&[
            ("tier", JsonValue::from("enterprise")),
            ("region", JsonValue::from("west")),
        ]);
        let derivation = derive_groups(&groups, &ls);

        let selected: Vec<&str> = derivation
            .selected()
            .iter()
            .map(|s| s.group.name.as_str())
            .collect();
        // enterprise + west match; east does not.
        assert_eq!(selected, vec!["west", "enterprise"]);
        // Every group has a record in the transparent report.
        assert_eq!(derivation.all().len(), 3);
        assert!(!derivation.has_errors());
    }

    #[test]
    fn explicit_only_group_is_never_auto_selected() {
        // A group with no `match:` is explicit-only regardless of labels.
        let groups = vec![group_with("profile", None, 50, &[])];
        let ls = labels(&[("tier", JsonValue::from("enterprise"))]);
        let derivation = derive_groups(&groups, &ls);

        assert!(derivation.selected().is_empty());
        assert!(matches!(
            derivation.all()[0].outcome,
            SelectionOutcome::ExplicitOnly
        ));
    }

    #[test]
    fn no_groups_match_when_labels_disjoint() {
        let groups = vec![group_with("west", Some(r#"region == "west""#), 10, &[])];
        let ls = labels(&[("region", JsonValue::from("south"))]);
        let derivation = derive_groups(&groups, &ls);
        assert!(derivation.selected().is_empty());
        assert!(matches!(
            derivation.all()[0].outcome,
            SelectionOutcome::Unmatched
        ));
    }

    // ── Priority-then-declaration ordering ───────────────────────────────

    #[test]
    fn selected_ordered_by_priority_then_declaration() {
        // All match; distinct priorities. `selected()` is ascending precedence,
        // so the highest-priority group is last (it wins the clobber).
        let groups = vec![
            group_with("low", Some("true"), 5, &[]),
            group_with("high", Some("true"), 30, &[]),
            group_with("mid", Some("true"), 20, &[]),
        ];
        let derivation = derive_groups(&groups, &labels(&[]));
        let order: Vec<&str> = derivation
            .selected()
            .iter()
            .map(|s| s.group.name.as_str())
            .collect();
        assert_eq!(order, vec!["low", "mid", "high"]);
    }

    #[test]
    fn equal_priority_breaks_by_declaration_order() {
        // Equal priority: later declaration (higher seq) is the greater layer
        // and sorts last (wins ties).
        let groups = vec![
            group_with("first", Some("true"), 10, &[]),
            group_with("second", Some("true"), 10, &[]),
            group_with("third", Some("true"), 10, &[]),
        ];
        let derivation = derive_groups(&groups, &labels(&[]));
        let order: Vec<&str> = derivation
            .selected()
            .iter()
            .map(|s| s.group.name.as_str())
            .collect();
        assert_eq!(order, vec!["first", "second", "third"]);
        // seq follows declaration index.
        assert_eq!(derivation.all()[0].seq, 0);
        assert_eq!(derivation.all()[2].seq, 2);
    }

    #[test]
    fn priority_dominates_declaration_order() {
        // A later-declared but lower-priority group still sorts before an
        // earlier-declared higher-priority one — priority is the primary key.
        let groups = vec![
            group_with("early_high", Some("true"), 30, &[]),
            group_with("late_low", Some("true"), 10, &[]),
        ];
        let derivation = derive_groups(&groups, &labels(&[]));
        let order: Vec<&str> = derivation
            .selected()
            .iter()
            .map(|s| s.group.name.as_str())
            .collect();
        assert_eq!(order, vec!["late_low", "early_high"]);
    }

    // ── Transparent error reporting ──────────────────────────────────────

    #[test]
    fn unknown_label_in_selector_is_reported_not_silent() {
        // `regon` is a typo; the label set declares `region`. The derivation
        // surfaces the selector error rather than silently not selecting.
        let groups = vec![group_with("typo", Some(r#"regon == "west""#), 10, &[])];
        let ls = labels(&[("region", JsonValue::from("west"))]);
        let derivation = derive_groups(&groups, &ls);

        assert!(derivation.selected().is_empty());
        assert!(derivation.has_errors());
        let reported: Vec<_> = derivation.errors().collect();
        assert_eq!(reported.len(), 1);
        assert_eq!(reported[0].0.name, "typo");
        assert!(matches!(
            derivation.all()[0].outcome,
            SelectionOutcome::Error(SelectorError::Resolve(_))
        ));
    }

    #[test]
    fn describe_gives_a_reason_for_each_outcome() {
        let groups = vec![
            group_with("m", Some("true"), 7, &[]),
            group_with("u", Some("false"), 7, &[]),
            group_with("x", None, 7, &[]),
        ];
        let derivation = derive_groups(&groups, &labels(&[]));
        let all = derivation.all();
        assert!(all[0].describe().contains("selected"));
        assert!(all[1].describe().contains("did not match"));
        assert!(all[2].describe().contains("explicit-only"));
    }

    // ── Standalone application (no channel present) ──────────────────────

    #[test]
    fn standalone_group_applies_config_without_a_channel() {
        // A group invoked by name with no channel: its selector is never
        // evaluated, its config clobbers the provenance directly at its layer.
        let group = group_with(
            "enterprise",
            Some(r#"tier == "enterprise""#),
            20,
            &[("fraud_check.threshold", JsonValue::from(0.8))],
        );
        let mut prov = ProvenanceDb::default();
        prov.insert(
            "fraud_check".to_string(),
            "threshold".to_string(),
            ResolvedValue::new(
                JsonValue::from(0.5),
                LayerKind::PipelineDefault,
                Span::SYNTHETIC,
            ),
        );

        let mut diags = Vec::new();
        let layer = group_layer(&group, 0);
        apply_group_config(&mut prov, &group, layer, &mut diags).unwrap();

        assert!(diags.is_empty(), "well-formed keys raise no diagnostics");
        let resolved = prov.get("fraud_check", "threshold").unwrap();
        assert_eq!(resolved.value, JsonValue::from(0.8));
        assert_eq!(resolved.winning_layer().unwrap().kind, layer);
    }

    // ── Group layer materialization + precedence ─────────────────────────

    #[test]
    fn higher_priority_group_wins_the_clobber() {
        // Two matching groups touch the same param; the higher-priority group's
        // value must win once both layers are applied.
        let base = group_with(
            "base",
            Some("true"),
            10,
            &[("fraud_check.threshold", JsonValue::from(0.6))],
        );
        let strong = group_with(
            "strong",
            Some("true"),
            30,
            &[("fraud_check.threshold", JsonValue::from(0.95))],
        );
        let groups = vec![base, strong];

        let mut prov = ProvenanceDb::default();
        prov.insert(
            "fraud_check".to_string(),
            "threshold".to_string(),
            ResolvedValue::new(
                JsonValue::from(0.5),
                LayerKind::PipelineDefault,
                Span::SYNTHETIC,
            ),
        );

        let derivation = derive_groups(&groups, &labels(&[]));
        let mut diags = Vec::new();
        // Apply in the precedence order the overlay uses.
        for selection in derivation.selected() {
            apply_group_config(
                &mut prov,
                selection.group,
                selection.layer().unwrap(),
                &mut diags,
            )
            .unwrap();
        }

        assert!(diags.is_empty());
        let resolved = prov.get("fraud_check", "threshold").unwrap();
        assert_eq!(
            resolved.value,
            JsonValue::from(0.95),
            "priority-30 group wins"
        );
        assert_eq!(
            resolved.winning_layer().unwrap().kind,
            LayerKind::Group {
                priority: 30,
                seq: 1
            }
        );
    }

    #[test]
    fn unknown_group_config_key_is_e113_diagnostic() {
        // A well-formed key that matches no plan param is an E113 diagnostic
        // (never a silent no-op), consistent with the channel layers.
        let group = group_with(
            "g",
            Some("true"),
            10,
            &[("missing.param", JsonValue::from(1))],
        );
        let mut prov = ProvenanceDb::default();
        let mut diags = Vec::new();
        apply_group_config(&mut prov, &group, group_layer(&group, 0), &mut diags).unwrap();
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].code, "E113");
    }

    #[test]
    fn malformed_group_config_key_is_hard_error() {
        // A syntactically invalid dotted path fails the apply outright.
        let group = group_with("g", Some("true"), 10, &[("a.b.c", JsonValue::from(1))]);
        let mut prov = ProvenanceDb::default();
        let mut diags = Vec::new();
        let result = apply_group_config(&mut prov, &group, group_layer(&group, 0), &mut diags);
        assert!(matches!(
            result,
            Err(ChannelError::InvalidDottedPath { .. })
        ));
    }
}

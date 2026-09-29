//! One observed failure as the dead-letter output writes it.
//!
//! A [`HeldFailure`] is the unit every failing evaluation produces: the row
//! the evaluation is attributed to, written as the failure's trigger, and,
//! for a Combine output failure a build record contributed to, that build
//! record, written right after it as a collateral carrying the trigger's id.
//! The two travel as one value and are written by one call, so a build row
//! can never be written without the trigger row it names.
//!
//! Without a correlation key a failure is written at once through
//! [`write_failure`]. Under one, [`hold_failure_if_grouped`] holds it in its
//! group until the group commits, which writes it through the same entries.
//! A correlation key therefore never removes, merges or relabels a failure
//! row: it only adds the rows a group verdict condemns, and decides when rows
//! are written or rolled back.

use std::collections::HashMap;
use std::sync::Arc;

use clinker_record::Record;

use crate::executor::dispatch::{
    ExecutorContext, buffer_key_for_record, push_dlq, source_name_arc_of,
};
use crate::executor::stream_event::SourceRowId;
use crate::executor::{DlqEntry, DlqFailureStamp};
use clinker_plan::error::PipelineError;

/// One observed failure: its trigger row and, when a build record
/// contributed to a failing Combine output row, that build row.
///
/// Held in a correlation group, it counts as one entry against
/// `max_group_buffer` and puts only its trigger row in the group's retract
/// scope. It holds two cloned records at most; the group's buffer owns it
/// until commit.
#[derive(Debug, Clone)]
pub(crate) struct HeldFailure {
    /// The row the failing evaluation is attributed to.
    pub(crate) row_num: SourceRowId,
    /// The record at the moment of failure (e.g. the Transform input that
    /// failed evaluation).
    pub(crate) original_record: Record,
    pub(crate) category: clinker_core_types::dlq::DlqErrorCategory,
    pub(crate) error_message: String,
    pub(crate) stage: Option<String>,
    pub(crate) route: Option<String>,
    /// The field the failing evaluation was computing, when it named one.
    pub(crate) triggering_field: Option<Arc<str>>,
    /// The value the failing evaluation rejected, when it carries one.
    pub(crate) triggering_value: Option<clinker_record::Value>,
    /// The trigger's stamp, taken when the failure was observed
    /// ([`DlqFailureStamp::now`]).
    pub(crate) failed_at: DlqFailureStamp,
    contributing_build: Option<ContributingBuild>,
}

/// The build row of a failing Combine output row, stamped from its
/// failure's trigger. Only [`HeldFailure::with_contributing_build`] builds
/// one, so its pairing always names its own failure's trigger.
#[derive(Debug, Clone)]
struct ContributingBuild {
    row_num: SourceRowId,
    record: Record,
    failed_at: DlqFailureStamp,
}

/// What identifies a held failure across the relaxed-key commit's
/// retraction iterations, where each re-dispatch stamps it afresh: the
/// failing row, its stage, route and message, and the build row that
/// contributed to it.
type FailureKey = (
    SourceRowId,
    Option<String>,
    Option<String>,
    String,
    Option<SourceRowId>,
);

impl HeldFailure {
    /// A failure with no contributing build row. `failed_at` is the stamp
    /// taken where the failure was observed.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        row_num: SourceRowId,
        original_record: Record,
        category: clinker_core_types::dlq::DlqErrorCategory,
        error_message: String,
        stage: Option<String>,
        route: Option<String>,
        triggering_field: Option<Arc<str>>,
        triggering_value: Option<clinker_record::Value>,
        failed_at: DlqFailureStamp,
    ) -> Self {
        Self {
            row_num,
            original_record,
            category,
            error_message,
            stage,
            route,
            triggering_field,
            triggering_value,
            failed_at,
            contributing_build: None,
        }
    }

    /// This failure with `record`, the build row `row_num`, as the build
    /// record that contributed to it. The build row shares the failure's
    /// time and trigger id under its own id.
    pub(crate) fn with_contributing_build(mut self, record: Record, row_num: SourceRowId) -> Self {
        self.contributing_build = Some(ContributingBuild {
            row_num,
            record,
            failed_at: self.failed_at.sibling(),
        });
        self
    }

    /// The contributing build row's id, if a build record contributed.
    pub(crate) fn contributing_build_row(&self) -> Option<SourceRowId> {
        self.contributing_build.as_ref().map(|build| build.row_num)
    }

    fn key(&self) -> FailureKey {
        (
            self.row_num,
            self.stage.clone(),
            self.route.clone(),
            self.error_message.clone(),
            self.contributing_build_row(),
        )
    }

    /// The dead-letter entries this failure writes, in order: the trigger,
    /// then the contributing build row.
    pub(crate) fn into_entries(self) -> (DlqEntry, Option<DlqEntry>) {
        let trigger = DlqEntry {
            source_row: self.row_num,
            category: self.category,
            error_message: self.error_message.clone(),
            source_name: source_name_arc_of(&self.original_record),
            original_record: self.original_record,
            stage: self.stage.clone(),
            route: self.route,
            trigger: true,
            triggering_field: self.triggering_field,
            triggering_value: self.triggering_value,
            failed_at: self.failed_at,
        };
        let build = self.contributing_build.map(|build| DlqEntry {
            source_row: build.row_num,
            category: self.category,
            error_message: self.error_message,
            source_name: source_name_arc_of(&build.record),
            original_record: build.record,
            stage: self.stage,
            route: None,
            trigger: false,
            triggering_field: None,
            triggering_value: None,
            failed_at: build.failed_at,
        });
        (trigger, build)
    }
}

/// Write `failure` to the dead-letter output now: its trigger row, then its
/// contributing build row.
pub(crate) fn write_failure(
    ctx: &mut ExecutorContext<'_>,
    failure: HeldFailure,
) -> Result<(), PipelineError> {
    let (trigger, build) = failure.into_entries();
    push_dlq(ctx, trigger)?;
    if let Some(build) = build {
        push_dlq(ctx, build)?;
    }
    Ok(())
}

/// Hold `failure` in its trigger row's correlation group until the group
/// commits, when correlation buffering is active, and return `None`; return
/// the failure unchanged otherwise, for the caller to write or route.
///
/// The failure is admitted as one held entry against `max_group_buffer`,
/// stamping the group's overflow once the cap is exceeded, and its trigger
/// row joins the group's `error_rows`. A contributing build row is part of
/// the failure: it is neither admitted nor added to `error_rows`, so it
/// never condemns its own group and stays out of the relaxed-key retract
/// scope. Null-keyed records get a row-number-disambiguated cell, so each is
/// its own group of one.
pub(crate) fn hold_failure_if_grouped(
    ctx: &mut ExecutorContext<'_>,
    failure: HeldFailure,
) -> Option<HeldFailure> {
    let max_buf = ctx.correlation_max_group_buffer;
    let Some(buffers) = ctx.correlation_buffers.as_mut() else {
        return Some(failure);
    };
    let entry = buffers
        .entry(buffer_key_for_record(
            &failure.original_record,
            failure.row_num,
        ))
        .or_default();
    entry.admit_entry(max_buf);
    entry.error_rows.insert(failure.row_num);
    entry.error_messages.push(failure);
    None
}

/// Fold `from` into `into` so that each failure is held as many times as
/// the larger of the two holds it, identified by [`FailureKey`].
///
/// The relaxed-key commit archives each retraction iteration's held
/// failures and folds the archive back into the live buffer with this. A
/// failure observed again on a later iteration, under fresh stamps, is held
/// once; two failures of one driver against two build rows stay two; and a
/// row that reaches the same failing stage twice (an inclusive Route fanned
/// back in) keeps both. Failures new to `into` are appended in `from`'s
/// order.
pub(crate) fn union_held_failures(
    into: &mut Vec<HeldFailure>,
    from: impl IntoIterator<Item = HeldFailure>,
) {
    let mut held: HashMap<FailureKey, usize> = HashMap::new();
    for failure in into.iter() {
        *held.entry(failure.key()).or_default() += 1;
    }
    let mut offered: HashMap<FailureKey, usize> = HashMap::new();
    for failure in from {
        let key = failure.key();
        let seen = offered.entry(key.clone()).or_default();
        *seen += 1;
        if *seen > held.get(&key).copied().unwrap_or(0) {
            into.push(failure);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_core_types::dlq::DlqErrorCategory;
    use clinker_plan::plan::{EntityRef, PlanNodeId};
    use clinker_record::owned_storage::SharedStorage;
    use clinker_record::{Schema, Value};

    const MESSAGE: &str = "division by zero";

    fn row(source: usize, ordinal: u64) -> SourceRowId {
        SourceRowId::new(PlanNodeId::new(source), ordinal)
    }

    fn record(ordinal: u64) -> Record {
        Record::new(
            SharedStorage::from_arc(Arc::new(Schema::new(vec!["id".into()]))),
            vec![Value::Integer(ordinal as i64)],
        )
    }

    /// Driver `driver` of Source 0 failing against build `build` of Source 1,
    /// stamped afresh as a re-dispatch stamps it.
    fn combine_failure(driver: u64, build: u64) -> HeldFailure {
        HeldFailure::new(
            row(0, driver),
            record(driver),
            DlqErrorCategory::CombineOutputRow,
            MESSAGE.to_string(),
            Some("combine:joined".to_string()),
            None,
            Some(Arc::from("q")),
            None,
            DlqFailureStamp::now(),
        )
        .with_contributing_build(record(build), row(1, build))
    }

    fn shape(failures: &[HeldFailure]) -> Vec<(SourceRowId, Option<SourceRowId>)> {
        failures
            .iter()
            .map(|failure| (failure.row_num, failure.contributing_build_row()))
            .collect()
    }

    #[test]
    fn a_failure_writes_its_trigger_then_its_build_row_paired_with_it() {
        let (trigger, build) = combine_failure(1, 2).into_entries();
        let build = build.expect("a contributing build row is written");
        assert!(trigger.trigger);
        assert_eq!(trigger.failed_at.trigger_id(), trigger.failed_at.id());
        assert_eq!(trigger.triggering_field.as_deref(), Some("q"));
        assert!(!build.trigger);
        assert_eq!(build.source_row, row(1, 2));
        assert_eq!(build.failed_at.trigger_id(), trigger.failed_at.id());
        assert_ne!(build.failed_at.id(), trigger.failed_at.id());
        assert_eq!(build.failed_at.at(), trigger.failed_at.at());
    }

    /// Two failures of driver 1 against build rows 1 and 2 share row, stage
    /// and message; both survive two iterations of the archive, and each
    /// re-observed failure is held once.
    #[test]
    fn the_archive_keeps_every_failure_of_one_driver_across_iterations() {
        let iteration = || vec![combine_failure(1, 1), combine_failure(1, 2)];
        let mut archive = Vec::new();
        union_held_failures(&mut archive, iteration());
        union_held_failures(&mut archive, iteration());
        assert_eq!(
            shape(&archive),
            [(row(0, 1), Some(row(1, 1))), (row(0, 1), Some(row(1, 2)))]
        );

        let mut live = iteration();
        union_held_failures(&mut live, archive.clone());
        assert_eq!(
            shape(&live),
            shape(&archive),
            "the live cell already holds them"
        );

        let mut empty = Vec::new();
        union_held_failures(&mut empty, archive);
        assert_eq!(
            shape(&empty),
            [(row(0, 1), Some(row(1, 1))), (row(0, 1), Some(row(1, 2)))],
            "folding the archive into a cell without them restores both"
        );
    }

    /// A row that reached the same failing stage twice holds two failures
    /// with one key; the fold keeps both, and never more than the larger
    /// side holds.
    #[test]
    fn the_archive_keeps_a_repeated_failure_by_its_multiplicity() {
        let twice = || vec![combine_failure(1, 1), combine_failure(1, 1)];
        let mut archive = vec![combine_failure(1, 1)];
        union_held_failures(&mut archive, twice());
        assert_eq!(archive.len(), 2);
        union_held_failures(&mut archive, twice());
        assert_eq!(archive.len(), 2);
    }
}

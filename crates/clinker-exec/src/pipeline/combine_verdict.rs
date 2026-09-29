//! One Combine driver's verdict over its key candidates, shared by every join
//! strategy.
//!
//! A predicate has three outcomes, not two: it is true, it is not true (false
//! or null), or it failed to evaluate. A failed evaluation has said nothing
//! about the pair, so no decision that depends on every candidate may read it
//! as "not true". [`PredicateOutcome`] keeps the three apart and
//! [`eval_predicate`] is the one place that reduces an evaluator result to
//! them.
//!
//! [`DriverScan`] folds one driver's candidate outcomes into a
//! [`DriverVerdict`] under its `match` mode:
//!
//! - `first` is decided by the earliest candidate, in candidate order, whose
//!   outcome is not "not true". A true one is selected; a failed one is the
//!   driver's only result. Candidates after it are not part of the result, so
//!   a strategy that evaluated them anyway discards them.
//! - `all` keeps every true candidate and every failed one, each acted on as
//!   it is observed.
//! - `collect` returns the complete set of true candidates, so a single failed
//!   candidate leaves the set unknown and the driver writes no row, only its
//!   failures.
//!
//! A driver is a miss only when it had no true and no failed candidate.
//! [`MissToken`] is the proof of that: only [`DriverScan::finish`] (and
//! [`MissToken::no_candidates`], for a driver whose keys admit none) can make
//! one, and every `on_miss` path takes it, so an `on_miss` action after a
//! failure does not compile.
//!
//! Body outcomes come after selection and never reach the verdict: a true
//! candidate whose body skips or fails is still a match.
//!
//! Route reduces its branch conditions through [`PredicateOutcome`] too: a
//! failed condition takes no branch and not the default, as a failed `first`
//! candidate takes no later one.

use clinker_record::FieldResolver;
use cxl::eval::{EvalContext, EvalError, EvalResult, ProgramEvaluator};

use clinker_plan::config::pipeline_node::MatchMode;
use clinker_plan::error::PipelineError;

/// The outcome of one predicate evaluation for one pair.
#[derive(Debug)]
pub(crate) enum PredicateOutcome {
    /// The predicate evaluated to exactly `true`.
    True,
    /// The predicate evaluated to anything else: `false`, `null`, or a value
    /// that is not a bool. Not a match.
    NotTrue,
    /// The evaluation failed. Neither a match nor a non-match.
    Failed(EvalError),
}

impl PredicateOutcome {
    /// Reduce one evaluation of a program used as a predicate: a program
    /// that emitted, singly or fanned out, held; one that skipped did not;
    /// one that returned an error failed.
    pub(crate) fn from_result(result: Result<EvalResult, EvalError>) -> Self {
        match result {
            Ok(EvalResult::Emit { .. } | EvalResult::EmitMany { .. }) => PredicateOutcome::True,
            Ok(EvalResult::Skip(_)) => PredicateOutcome::NotTrue,
            Err(error) => PredicateOutcome::Failed(error),
        }
    }
}

/// Evaluate a Combine residual `program` over `resolver` and reduce the
/// result to a [`PredicateOutcome`].
///
/// Streams nothing and holds nothing past the call. A residual is a filter
/// and cannot fan out, so a fan-out result is an engine invariant violation,
/// reported as `PipelineError::Internal` under `op` and the Combine `name`.
pub(crate) fn eval_predicate<S: clinker_record::RecordStorage + 'static>(
    program: &mut ProgramEvaluator,
    ctx: &EvalContext<'_>,
    resolver: &dyn FieldResolver,
    op: &'static str,
    name: &str,
) -> Result<PredicateOutcome, PipelineError> {
    let result = program.eval_record::<S>(ctx, resolver, None);
    if let Ok(EvalResult::EmitMany { .. }) = result {
        return Err(PipelineError::Internal {
            op,
            node: name.to_string(),
            detail: "emit_each fan-out is not supported in a combine residual filter".into(),
        });
    }
    Ok(PredicateOutcome::from_result(result))
}

/// Proof that a driver had no true and no failed candidate, so its `on_miss`
/// policy applies. Only [`DriverScan::finish`] and
/// [`MissToken::no_candidates`] construct one.
#[derive(Debug)]
pub(crate) struct MissToken(());

impl MissToken {
    /// The miss of a driver whose join keys admit no candidate at all: a
    /// NULL or non-orderable key value, which cannot satisfy the predicate.
    /// Also the miss a strategy replays for a driver it recorded as missed
    /// with a token earlier, after moving it through a spillable pile.
    pub(crate) fn no_candidates() -> Self {
        MissToken(())
    }
}

/// The candidate that decides a `first` driver: a true one or a failed one.
#[derive(Debug)]
pub(crate) enum Decisive<M, F> {
    Match(M),
    Failure(F),
}

/// What a strategy must do with the candidate it just observed.
#[derive(Debug)]
pub(crate) enum Admit<M, F> {
    /// `all` / `collect`: the true candidate is part of the result; act on it
    /// now (run the body, or add it to the collect array).
    Take,
    /// `all` / `collect`: the failed candidate is part of the result; write
    /// its failure now.
    Fail(F),
    /// `first`: the candidate is now the deciding one and the scan holds it;
    /// `displaced` is the candidate it replaced, if any.
    Decides { displaced: Option<Decisive<M, F>> },
    /// The candidate is not part of the result: a `first` candidate after the
    /// deciding one, or a true `collect` candidate after a failed one.
    Ignore,
}

/// One driver's verdict.
#[derive(Debug)]
pub(crate) enum DriverVerdict<M, F> {
    /// `first`: the deciding candidate is true; run the body on it once.
    Selected(M),
    /// `first`: the deciding candidate failed; write this one failure and
    /// nothing else.
    FailedFirst(F),
    /// `all`: the driver had a true or a failed candidate, each already acted
    /// on as it was observed.
    Pairs,
    /// `collect`: no candidate failed; write the collect row, with an empty
    /// array when nothing matched.
    Collected,
    /// `collect`: a candidate failed, so the array is unknown; write no row.
    CollectFailed,
    /// No true and no failed candidate: apply `on_miss`.
    Miss(MissToken),
}

impl<M, F> DriverVerdict<M, F> {
    /// The engine invariant violation for a verdict its scan's `match` mode
    /// cannot produce, reported under `op` and the Combine `name`.
    pub(crate) fn mode_mismatch(&self, op: &'static str, name: &str) -> PipelineError {
        let verdict = match self {
            DriverVerdict::Selected(_) => "a first selection",
            DriverVerdict::FailedFirst(_) => "a first failure",
            DriverVerdict::Pairs => "an all verdict",
            DriverVerdict::Collected | DriverVerdict::CollectFailed => "a collect verdict",
            DriverVerdict::Miss(_) => "a miss",
        };
        PipelineError::Internal {
            op,
            node: name.to_string(),
            detail: format!("driver scan produced {verdict} its match mode cannot produce"),
        }
    }
}

/// Folds one driver's candidate outcomes into its [`DriverVerdict`].
///
/// Holds two counts and, under `first`, at most one boxed candidate (`M` or
/// `F`), so a slot stays small when a strategy keeps one per driver; nothing
/// grows with the number of candidates. The scan never evaluates anything
/// itself: the strategy evaluates each candidate's predicate and reports it
/// with its position in candidate order, the build input's arrival order.
/// A strategy that walks candidates in that order may stop once
/// [`DriverScan::settled`] holds; one that visits them in any order reports
/// each with its position and the scan keeps the earliest.
#[derive(Debug)]
pub(crate) struct DriverScan<M, F> {
    mode: MatchMode,
    matched: u64,
    failed: u64,
    decisive: Option<Box<(u64, Decisive<M, F>)>>,
}

impl<M, F> DriverScan<M, F> {
    pub(crate) fn new(mode: MatchMode) -> Self {
        Self {
            mode,
            matched: 0,
            failed: 0,
            decisive: None,
        }
    }

    /// Whether a `first` driver's decision is made for a strategy that walks
    /// its candidates in candidate order: every later candidate is outside
    /// the result. Always `false` under `all` and `collect`.
    pub(crate) fn settled(&self) -> bool {
        self.decisive.is_some()
    }

    /// Report a candidate at `order` whose predicate is true. `pick` builds
    /// the candidate a `first` scan holds; it runs only when this candidate
    /// becomes the deciding one.
    pub(crate) fn observe_true(&mut self, order: u64, pick: impl FnOnce() -> M) -> Admit<M, F> {
        match self.mode {
            MatchMode::All => {
                self.matched += 1;
                Admit::Take
            }
            MatchMode::Collect => {
                self.matched += 1;
                if self.failed > 0 {
                    Admit::Ignore
                } else {
                    Admit::Take
                }
            }
            MatchMode::First => self.decide(order, || Decisive::Match(pick())),
        }
    }

    /// Report a candidate at `order` whose predicate failed to evaluate.
    /// `failure` builds its failure; under `first` it runs only when this
    /// candidate becomes the deciding one.
    pub(crate) fn observe_failed(
        &mut self,
        order: u64,
        failure: impl FnOnce() -> F,
    ) -> Admit<M, F> {
        match self.mode {
            MatchMode::All | MatchMode::Collect => {
                self.failed += 1;
                Admit::Fail(failure())
            }
            MatchMode::First => self.decide(order, || Decisive::Failure(failure())),
        }
    }

    fn decide(&mut self, order: u64, candidate: impl FnOnce() -> Decisive<M, F>) -> Admit<M, F> {
        match self.decisive.as_deref() {
            Some((held, _)) if *held <= order => Admit::Ignore,
            _ => {
                let displaced = self
                    .decisive
                    .replace(Box::new((order, candidate())))
                    .map(|held| held.1);
                Admit::Decides { displaced }
            }
        }
    }

    /// The driver's verdict. Every candidate must have been reported.
    pub(crate) fn finish(self) -> DriverVerdict<M, F> {
        match self.mode {
            MatchMode::First => match self.decisive.map(|held| held.1) {
                Some(Decisive::Match(pick)) => DriverVerdict::Selected(pick),
                Some(Decisive::Failure(failure)) => DriverVerdict::FailedFirst(failure),
                None => DriverVerdict::Miss(MissToken(())),
            },
            MatchMode::All => {
                if self.matched + self.failed > 0 {
                    DriverVerdict::Pairs
                } else {
                    DriverVerdict::Miss(MissToken(()))
                }
            }
            MatchMode::Collect => {
                if self.failed > 0 {
                    DriverVerdict::CollectFailed
                } else {
                    DriverVerdict::Collected
                }
            }
        }
    }

    /// The held `first` candidate, without finishing the scan. Used by a
    /// strategy that charges the held candidate's bytes.
    pub(crate) fn held(&self) -> Option<&Decisive<M, F>> {
        self.decisive.as_deref().map(|(_, held)| held)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type Scan = DriverScan<&'static str, &'static str>;

    fn verdict(
        mode: MatchMode,
        outcomes: &[(u64, Option<bool>)],
    ) -> DriverVerdict<&'static str, &'static str> {
        // `Some(true)`: true; `Some(false)`: failed; `None`: not true.
        let mut scan = Scan::new(mode);
        for &(order, outcome) in outcomes {
            match outcome {
                Some(true) => {
                    scan.observe_true(order, || "match");
                }
                Some(false) => {
                    scan.observe_failed(order, || "failure");
                }
                None => {}
            }
        }
        scan.finish()
    }

    #[test]
    fn a_failed_candidate_is_never_a_miss() {
        for mode in [MatchMode::First, MatchMode::All] {
            assert!(
                !matches!(verdict(mode, &[(0, Some(false))]), DriverVerdict::Miss(_)),
                "{mode:?}: a failed candidate is not a miss"
            );
            assert!(
                !matches!(
                    verdict(mode, &[(0, None), (1, Some(false))]),
                    DriverVerdict::Miss(_)
                ),
                "{mode:?}: [not true, failed] is not a miss"
            );
        }
    }

    #[test]
    fn only_a_driver_with_no_true_and_no_failed_candidate_is_a_miss() {
        for mode in [MatchMode::First, MatchMode::All] {
            assert!(matches!(verdict(mode, &[]), DriverVerdict::Miss(_)));
            assert!(matches!(
                verdict(mode, &[(0, None), (1, None)]),
                DriverVerdict::Miss(_)
            ));
        }
    }

    #[test]
    fn first_stops_at_the_earliest_candidate_that_is_not_not_true() {
        assert!(matches!(
            verdict(
                MatchMode::First,
                &[(0, None), (1, Some(false)), (2, Some(true))]
            ),
            DriverVerdict::FailedFirst("failure")
        ));
        assert!(matches!(
            verdict(MatchMode::First, &[(0, Some(true)), (1, Some(false))]),
            DriverVerdict::Selected("match")
        ));
    }

    #[test]
    fn first_keeps_the_earliest_candidate_whatever_order_it_is_reported_in() {
        // Reported out of candidate order: the failure at 1 precedes the
        // match at 2, so it decides.
        assert!(matches!(
            verdict(
                MatchMode::First,
                &[(2, Some(true)), (1, Some(false)), (3, Some(true))]
            ),
            DriverVerdict::FailedFirst("failure")
        ));
        let mut scan = Scan::new(MatchMode::First);
        assert!(matches!(
            scan.observe_failed(5, || "late failure"),
            Admit::Decides { displaced: None }
        ));
        assert!(matches!(
            scan.observe_true(3, || "early match"),
            Admit::Decides {
                displaced: Some(Decisive::Failure("late failure"))
            }
        ));
        assert!(matches!(scan.observe_failed(4, || "later"), Admit::Ignore));
        assert!(matches!(
            scan.finish(),
            DriverVerdict::Selected("early match")
        ));
    }

    #[test]
    fn first_builds_a_candidate_only_when_it_decides() {
        let mut scan = Scan::new(MatchMode::First);
        scan.observe_true(0, || "match");
        assert!(scan.settled());
        let built = std::cell::Cell::new(false);
        scan.observe_failed(1, || {
            built.set(true);
            "never"
        });
        assert!(
            !built.get(),
            "a candidate after the deciding one is never built"
        );
    }

    #[test]
    fn collect_writes_no_row_once_a_candidate_failed() {
        assert!(matches!(
            verdict(MatchMode::Collect, &[(0, Some(true)), (1, Some(false))]),
            DriverVerdict::CollectFailed
        ));
        assert!(matches!(
            verdict(MatchMode::Collect, &[(0, Some(false))]),
            DriverVerdict::CollectFailed
        ));
        assert!(matches!(
            verdict(MatchMode::Collect, &[(0, None)]),
            DriverVerdict::Collected
        ));
        let mut scan = Scan::new(MatchMode::Collect);
        assert!(matches!(scan.observe_true(0, || "m"), Admit::Take));
        assert!(matches!(scan.observe_failed(1, || "f"), Admit::Fail("f")));
        assert!(
            matches!(scan.observe_true(2, || "m"), Admit::Ignore),
            "a true candidate after a failure joins no array"
        );
    }

    #[test]
    fn all_takes_every_true_and_every_failed_candidate() {
        let mut scan = Scan::new(MatchMode::All);
        assert!(matches!(scan.observe_true(0, || "m"), Admit::Take));
        assert!(matches!(scan.observe_failed(1, || "f"), Admit::Fail("f")));
        assert!(matches!(scan.observe_true(2, || "m"), Admit::Take));
        assert!(!scan.settled(), "all never settles early");
        assert!(matches!(scan.finish(), DriverVerdict::Pairs));
    }
}

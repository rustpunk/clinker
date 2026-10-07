//! The end of a streaming hop.
//!
//! A streaming hop hands a producer's rows, as the walk produces them, to a
//! step running on its own thread over a bounded channel: a streaming Sink's
//! writer, a streaming Aggregate's ingest, a streaming Combine's probe. The
//! channel carries the producer's events and then, only once the producer's
//! dispatch has returned `Ok`, one [`HopMessage::End`], sent by the hop's
//! driver through its [`HopEnd`]. A consumer finishes its work (finalizes its
//! groups, completes its probe, closes its output) only on that End. A
//! channel that closes without End means the producer failed or was stopped,
//! so the consumer's input is a prefix and the consumer finishes nothing.
//!
//! Every producer hands its consumer the rows it emitted before it reports
//! its own failure. A consumer therefore meets its rows in data order and
//! fails on the earliest row it cannot take, and [`settle_hop`] decides the
//! hop from that order: a consumer's own failure is on a row its producer
//! emitted before any failure the producer reports.

use std::ops::ControlFlow;
use std::sync::Arc;

use clinker_plan::error::PipelineError;

use crate::executor::stream_event::StreamEvent;

/// One item on a streaming hop's channel: a producer's event, or the end of
/// the producer's output. Node buffers hold [`StreamEvent`]s; only the
/// channels between threads carry the end.
#[derive(Debug)]
pub(crate) enum HopMessage {
    Event(StreamEvent),
    /// The producer's dispatch returned `Ok`, so the events before this one
    /// are its whole output.
    End,
}

/// The producer side of a streaming hop's channel.
pub(crate) type HopSender = crossbeam_channel::Sender<HopMessage>;

/// The consumer side of a streaming hop's channel.
pub(crate) type HopReceiver = crossbeam_channel::Receiver<HopMessage>;

/// The hop driver's hold on the end of its hop's channel.
///
/// It owns a sender of its own, so the channel stays open until the driver
/// lets go of it, whenever the producer drops its own sender. Calling
/// [`Self::end`] tells the consumer its input is complete; dropping the
/// value without calling it, on a failed or interrupted producer or while
/// the walk unwinds, leaves the channel to close without End, which the
/// consumer reads as an incomplete input.
pub(crate) struct HopEnd {
    sender: HopSender,
}

impl HopEnd {
    pub(crate) fn new(sender: HopSender) -> Self {
        Self { sender }
    }

    /// Send End and release the driver's sender. Call it only once the
    /// producer's dispatch has returned `Ok`.
    ///
    /// Blocks while the bounded channel is full. A consumer keeps reading its
    /// channel until End or until the channel closes, on its failure path
    /// too, so the send completes. A send to a consumer that has already gone
    /// is ignored: that consumer's own result reports why it went.
    pub(crate) fn end(self) {
        let _ = self.sender.send(HopMessage::End);
    }
}

/// How a consumer's input ended, when the consumer did not fail first.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HopVerdict {
    /// The consumer read End: its input is the producer's whole output, and
    /// it finished its work on it.
    Ended,
    /// The channel closed without End: the producer failed or was stopped,
    /// and the consumer finished nothing.
    UpstreamIncomplete,
}

/// Take the next event from `rx`, or stop with the verdict on how the input
/// ended.
pub(crate) fn next_event(rx: &HopReceiver) -> ControlFlow<HopVerdict, StreamEvent> {
    match rx.recv() {
        Ok(HopMessage::Event(event)) => ControlFlow::Continue(event),
        Ok(HopMessage::End) => ControlFlow::Break(HopVerdict::Ended),
        Err(crossbeam_channel::RecvError) => ControlFlow::Break(HopVerdict::UpstreamIncomplete),
    }
}

/// Read `rx` until the channel closes, discharging each record's per-row
/// charge from `charge`, whatever the messages are.
///
/// A consumer that stops taking its input calls this on its way out, so a
/// producer blocked on the bounded send, or the driver sending End, can
/// finish and the hop's join cannot wait on a send that never completes.
pub(crate) fn discard_until_closed(
    rx: &HopReceiver,
    charge: &crate::pipeline::memory::ConsumerHandle,
    resources: &clinker_record::owned_storage::AllocationResources,
) {
    while let Ok(message) = rx.recv() {
        if let HopMessage::Event(StreamEvent::Record(record, _)) = message {
            charge.sub_bytes(crate::executor::node_buffer::unaccounted_record_byte_cost(
                &record, resources,
            ));
        }
    }
}

/// Decide a streaming hop from its consumer's and its producer's results.
/// Runs on the walk's thread, after the consumer's thread has joined; `node`
/// is the consumer and `upstream` the producer feeding it.
///
/// - The consumer's own failure wins: it is on a row the producer emitted
///   before any failure the producer reports. A producer failure beside it
///   is logged with the consumer's name, unless it is the run's
///   cancellation, which is not a failure.
/// - A consumer whose input closed without End reports its producer's
///   result.
/// - A consumer that read End, beside a producer that returned `Ok`, gives
///   `Ok`.
/// - End beside a failed producer, or a closed input beside a producer that
///   returned `Ok`, is an invariant violation: the driver sends End exactly
///   when the producer returned `Ok`.
pub(crate) fn settle_hop(
    node: &str,
    upstream: &str,
    consumer: Result<HopVerdict, PipelineError>,
    producer: Result<(), PipelineError>,
) -> Result<(), PipelineError> {
    match (consumer, producer) {
        (Err(consumer_error), producer) => {
            if let Err(later) = producer
                && !super::preparation::is_explicit_cancellation(&later)
            {
                tracing::warn!(
                    node,
                    upstream,
                    error = %later,
                    "the step feeding this one also failed, after this step had failed on an \
                     earlier row; the run reports this step's failure"
                );
            }
            Err(consumer_error)
        }
        (Ok(HopVerdict::UpstreamIncomplete), Err(producer_error)) => Err(producer_error),
        (Ok(HopVerdict::Ended), Ok(())) => Ok(()),
        (Ok(HopVerdict::Ended), Err(producer_error)) => Err(PipelineError::Internal {
            op: "streaming-hop",
            node: node.to_string(),
            detail: format!(
                "the step read the end of its input from {upstream:?}, which failed: \
                 {producer_error}"
            ),
        }),
        (Ok(HopVerdict::UpstreamIncomplete), Ok(())) => Err(PipelineError::Internal {
            op: "streaming-hop",
            node: node.to_string(),
            detail: format!(
                "the step's input from {upstream:?} closed without its end, though \
                 {upstream:?} finished"
            ),
        }),
    }
}

/// Where a run records how each of its streaming consumers ended. Only an
/// in-process test, through `MemoryTestOverrides::with_streaming_ends`
/// (compiled only for tests), gives it a record; otherwise recording does
/// nothing.
#[derive(Clone, Default)]
pub(crate) struct HopEndLog {
    #[cfg(any(test, feature = "test-utils"))]
    record: Option<crate::executor::StreamingEnds>,
}

impl HopEndLog {
    /// The log `overrides` asks the run to keep, if any.
    pub(crate) fn for_run(overrides: &crate::executor::MemoryTestOverrides) -> Self {
        #[cfg(any(test, feature = "test-utils"))]
        {
            Self {
                record: overrides.streaming_ends().cloned(),
            }
        }
        #[cfg(not(any(test, feature = "test-utils")))]
        {
            let _ = overrides;
            Self::default()
        }
    }

    /// Record that the consumer `node` stopped with its input ended as
    /// `input` (`None` when it failed first), having `finished` its work
    /// (finalized, completed or closed) or not.
    pub(crate) fn record(&self, node: &str, input: Option<HopVerdict>, finished: bool) {
        #[cfg(any(test, feature = "test-utils"))]
        if let Some(record) = &self.record {
            let input = match input {
                Some(HopVerdict::Ended) => crate::executor::StreamingInputEnd::Ended,
                Some(HopVerdict::UpstreamIncomplete) => {
                    crate::executor::StreamingInputEnd::Incomplete
                }
                None => crate::executor::StreamingInputEnd::ConsumerFailed,
            };
            record.record(crate::executor::StreamingEnd {
                node: node.to_string(),
                input,
                finished,
            });
        }
        #[cfg(not(any(test, feature = "test-utils")))]
        {
            let _ = (node, input, finished);
        }
    }
}

/// The end of a streaming Sink's hop, held by the walk until the Sink's
/// producer finishes its top-level turn.
pub(crate) struct SinkHopEnd {
    /// The Sink the hop feeds.
    pub(crate) sink: String,
    pub(crate) end: HopEnd,
}

/// Let go of every streaming Sink hop end the walk still holds once it has
/// stopped, before the Sinks' threads are joined: each Sink then sees its
/// channel close without End and finishes nothing.
///
/// A walk that completed has ended every Sink's hop as each producer's turn
/// returned, so one still held there names a Sink no producer finished:
/// [`PipelineError::Internal`] naming that Sink. After a failed or
/// interrupted walk, the ends held are expected and dropped silently.
pub(crate) fn release_sink_hop_ends(
    ends: std::collections::HashMap<petgraph::graph::NodeIndex, SinkHopEnd>,
    walk_completed: bool,
) -> Result<(), PipelineError> {
    let mut sinks: Vec<String> = ends.into_values().map(|held| held.sink).collect();
    if !walk_completed || sinks.is_empty() {
        return Ok(());
    }
    sinks.sort_unstable();
    Err(PipelineError::Internal {
        op: "streaming-hop",
        node: sinks[0].clone(),
        detail: "the walk completed without ending this streaming Sink's input; no producer \
                 turn finished it"
            .to_string(),
    })
}

/// The receiving end of a streaming hop a consumer drains on its own thread,
/// with the driver's end and the edge's charge registration.
pub(crate) struct StreamingIngestHop {
    pub(crate) rx: HopReceiver,
    pub(crate) end: HopEnd,
    pub(crate) charge_handle: Arc<crate::pipeline::memory::ConsumerHandle>,
    pub(crate) charge_consumer_id: crate::pipeline::memory::ConsumerId,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn failure(detail: &str) -> PipelineError {
        PipelineError::Internal {
            op: "test",
            node: "any".to_string(),
            detail: detail.to_string(),
        }
    }

    /// Settle one pair on this thread, keeping the warnings it logs.
    fn settle(
        consumer: Result<HopVerdict, PipelineError>,
        producer: Result<(), PipelineError>,
    ) -> (Result<(), PipelineError>, Vec<String>) {
        crate::executor::tests::capture_warnings(|| {
            settle_hop("totals", "pass", consumer, producer)
        })
    }

    fn internal_naming_totals(result: &Result<(), PipelineError>) -> bool {
        matches!(
            result,
            Err(PipelineError::Internal { op: "streaming-hop", node, .. }) if node == "totals"
        )
    }

    /// Every pair of a consumer's and a producer's result settles as the
    /// data order decides: the consumer's own failure first, a producer
    /// failure beside it logged unless it is the cancellation, an incomplete
    /// input reporting its producer, and the two pairs the driver cannot
    /// produce reported as invariant violations naming the consumer.
    #[test]
    fn settle_hop_ranks_the_consumers_own_failure_first() {
        let (result, warnings) = settle(Err(failure("consumer row 1")), Ok(()));
        assert_eq!(
            result.unwrap_err().to_string(),
            failure("consumer row 1").to_string()
        );
        assert!(warnings.is_empty(), "{warnings:?}");

        let (result, warnings) = settle(
            Err(failure("consumer row 1")),
            Err(failure("producer row 5000")),
        );
        assert_eq!(
            result.unwrap_err().to_string(),
            failure("consumer row 1").to_string()
        );
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(
            warnings[0].contains(r#"node="totals""#)
                && warnings[0].contains(r#"upstream="pass""#)
                && warnings[0].contains("producer row 5000"),
            "the producer's later failure is logged naming the step: {warnings:?}"
        );

        let (result, warnings) = settle(
            Err(failure("consumer row 1")),
            Err(PipelineError::Interrupted),
        );
        assert_eq!(
            result.unwrap_err().to_string(),
            failure("consumer row 1").to_string()
        );
        assert!(
            warnings.is_empty(),
            "a cancellation is not logged: {warnings:?}"
        );

        let (result, warnings) = settle(
            Ok(HopVerdict::UpstreamIncomplete),
            Err(failure("producer row 5000")),
        );
        assert_eq!(
            result.unwrap_err().to_string(),
            failure("producer row 5000").to_string()
        );
        assert!(warnings.is_empty(), "{warnings:?}");

        let (result, _) = settle(
            Ok(HopVerdict::UpstreamIncomplete),
            Err(PipelineError::Interrupted),
        );
        assert!(matches!(result, Err(PipelineError::Interrupted)));

        let (result, warnings) = settle(Ok(HopVerdict::Ended), Ok(()));
        assert!(result.is_ok());
        assert!(warnings.is_empty(), "{warnings:?}");

        let (result, _) = settle(Ok(HopVerdict::Ended), Err(failure("producer row 5000")));
        assert!(
            internal_naming_totals(&result),
            "End beside a failed producer: {result:?}"
        );

        let (result, _) = settle(Ok(HopVerdict::UpstreamIncomplete), Ok(()));
        assert!(
            internal_naming_totals(&result),
            "a closed input beside a finished producer: {result:?}"
        );
    }

    /// A consumer meets End only when the driver sends it; a channel whose
    /// senders all drop without it reads as incomplete.
    #[test]
    fn a_hop_ends_only_on_its_drivers_end() {
        let (tx, rx) = crossbeam_channel::unbounded::<HopMessage>();
        let end = HopEnd::new(tx.clone());
        drop(tx);
        end.end();
        assert!(matches!(
            next_event(&rx),
            ControlFlow::Break(HopVerdict::Ended)
        ));

        let (tx, rx) = crossbeam_channel::unbounded::<HopMessage>();
        let end = HopEnd::new(tx.clone());
        drop(tx);
        drop(end);
        assert!(matches!(
            next_event(&rx),
            ControlFlow::Break(HopVerdict::UpstreamIncomplete)
        ));
    }

    /// A completed walk that still holds a Sink's hop end reports the Sink
    /// no producer finished; a failed or interrupted walk lets every held
    /// end go silently. Either way each held end is dropped unsent, so its
    /// Sink sees its input close without End.
    #[test]
    fn a_completed_walk_holding_a_sink_hop_end_is_an_engine_defect() {
        let held = |sink: &str| {
            let (tx, rx) = crossbeam_channel::unbounded::<HopMessage>();
            let end = SinkHopEnd {
                sink: sink.to_string(),
                end: HopEnd::new(tx),
            };
            (end, rx)
        };
        let (b, b_rx) = held("b_out");
        let (a, a_rx) = held("a_out");
        let ends = std::collections::HashMap::from([
            (petgraph::graph::NodeIndex::new(1), b),
            (petgraph::graph::NodeIndex::new(2), a),
        ]);
        let result = release_sink_hop_ends(ends, true);
        assert!(
            matches!(
                &result,
                Err(PipelineError::Internal { op: "streaming-hop", node, .. }) if node == "a_out"
            ),
            "the first Sink by name is reported: {result:?}"
        );
        for rx in [&a_rx, &b_rx] {
            assert!(matches!(
                next_event(rx),
                ControlFlow::Break(HopVerdict::UpstreamIncomplete)
            ));
        }

        let (c, c_rx) = held("c_out");
        let ends = std::collections::HashMap::from([(petgraph::graph::NodeIndex::new(3), c)]);
        assert!(release_sink_hop_ends(ends, false).is_ok());
        assert!(matches!(
            next_event(&c_rx),
            ControlFlow::Break(HopVerdict::UpstreamIncomplete)
        ));
        assert!(release_sink_hop_ends(std::collections::HashMap::new(), true).is_ok());
    }

    /// A producer's streaming sender pairs with its slot's charge; a sender
    /// without one is an invariant violation naming the producer, reported
    /// instead of panicking, and no sender is no hop.
    #[test]
    fn a_streaming_sender_without_its_charge_is_an_internal_error_naming_the_step() {
        let (tx, _rx) = crossbeam_channel::unbounded::<HopMessage>();
        let result = crate::executor::dispatch::pair_streaming_hop(Some(tx), None::<()>, "routed");
        assert!(
            matches!(
                &result,
                Err(PipelineError::Internal { node, .. }) if node == "routed"
            ),
            "{result:?}"
        );
        let (tx, _rx) = crossbeam_channel::unbounded::<HopMessage>();
        assert!(matches!(
            crate::executor::dispatch::pair_streaming_hop(Some(tx), Some(()), "routed"),
            Ok(Some((_, ())))
        ));
        assert!(matches!(
            crate::executor::dispatch::pair_streaming_hop(None, None::<()>, "routed"),
            Ok(None)
        ));
    }
}

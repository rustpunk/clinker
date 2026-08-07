use std::io::{self, Write};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::time::{Duration, Instant};

use clinker_lineage::{
    EventType, Job, LineageAdmission, LineageDelivery, LineageDeliveryConfig,
    LineageDeliveryTerminal, Run, RunEvent,
};

fn event(job_name: &str) -> RunEvent {
    RunEvent {
        event_time: "2026-08-07T00:00:00Z".to_owned(),
        producer: "https://github.com/rustpunk/clinker".to_owned(),
        schema_url: "https://openlineage.io/spec/2-0-2/OpenLineage.json".to_owned(),
        event_type: EventType::Start,
        run: Run::new("0190b7e0-0000-7000-8000-000000000000"),
        job: Job {
            namespace: "clinker".to_owned(),
            name: job_name.to_owned(),
            facets: None,
        },
        inputs: Vec::new(),
        outputs: Vec::new(),
    }
}

fn config(queue_bytes: usize, max_event_bytes: usize, deadline_ms: u64) -> LineageDeliveryConfig {
    LineageDeliveryConfig::new(
        queue_bytes,
        max_event_bytes,
        Duration::from_millis(deadline_ms),
    )
    .expect("valid test delivery bounds")
}

#[derive(Default)]
struct SinkState {
    bytes: Mutex<Vec<u8>>,
    flushes: Mutex<u64>,
}

struct RecordingSink(Arc<SinkState>);

impl Write for RecordingSink {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.bytes.lock().expect("bytes lock").extend(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        *self.0.flushes.lock().expect("flush lock") += 1;
        Ok(())
    }
}

struct FailingSink {
    write_kind: Option<io::ErrorKind>,
    flush_kind: Option<io::ErrorKind>,
}

impl Write for FailingSink {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        match self.write_kind {
            Some(kind) => Err(io::Error::from(kind)),
            None => Ok(bytes.len()),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self.flush_kind {
            Some(kind) => Err(io::Error::from(kind)),
            None => Ok(()),
        }
    }
}

#[derive(Default)]
struct WriteGate {
    released: Mutex<bool>,
    release: Condvar,
}

struct GatedSink {
    receipt: Option<mpsc::Sender<usize>>,
    gate: Arc<WriteGate>,
}

impl Write for GatedSink {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if let Some(receipt) = self.receipt.take() {
            receipt
                .send(bytes.len())
                .expect("receipt receiver remains live");
        }
        let mut released = self.gate.released.lock().expect("gate lock");
        while !*released {
            released = self.gate.release.wait(released).expect("gate wait");
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn bounded_delivery_and_hung_sink() {
    let state = Arc::new(SinkState::default());
    let delivery =
        LineageDelivery::start(config(4_096, 4_096, 200), RecordingSink(Arc::clone(&state)));
    assert_eq!(
        delivery.try_emit(&event("accepted")),
        LineageAdmission::Accepted
    );
    let outcome = delivery.finish();
    assert_eq!(outcome.accepted(), 1);
    assert_eq!(outcome.dropped(), 0);
    assert_eq!(outcome.full(), 0);
    assert_eq!(outcome.terminal(), LineageDeliveryTerminal::Shutdown);
    assert!(state.bytes.lock().expect("bytes lock").ends_with(b"\n"));
    assert_eq!(*state.flushes.lock().expect("flush lock"), 1);

    let oversized = LineageDelivery::start(
        config(64, 64, 200),
        RecordingSink(Arc::new(SinkState::default())),
    );
    assert_eq!(
        oversized.try_emit(&event(&"x".repeat(256))),
        LineageAdmission::DroppedEventTooLarge
    );
    let outcome = oversized.finish();
    assert_eq!(
        (outcome.accepted(), outcome.dropped(), outcome.full()),
        (0, 1, 0)
    );
    assert_eq!(outcome.terminal(), LineageDeliveryTerminal::Shutdown);

    let (receipt_tx, receipt_rx) = mpsc::channel();
    let gate = Arc::new(WriteGate::default());
    let full = LineageDelivery::start(
        config(1_024, 1_024, 500),
        GatedSink {
            receipt: Some(receipt_tx),
            gate: Arc::clone(&gate),
        },
    );
    assert_eq!(
        full.try_emit(&event("queue-full")),
        LineageAdmission::Accepted
    );
    assert!(
        receipt_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("worker receives first event")
            > 0
    );
    let mut saw_full = false;
    for _ in 0..16 {
        if full.try_emit(&event("queue-full")) == LineageAdmission::DroppedQueueFull {
            saw_full = true;
            break;
        }
    }
    assert!(
        saw_full,
        "fixed byte capacity must deterministically drop newest"
    );
    *gate.released.lock().expect("gate lock") = true;
    gate.release.notify_all();
    let outcome = full.finish();
    assert!(outcome.accepted() >= 2);
    assert_eq!(outcome.dropped(), 1);
    assert_eq!(outcome.full(), 1);
    assert_eq!(outcome.terminal(), LineageDeliveryTerminal::Shutdown);

    let write_failed = LineageDelivery::start(
        config(4_096, 4_096, 200),
        FailingSink {
            write_kind: Some(io::ErrorKind::BrokenPipe),
            flush_kind: None,
        },
    );
    assert_eq!(
        write_failed.try_emit(&event("write-failed")),
        LineageAdmission::Accepted
    );
    assert_eq!(
        write_failed.finish().terminal(),
        LineageDeliveryTerminal::WriteFailed(io::ErrorKind::BrokenPipe)
    );

    let flush_failed = LineageDelivery::start(
        config(4_096, 4_096, 200),
        FailingSink {
            write_kind: None,
            flush_kind: Some(io::ErrorKind::WriteZero),
        },
    );
    assert_eq!(
        flush_failed.try_emit(&event("flush-failed")),
        LineageAdmission::Accepted
    );
    assert_eq!(
        flush_failed.finish().terminal(),
        LineageDeliveryTerminal::FlushFailed(io::ErrorKind::WriteZero)
    );

    let (receipt_tx, receipt_rx) = mpsc::channel();
    let hung_gate = Arc::new(WriteGate::default());
    let hung = LineageDelivery::start(
        config(4_096, 4_096, 40),
        GatedSink {
            receipt: Some(receipt_tx),
            gate: hung_gate,
        },
    );
    assert_eq!(hung.try_emit(&event("hung")), LineageAdmission::Accepted);
    assert!(
        receipt_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("hung sink signals byte receipt")
            > 0
    );
    let started = Instant::now();
    let outcome = hung.finish();
    assert_eq!(
        outcome.terminal(),
        LineageDeliveryTerminal::DeadlineExceeded
    );
    assert!(started.elapsed() >= Duration::from_millis(40));
    assert!(started.elapsed() < Duration::from_secs(1));
}

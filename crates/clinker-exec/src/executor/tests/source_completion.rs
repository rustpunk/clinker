//! Final report values follow actual worker-owned resource teardown.

use super::*;
use crate::pipeline::memory::{ConsumerHandle, MemoryArbitrator, NoOpPolicy};
use std::io::Write;
use std::time::Duration;

struct WorkerSpill {
    file: Option<tempfile::NamedTempFile>,
    memory: Arc<MemoryArbitrator>,
    bytes: u64,
}

impl Drop for WorkerSpill {
    fn drop(&mut self) {
        self.file.take().unwrap().close().unwrap();
        self.memory
            .release_spill_bytes("ordered-source", self.bytes);
    }
}

#[test]
fn source_completion_observes_worker_teardown_and_late_peak() {
    for failed_worker in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let memory = Arc::new(MemoryArbitrator::with_policy(
            1024,
            0.8,
            0.7,
            Box::new(NoOpPolicy),
        ));
        let mut file = tempfile::NamedTempFile::new_in(directory.path()).unwrap();
        file.write_all(b"spill retained by the unfinished source")
            .unwrap();
        file.flush().unwrap();
        let bytes = file.as_file().metadata().unwrap().len();
        assert!(bytes > 0);
        assert!(!memory.record_spill_bytes("ordered-source", bytes));
        let spill = WorkerSpill {
            file: Some(file),
            memory: memory.clone(),
            bytes,
        };
        let handle = ConsumerHandle::new();
        let consumer = memory.register_consumer(Arc::new(
            crate::executor::node_buffer::NodeBufferConsumer::new(handle.clone()),
        ));
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        let worker_memory = memory.clone();
        let worker = std::thread::spawn(move || {
            let owned_spill = spill;
            release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            // Work completed during join must contribute to the final peak.
            handle.set_bytes(512);
            worker_memory.sample_peak_consumer_usage();
            drop(owned_spill);
            handle.set_bytes(0);
            worker_memory.unregister_consumer(consumer);
            Ok(ingest::IngestTaskOutcome {
                source_name: "source".to_string(),
                total_count: 1,
                interrupted: true,
                watermark_observations: Vec::new(),
            })
        });
        let mut workers = Vec::new();
        if failed_worker {
            workers.push(std::thread::spawn(|| {
                Err(PipelineError::Io(std::io::Error::other("reader failure")))
            }));
        }
        workers.push(worker);

        assert_eq!(memory.cumulative_spill_bytes(), bytes);
        assert_eq!(memory.peak_consumer_usage(), 0);
        // The worker cannot release its spill or record its late peak until
        // this join begins. Sampling before it therefore fails deterministically.
        let completion = SourceCompletion::join(&memory, || {
            release_tx.try_send(()).unwrap();
            ingest::join_source_workers(workers, "source-completion-test")
        });
        assert_eq!(memory.cumulative_spill_bytes(), 0);
        assert_eq!(memory.consumer_count(), 0);
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
        if failed_worker {
            let error = completion.unwrap_err();
            assert!(
                matches!(error, PipelineError::Io(error) if error.to_string() == "reader failure")
            );
        } else {
            let completion = completion.unwrap();
            assert_eq!(completion.outcomes.len(), 1);
            assert_eq!(completion.outcomes[0].total_count, 1);
            assert!(completion.outcomes[0].interrupted);
            assert_eq!(completion.cumulative_spill_bytes, 0);
            assert_eq!(completion.per_stage_spill_bytes["ordered-source"], 0);
            assert_eq!(completion.peak_consumer_usage_bytes, 512);
        }
    }
}

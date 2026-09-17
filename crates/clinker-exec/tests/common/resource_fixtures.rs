//! Writer workspace allowance for low-memory spill fixtures.

use std::num::NonZeroUsize;
use std::sync::Arc;

use clinker_exec::executor::preparation::ExecutorResources;
use clinker_exec::pipeline::memory::{MemoryArbitrator, NoOpPolicy};
use clinker_exec::pipeline::shutdown::ShutdownToken;
use clinker_format::csv::writer::{CsvEncoder, CsvWriterConfig};
use clinker_record::Record;

/// Measure one real prepared CSV operation, then round its peak up to leave
/// room for the executor's factory and delivery wrappers. This is a fixture
/// allowance, not a runtime memory estimate. Callers retain their independent
/// operator budget and must still prove that their intended spill occurred.
pub fn csv_workspace_headroom(record: &Record) -> u64 {
    let arbitrator = Arc::new(MemoryArbitrator::with_policy(
        1024 * 1024,
        0.8,
        0.7,
        Box::new(NoOpPolicy),
    ));
    let provider = ExecutorResources::new(
        arbitrator.clone(),
        ShutdownToken::detached(),
        None,
        NonZeroUsize::new(1).unwrap(),
        None,
    )
    .expect("finite writer fixture provider");
    let resources = provider.resources();
    let mut writer = CsvEncoder::new(
        record.schema().clone(),
        &CsvWriterConfig::default(),
        resources.clone(),
    )
    .expect("admit CSV fixture encoder")
    .into_boxed_writer(std::io::sink(), resources)
    .expect("admit CSV fixture writer");
    writer.write_record(record).expect("encode fixture sample");
    writer.flush().expect("flush fixture sample");
    let peak = arbitrator.writer_resource_usage().peak_memory;
    assert!(peak > 0 && peak < 1024 * 1024);
    drop(writer);
    assert_eq!(arbitrator.writer_resource_usage().memory, 0);
    peak.next_power_of_two()
}

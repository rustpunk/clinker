//! Run-scoped ledger for destination-local output staging and publication.

use std::fs::File;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use clinker_plan::config::{ConfigError, IfExistsPolicy};
use clinker_plan::error::PipelineError;

use super::containment::StagedOutput;
use super::open::{containment_error, open_output};

#[derive(Debug)]
struct PendingOutput {
    name: String,
    final_path: PathBuf,
    staged: StagedOutput,
}

/// An operator-visible partial retained after an unsuccessful run.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartialOutput {
    /// Authored output node name.
    pub name: String,
    /// Final path that was never published.
    pub final_path: PathBuf,
    /// Hidden destination-local file containing the partial bytes.
    pub partial_path: PathBuf,
}

/// Shared pending-publication ledger used by single, fan-out, and split sinks.
///
/// Split writers may create files from executor-owned threads, so clones all
/// point to the same synchronized ledger. Publication remains owned by the CLI
/// after the executor has dropped every writer.
#[derive(Clone, Debug, Default)]
pub struct OutputStagingRegistry {
    pending: Arc<Mutex<Vec<PendingOutput>>>,
}

impl OutputStagingRegistry {
    /// Stage one resolved output without touching its final leaf.
    ///
    /// # Errors
    ///
    /// Returns configuration, confinement, collision, or I/O failures from
    /// output admission and destination-local hidden-file creation.
    pub fn stage_output<F>(
        &self,
        name: impl Into<String>,
        policy: IfExistsPolicy,
        cli_force: bool,
        path_for_n: F,
    ) -> Result<(PathBuf, File), PipelineError>
    where
        F: FnMut(Option<u64>) -> Result<PathBuf, ConfigError>,
    {
        let (final_path, file, staged) = open_output(policy, cli_force, path_for_n)?;
        self.pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(PendingOutput {
                name: name.into(),
                final_path: final_path.clone(),
                staged,
            });
        Ok((final_path, file))
    }

    /// Snapshot all hidden files that remain pending. This is used to report
    /// inspectable partials after a failed run without consuming the ledger.
    #[must_use]
    pub fn partials(&self) -> Vec<PartialOutput> {
        self.pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .map(|pending| PartialOutput {
                name: pending.name.clone(),
                final_path: pending.final_path.clone(),
                partial_path: pending.staged.partial_path().to_path_buf(),
            })
            .collect()
    }

    /// Publish every staged file after successful pipeline execution.
    ///
    /// # Errors
    ///
    /// Stops at the first failed promotion and returns its precise containment
    /// or durability diagnostic. Unattempted hidden files remain preserved.
    pub fn commit_all(&self) -> Result<(), PipelineError> {
        let pending = {
            let mut guard = self
                .pending
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            std::mem::take(&mut *guard)
        };
        let mut remaining = pending.into_iter();
        while let Some(pending) = remaining.next() {
            if let Err(error) = pending.staged.commit() {
                let mut guard = self
                    .pending
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                guard.extend(remaining);
                return Err(containment_error(error));
            }
        }
        Ok(())
    }
}

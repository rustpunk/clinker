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
    committed: Arc<Mutex<Vec<(String, PathBuf)>>>,
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

    /// Final paths successfully committed for one authored output name.
    #[must_use]
    pub fn committed_paths(&self, name: &str) -> Vec<PathBuf> {
        self.committed
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .filter(|(entry_name, _)| entry_name == name)
            .map(|(_, path)| path.clone())
            .collect()
    }

    /// Publish every staged file after successful pipeline execution.
    ///
    /// # Errors
    ///
    /// Stops at the first failed promotion and returns its precise containment
    /// or durability diagnostic. Unattempted hidden files remain preserved.
    pub fn commit_all(&self) -> Result<(), PipelineError> {
        self.commit_all_inner(None)
    }

    fn commit_all_inner(&self, fail_after_rename_at: Option<usize>) -> Result<(), PipelineError> {
        let mut pending = {
            let mut guard = self
                .pending
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            std::mem::take(&mut *guard)
        };
        for entry in &pending {
            if let Err(error) = entry.staged.preflight() {
                self.restore_pending(pending);
                return Err(containment_error(error));
            }
        }

        for index in 0..pending.len() {
            if let Err(error) = pending[index].staged.prepare_backup() {
                let rollback = rollback_all(&mut pending);
                self.restore_pending(pending);
                return Err(containment_error(with_rollback(error, rollback)));
            }
        }

        for index in 0..pending.len() {
            if let Err(error) = pending[index]
                .staged
                .publish(fail_after_rename_at == Some(index))
            {
                let rollback = rollback_all(&mut pending);
                self.restore_pending(pending);
                return Err(containment_error(with_rollback(error, rollback)));
            }
        }

        let committed: Vec<_> = pending
            .iter()
            .map(|entry| (entry.name.clone(), entry.final_path.clone()))
            .collect();
        let mut cleanup_error = None;
        for entry in &mut pending {
            if let Err(error) = entry.staged.finalize()
                && cleanup_error.is_none()
            {
                cleanup_error = Some(error);
            }
        }
        self.committed
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .extend(committed);
        cleanup_error.map_or(Ok(()), |error| Err(containment_error(error)))
    }

    fn restore_pending(&self, pending: Vec<PendingOutput>) {
        self.pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .extend(pending);
    }

    #[cfg(test)]
    fn commit_all_with_fault(&self, fail_after_rename_at: usize) -> Result<(), PipelineError> {
        self.commit_all_inner(Some(fail_after_rename_at))
    }

    /// Publish the ledger only for a complete, non-interrupted execution.
    ///
    /// Returns `false` without consuming any entry when shutdown interrupted
    /// the run, leaving every quarantine path available for inspection.
    ///
    /// # Errors
    ///
    /// Returns the same publication failures as [`Self::commit_all`].
    pub fn commit_all_if_complete(&self, interrupted: bool) -> Result<bool, PipelineError> {
        if interrupted {
            return Ok(false);
        }
        self.commit_all()?;
        Ok(true)
    }
}

fn rollback_all(pending: &mut [PendingOutput]) -> Option<super::containment::ContainmentError> {
    let mut first_error = None;
    for entry in pending.iter_mut().rev() {
        if let Err(error) = entry.staged.rollback()
            && first_error.is_none()
        {
            first_error = Some(error);
        }
    }
    first_error
}

fn with_rollback(
    publication: super::containment::ContainmentError,
    rollback: Option<super::containment::ContainmentError>,
) -> super::containment::ContainmentError {
    match rollback {
        None => publication,
        Some(rollback) => super::containment::ContainmentError::Io {
            operation: "rollback-publication-set",
            path: PathBuf::from("<multiple outputs>"),
            source: std::io::Error::other(format!(
                "publication failed: {publication}; rollback also failed: {rollback}"
            )),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use clinker_plan::config::IfExistsPolicy;

    use super::OutputStagingRegistry;

    #[test]
    fn interrupted_execution_keeps_existing_final_and_quarantine() {
        let root = tempfile::tempdir().expect("temporary output root");
        let final_path = root.path().join("result.csv");
        std::fs::write(&final_path, b"previous\n").expect("previous final");
        let registry = OutputStagingRegistry::default();
        let bare = final_path.clone();
        let (_, mut file) = registry
            .stage_output("out", IfExistsPolicy::Overwrite, false, move |_| {
                Ok(bare.clone())
            })
            .expect("stage replacement");
        file.write_all(b"new\n").expect("write quarantine");
        drop(file);

        assert!(!registry.commit_all_if_complete(true).expect("skip commit"));
        assert_eq!(
            std::fs::read(&final_path).expect("read existing final"),
            b"previous\n"
        );
        let partials = registry.partials();
        assert_eq!(partials.len(), 1);
        assert_eq!(
            std::fs::read(&partials[0].partial_path).expect("read quarantine"),
            b"new\n"
        );
    }

    #[test]
    fn second_promotion_failure_rolls_back_the_complete_output_set() {
        let root = tempfile::tempdir().expect("temporary output root");
        let first = root.path().join("first.csv");
        let second = root.path().join("second.csv");
        std::fs::write(&first, b"old first\n").expect("old first");
        std::fs::write(&second, b"old second\n").expect("old second");
        let registry = OutputStagingRegistry::default();

        for (name, path, body) in [
            ("first", first.clone(), b"new first\n".as_slice()),
            ("second", second.clone(), b"new second\n".as_slice()),
        ] {
            let (_, mut file) = registry
                .stage_output(name, IfExistsPolicy::Overwrite, false, move |_| {
                    Ok(path.clone())
                })
                .expect("stage output");
            file.write_all(body).expect("write quarantine");
        }

        let error = registry
            .commit_all_with_fault(1)
            .expect_err("second promotion must fail after rename");
        assert!(error.to_string().contains("visible"), "{error}");
        assert_eq!(std::fs::read(&first).expect("first final"), b"old first\n");
        assert_eq!(
            std::fs::read(&second).expect("second final"),
            b"old second\n"
        );
        let partials = registry.partials();
        assert_eq!(partials.len(), 2);
        assert_eq!(
            std::fs::read(&partials[0].partial_path).expect("first partial"),
            b"new first\n"
        );
        assert_eq!(
            std::fs::read(&partials[1].partial_path).expect("second partial"),
            b"new second\n"
        );
    }
}

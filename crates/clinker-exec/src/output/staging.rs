//! Run-scoped ledger for destination-local output staging and publication.

use std::collections::{BTreeMap, VecDeque};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use clinker_plan::config::{ConfigError, IfExistsPolicy, ResolvedPublicationPolicy};
use clinker_plan::error::PipelineError;
use clinker_plan::security::{ValidatedPath, check_overwrite, validate_path};

use super::attempt::{ArtifactKind, ArtifactRegistration, AttemptError, RunAttemptPublication};
use super::containment::{
    AttemptDestinationReservation, ContainmentError, PromotionDisposition, StagedOutput,
};
use super::open::{containment_error, open_output, open_output_with_policy};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AttemptCommitStage {
    Rename,
    ParentDirectorySynchronization,
}

#[derive(Debug)]
struct PendingOutput {
    name: String,
    final_path: PathBuf,
    staged: PendingStage,
}

#[derive(Debug)]
enum PendingStage {
    DestinationLocal(StagedOutput),
    AttemptOwned {
        reservation: AttemptDestinationReservation,
        source: ValidatedPath,
        source_path: PathBuf,
    },
}

impl PendingStage {
    fn partial_path(&self) -> &std::path::Path {
        match self {
            Self::DestinationLocal(staged) => staged.partial_path(),
            Self::AttemptOwned { source_path, .. } => source_path,
        }
    }

    fn preflight(&self) -> Result<(), ContainmentError> {
        match self {
            Self::DestinationLocal(staged) => staged.preflight(),
            Self::AttemptOwned { reservation, .. } => reservation.preflight(),
        }
    }

    fn publish(
        &mut self,
        fail_after_rename: bool,
        before_parent_directory_sync: Option<&mut dyn FnMut() -> std::io::Result<()>>,
    ) -> Result<(), ContainmentError> {
        match self {
            Self::DestinationLocal(staged) => {
                staged.publish_with_sync_barrier(fail_after_rename, before_parent_directory_sync)
            }
            Self::AttemptOwned {
                reservation,
                source,
                ..
            } => reservation.publish_from_with_sync_barrier(
                source.clone(),
                fail_after_rename,
                before_parent_directory_sync,
            ),
        }
    }

    fn finalize_with_cleanup_fault(&mut self, fail_cleanup: bool) -> Result<(), ContainmentError> {
        match self {
            Self::DestinationLocal(staged) => staged.finalize_with_cleanup_fault(fail_cleanup),
            Self::AttemptOwned { reservation, .. } => {
                reservation.finalize_with_cleanup_fault(fail_cleanup)
            }
        }
    }
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

/// A final destination that published but left a removable transaction file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CleanupDebt {
    pub name: String,
    pub final_path: PathBuf,
    pub stale_path: PathBuf,
    pub detail: String,
}

/// Truthful result of a run-scoped publication attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PublicationOutcome {
    Complete {
        published: Vec<(String, PathBuf)>,
        cleanup_debt: Vec<CleanupDebt>,
    },
    Incomplete {
        published: Vec<(String, PathBuf)>,
        visible_unsynchronized: Vec<(String, PathBuf)>,
        unpublished: Vec<PartialOutput>,
        cleanup_debt: Vec<CleanupDebt>,
        error: String,
    },
}

impl PublicationOutcome {
    #[must_use]
    pub fn is_complete(&self) -> bool {
        matches!(self, Self::Complete { .. })
    }

    #[must_use]
    pub fn cleanup_debt(&self) -> &[CleanupDebt] {
        match self {
            Self::Complete { cleanup_debt, .. } | Self::Incomplete { cleanup_debt, .. } => {
                cleanup_debt
            }
        }
    }
}

/// How durable a recorded promotion is for one final path.
///
/// A rename that landed without its parent-directory synchronization is
/// observable now but is not guaranteed to survive host power loss, so the
/// ledger keeps the two apart instead of calling both "committed".
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommittedVisibility {
    Published,
    VisibleUnsynchronized,
}

#[derive(Debug, Default)]
struct RegistryState {
    pending: Vec<PendingOutput>,
    committed: Vec<(String, PathBuf, CommittedVisibility)>,
    cleanup_debt: Vec<CleanupDebt>,
    claims: BTreeMap<String, (String, PathBuf)>,
}

/// Shared pending-publication ledger used by single, fan-out, and split sinks.
///
/// Split writers may create files from executor-owned threads, so clones all
/// point to the same synchronized ledger. Publication remains owned by the CLI
/// after the executor has dropped every writer.
#[derive(Clone, Debug)]
pub struct OutputStagingRegistry {
    state: Arc<Mutex<RegistryState>>,
    attempt: Option<RunAttemptPublication>,
}

impl Default for OutputStagingRegistry {
    fn default() -> Self {
        Self {
            state: Arc::new(Mutex::new(RegistryState::default())),
            attempt: None,
        }
    }
}

impl OutputStagingRegistry {
    /// Attach the shared output ledger to one run-owned publication attempt.
    pub fn for_run_attempt(attempt: RunAttemptPublication) -> Self {
        Self {
            state: Arc::new(Mutex::new(RegistryState::default())),
            attempt: Some(attempt),
        }
    }

    /// Whether dynamic artifacts are owned by a run attempt.
    pub(crate) fn has_run_attempt(&self) -> bool {
        self.attempt.is_some()
    }

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
        self.stage_output_inner(None, name.into(), policy, cli_force, path_for_n)
    }

    /// Stage one resolved output through an already validated publication
    /// policy.
    ///
    /// This typed entry point replaces detected-filesystem routing for the
    /// run-owned attempt path while preserving the shared collision ledger.
    pub fn stage_output_with_policy<F>(
        &self,
        publication: &ResolvedPublicationPolicy,
        name: impl Into<String>,
        policy: IfExistsPolicy,
        cli_force: bool,
        path_for_n: F,
    ) -> Result<(PathBuf, File), PipelineError>
    where
        F: FnMut(Option<u64>) -> Result<PathBuf, ConfigError>,
    {
        self.stage_output_inner(
            Some(publication),
            name.into(),
            policy,
            cli_force,
            path_for_n,
        )
    }

    /// Stage one dynamically resolved artifact through this run's owned
    /// attempt and the ordinary collision-policy vocabulary.
    pub fn stage_attempt_output<F>(
        &self,
        kind: ArtifactKind,
        name: impl Into<String>,
        policy: IfExistsPolicy,
        cli_force: bool,
        mut path_for_n: F,
    ) -> Result<(PathBuf, File), PipelineError>
    where
        F: FnMut(Option<u64>) -> Result<PathBuf, ConfigError>,
    {
        let name = name.into();
        let bare = path_for_n(None).map_err(PipelineError::Config)?;
        let disposition = match policy {
            IfExistsPolicy::Overwrite => PromotionDisposition::Replace,
            IfExistsPolicy::Error if cli_force => PromotionDisposition::Replace,
            IfExistsPolicy::Error | IfExistsPolicy::UniqueSuffix => PromotionDisposition::NoReplace,
        };
        let stage = |path: PathBuf| self.stage_attempt_candidate(kind, &name, disposition, path);
        match policy {
            IfExistsPolicy::Overwrite => stage(bare),
            IfExistsPolicy::Error => match stage(bare.clone()) {
                Err(error) if !cli_force && attempt_is_already_exists(&error) => {
                    Err(attempt_existing_output_error(&bare))
                }
                result => result,
            },
            IfExistsPolicy::UniqueSuffix => {
                // The same search the non-attempt path uses, and shared rather
                // than copied: this is the path every CLI output takes, and it
                // was left behind when the other one learned that Windows
                // reports a contended name as a sharing violation. A test
                // exercising only the other copy reported the policy fixed
                // while the live path still failed the run.
                let mut search = super::open::SuffixSearch::default();
                // A name another output in this same run has already claimed
                // is a taken candidate too, and it arrives as a validation
                // error rather than an I/O one. Without this the attempt path
                // aborted on the first collision while the non-attempt path
                // wrote `out-1.json` and succeeded — the same authored YAML
                // with opposite outcomes depending on whether a run attempt
                // happened to be active.
                let mut advance = |error: &PipelineError| {
                    if is_intra_run_claim_collision(error) {
                        return search.advance_past_taken_name();
                    }
                    search.advance(error)
                };
                match stage(bare.clone()) {
                    Ok(output) => return Ok(output),
                    Err(error) if advance(&error) => {}
                    Err(error) => return Err(error),
                }
                for n in 1_u64..=u64::MAX {
                    let candidate = path_for_n(Some(n)).map_err(PipelineError::Config)?;
                    match stage(candidate) {
                        Ok(output) => return Ok(output),
                        Err(error) if advance(&error) => continue,
                        Err(error) => return Err(error),
                    }
                }
                Err(PipelineError::Io(std::io::Error::other(
                    "exhausted u64 collision counter for unique_suffix policy",
                )))
            }
        }
    }

    fn stage_attempt_candidate(
        &self,
        kind: ArtifactKind,
        name: &str,
        disposition: PromotionDisposition,
        final_path: PathBuf,
    ) -> Result<(PathBuf, File), PipelineError> {
        let attempt = self
            .attempt
            .as_ref()
            .ok_or_else(|| PipelineError::Internal {
                op: "attempt-publication",
                node: name.to_owned(),
                detail: "attempt-owned output registry has no run attempt".to_owned(),
            })?;
        let base = std::env::current_dir().map_err(PipelineError::Io)?;
        let destination =
            validate_path(&final_path, &base, final_path.is_absolute()).map_err(|diagnostic| {
                PipelineError::Config(ConfigError::Validation(format!(
                    "{}: {}",
                    diagnostic.code, diagnostic.message
                )))
            })?;
        let logical_leaf = final_path
            .file_name()
            .and_then(|leaf| leaf.to_str())
            .ok_or_else(|| {
                PipelineError::Config(ConfigError::Validation(
                    "output artifact leaf must be valid UTF-8".to_owned(),
                ))
            })?;
        let registration =
            ArtifactRegistration::new(kind, name, logical_leaf, destination, disposition)
                .map_err(attempt_error_to_pipeline)?;
        let writer = attempt
            .stage(self, registration)
            .map_err(attempt_error_to_pipeline)?;
        Ok((final_path, writer.into_file()))
    }

    fn stage_output_inner<F>(
        &self,
        publication: Option<&ResolvedPublicationPolicy>,
        name: String,
        policy: IfExistsPolicy,
        cli_force: bool,
        mut path_for_n: F,
    ) -> Result<(PathBuf, File), PipelineError>
    where
        F: FnMut(Option<u64>) -> Result<PathBuf, ConfigError>,
    {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // Exact-path policies can collide with an entry already staged by this
        // run. Detect that in the shared ledger before the filesystem
        // reservation rejects the second opener, so the diagnostic retains
        // both authored producer names. Unique-suffix deliberately probes the
        // next candidate instead and is checked after selection.
        let bare = if policy == IfExistsPolicy::UniqueSuffix {
            None
        } else {
            let bare = path_for_n(None).map_err(PipelineError::Config)?;
            let key = destination_key(&bare)?;
            if let Some((first_name, first_path)) = state.claims.get(&key) {
                return Err(collision_error(&name, &bare, first_name, first_path));
            }
            Some(bare)
        };
        let (final_path, file, staged) = if let Some(bare) = bare {
            if let Some(publication) = publication {
                open_output_with_policy(publication, policy, cli_force, |n| match n {
                    None => Ok(bare.clone()),
                    Some(n) => path_for_n(Some(n)),
                })?
            } else {
                open_output(policy, cli_force, |n| match n {
                    None => Ok(bare.clone()),
                    Some(n) => path_for_n(Some(n)),
                })?
            }
        } else if let Some(publication) = publication {
            open_output_with_policy(publication, policy, cli_force, path_for_n)?
        } else {
            open_output(policy, cli_force, path_for_n)?
        };
        let key = destination_key(&final_path)?;
        if let Some((first_name, first_path)) = state.claims.get(&key) {
            drop(file);
            let _ = staged.discard();
            return Err(collision_error(&name, &final_path, first_name, first_path));
        }
        state.claims.insert(key, (name.clone(), final_path.clone()));
        state.pending.push(PendingOutput {
            name,
            final_path: final_path.clone(),
            staged: PendingStage::DestinationLocal(staged),
        });
        Ok((final_path, file))
    }

    /// Register an attempt-owned artifact for the ordinary publication gate.
    pub(crate) fn register_attempt_output(
        &self,
        name: String,
        final_path: PathBuf,
        reservation: AttemptDestinationReservation,
        source: ValidatedPath,
    ) -> Result<(), PipelineError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let key = destination_key(&final_path)?;
        if let Some((first_name, first_path)) = state.claims.get(&key) {
            return Err(collision_error(&name, &final_path, first_name, first_path));
        }
        state.claims.insert(key, (name.clone(), final_path.clone()));
        let source_path = source.as_path().to_path_buf();
        state.pending.push(PendingOutput {
            name,
            final_path,
            staged: PendingStage::AttemptOwned {
                reservation,
                source,
                source_path,
            },
        });
        Ok(())
    }

    pub(crate) fn ensure_destination_available(
        &self,
        name: &str,
        final_path: &std::path::Path,
    ) -> Result<(), PipelineError> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let key = destination_key(final_path)?;
        if let Some((first_name, first_path)) = state.claims.get(&key) {
            return Err(collision_error(name, final_path, first_name, first_path));
        }
        Ok(())
    }

    /// Snapshot all hidden files that remain pending. This is used to report
    /// inspectable partials after a failed run without consuming the ledger.
    #[must_use]
    pub fn partials(&self) -> Vec<PartialOutput> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .pending
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
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .committed
            .iter()
            .filter(|(entry_name, _, _)| entry_name == name)
            .map(|(_, path, _)| path.clone())
            .collect()
    }

    /// Snapshot every staged final path, including split and fan-out segments.
    #[must_use]
    pub fn pending_paths(&self, name: &str) -> Vec<PathBuf> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .pending
            .iter()
            .filter(|entry| entry.name == name)
            .map(|entry| entry.final_path.clone())
            .collect()
    }

    /// How the publication ledger recorded this exact final path, if at all.
    /// The attempt layer uses this only when publication itself errors before
    /// it can return a normal [`PublicationOutcome`], so the failure still
    /// carries truthful per-artifact visibility — including the difference
    /// between a durable promotion and one whose parent-directory
    /// synchronization never completed.
    pub(crate) fn committed_visibility(&self, path: &Path) -> Option<CommittedVisibility> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .committed
            .iter()
            .find(|(_, committed, _)| committed == path)
            .map(|(_, _, visibility)| *visibility)
    }

    /// Number of cleanup-debt entries this ledger observed during publication.
    ///
    /// A successful call returns its debt inside [`PublicationOutcome`]; this
    /// accessor serves the attempt layer's error path, where the outcome never
    /// reaches the caller and the debt would otherwise be reported as zero
    /// while the orphaned staged files remain on disk.
    pub(crate) fn recorded_cleanup_debt_count(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .cleanup_debt
            .len()
    }

    /// Publish every staged file after successful pipeline execution.
    ///
    /// # Errors
    ///
    /// Stops at the first failed promotion and reports every visible,
    /// unsynchronized, and unpublished path without attempting set rollback.
    pub fn commit_all(&self) -> Result<PublicationOutcome, PipelineError> {
        self.commit_all_inner(None, None, None, None)
    }

    pub(crate) fn commit_all_with_stage_control(
        &self,
        control: &mut dyn FnMut(usize, AttemptCommitStage) -> std::io::Result<()>,
    ) -> Result<PublicationOutcome, PipelineError> {
        self.commit_all_inner(None, None, None, Some(control))
    }

    pub(crate) fn commit_all_inner(
        &self,
        fail_before_rename_at: Option<usize>,
        fail_after_rename_at: Option<usize>,
        fail_cleanup_at: Option<usize>,
        mut control: Option<&mut dyn FnMut(usize, AttemptCommitStage) -> std::io::Result<()>>,
    ) -> Result<PublicationOutcome, PipelineError> {
        let pending = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            std::mem::take(&mut state.pending)
        };
        for entry in &pending {
            if let Err(error) = entry.staged.preflight() {
                self.restore_pending(pending);
                return Err(containment_error(error));
            }
        }
        let mut remaining = VecDeque::from(pending);
        let mut published = Vec::new();
        let mut visible_unsynchronized = Vec::new();
        let mut cleanup_debt = Vec::new();
        let mut index = 0_usize;
        while let Some(mut entry) = remaining.pop_front() {
            if let Some(control) = control.as_deref_mut()
                && let Err(error) = control(index, AttemptCommitStage::Rename)
            {
                remaining.push_front(entry);
                self.restore_pending(remaining.into());
                // This error path returns no outcome, so the ledger is the only
                // surviving record of debt already owed by earlier promotions.
                self.record_publication_progress(
                    &published,
                    &visible_unsynchronized,
                    &cleanup_debt,
                );
                return Err(PipelineError::Io(error));
            }
            if fail_before_rename_at == Some(index) {
                remaining.push_front(entry);
                let unpublished = partials_from(remaining.iter());
                self.restore_pending(remaining.into());
                self.record_publication_progress(
                    &published,
                    &visible_unsynchronized,
                    &cleanup_debt,
                );
                return Ok(PublicationOutcome::Incomplete {
                    published,
                    visible_unsynchronized,
                    unpublished,
                    cleanup_debt,
                    error: "injected failure before destination rename".to_owned(),
                });
            }
            let mut before_parent_directory_sync = || match control.as_deref_mut() {
                Some(control) => control(index, AttemptCommitStage::ParentDirectorySynchronization),
                None => Ok(()),
            };
            match entry.staged.publish(
                fail_after_rename_at == Some(index),
                Some(&mut before_parent_directory_sync),
            ) {
                Ok(()) => {
                    let identity = (entry.name.clone(), entry.final_path.clone());
                    if let Err(error) = entry
                        .staged
                        .finalize_with_cleanup_fault(fail_cleanup_at == Some(index))
                    {
                        cleanup_debt.push(cleanup_debt_for(&entry, error));
                    }
                    published.push(identity);
                }
                Err(error @ ContainmentError::VisibleButUnsynced { .. }) => {
                    let identity = (entry.name.clone(), entry.final_path.clone());
                    if let Err(cleanup) = entry
                        .staged
                        .finalize_with_cleanup_fault(fail_cleanup_at == Some(index))
                    {
                        cleanup_debt.push(cleanup_debt_for(&entry, cleanup));
                    }
                    visible_unsynchronized.push(identity);
                    let unpublished = partials_from(remaining.iter());
                    self.restore_pending(remaining.into());
                    self.record_publication_progress(
                        &published,
                        &visible_unsynchronized,
                        &cleanup_debt,
                    );
                    return Ok(PublicationOutcome::Incomplete {
                        published,
                        visible_unsynchronized,
                        unpublished,
                        cleanup_debt,
                        error: error.to_string(),
                    });
                }
                Err(error) => {
                    remaining.push_front(entry);
                    let unpublished = partials_from(remaining.iter());
                    self.restore_pending(remaining.into());
                    self.record_publication_progress(
                        &published,
                        &visible_unsynchronized,
                        &cleanup_debt,
                    );
                    return Ok(PublicationOutcome::Incomplete {
                        published,
                        visible_unsynchronized,
                        unpublished,
                        cleanup_debt,
                        error: error.to_string(),
                    });
                }
            }
            index += 1;
        }
        self.record_publication_progress(&published, &[], &cleanup_debt);
        Ok(PublicationOutcome::Complete {
            published,
            cleanup_debt,
        })
    }

    fn restore_pending(&self, pending: Vec<PendingOutput>) {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .pending
            .extend(pending);
    }

    fn record_publication_progress(
        &self,
        published: &[(String, PathBuf)],
        visible_unsynchronized: &[(String, PathBuf)],
        cleanup_debt: &[CleanupDebt],
    ) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.committed.extend(
            published
                .iter()
                .map(|(name, path)| (name.clone(), path.clone(), CommittedVisibility::Published)),
        );
        state
            .committed
            .extend(visible_unsynchronized.iter().map(|(name, path)| {
                (
                    name.clone(),
                    path.clone(),
                    CommittedVisibility::VisibleUnsynchronized,
                )
            }));
        state.cleanup_debt.extend_from_slice(cleanup_debt);
    }

    #[cfg(test)]
    fn commit_all_with_fault(
        &self,
        fail_after_rename_at: usize,
    ) -> Result<PublicationOutcome, PipelineError> {
        self.commit_all_inner(None, Some(fail_after_rename_at), None, None)
    }

    #[cfg(test)]
    fn commit_all_with_pre_rename_fault(
        &self,
        fail_before_rename_at: usize,
    ) -> Result<PublicationOutcome, PipelineError> {
        self.commit_all_inner(Some(fail_before_rename_at), None, None, None)
    }

    #[cfg(test)]
    fn commit_all_with_cleanup_fault(
        &self,
        fail_cleanup_at: usize,
    ) -> Result<PublicationOutcome, PipelineError> {
        self.commit_all_inner(None, None, Some(fail_cleanup_at), None)
    }

    /// Publish the ledger only for a complete, non-interrupted execution.
    ///
    /// Returns `None` without consuming any entry when shutdown interrupted
    /// the run, leaving every quarantine path available for inspection.
    ///
    /// # Errors
    ///
    /// Returns the same publication failures as [`Self::commit_all`].
    pub fn commit_all_if_complete(
        &self,
        interrupted: bool,
    ) -> Result<Option<PublicationOutcome>, PipelineError> {
        if interrupted {
            return Ok(None);
        }
        self.commit_all().map(Some)
    }
}

/// Whether this candidate name is already claimed by another output of the
/// same run, which the registry reports as a validation error rather than an
/// I/O one because no file was involved.
fn is_intra_run_claim_collision(error: &PipelineError) -> bool {
    matches!(
        error,
        PipelineError::Config(ConfigError::Validation(message))
            if message.starts_with("output destination collision:")
    )
}

fn collision_error(
    name: &str,
    final_path: &std::path::Path,
    first_name: &str,
    first_path: &std::path::Path,
) -> PipelineError {
    PipelineError::Config(ConfigError::Validation(format!(
        "output destination collision: producer {name:?} resolves to {} already claimed by producer {first_name:?} at {}",
        final_path.display(),
        first_path.display(),
    )))
}

fn destination_key(path: &std::path::Path) -> Result<String, PipelineError> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(PipelineError::Io)?
            .join(path)
    };
    Ok(clinker_plan::config::collision_key(
        &absolute.to_string_lossy(),
    ))
}

fn attempt_error_to_pipeline(error: AttemptError) -> PipelineError {
    match error {
        AttemptError::Containment(error) => containment_error(error),
        AttemptError::Pipeline(error) => error,
        other => PipelineError::Config(ConfigError::Validation(other.to_string())),
    }
}

fn attempt_is_already_exists(error: &PipelineError) -> bool {
    matches!(error, PipelineError::Io(source) if source.kind() == std::io::ErrorKind::AlreadyExists)
}

fn attempt_existing_output_error(path: &Path) -> PipelineError {
    let detail = match check_overwrite(path) {
        Err(diagnostic) => diagnostic.message,
        Ok(()) => format!(
            "output file already exists: {path:?} — use --force or set if_exists: overwrite"
        ),
    };
    PipelineError::Config(ConfigError::Validation(format!("E-SEC-001: {detail}")))
}

fn partials_from<'a>(entries: impl Iterator<Item = &'a PendingOutput>) -> Vec<PartialOutput> {
    entries
        .map(|entry| PartialOutput {
            name: entry.name.clone(),
            final_path: entry.final_path.clone(),
            partial_path: entry.staged.partial_path().to_path_buf(),
        })
        .collect()
}

fn cleanup_debt_for(entry: &PendingOutput, error: ContainmentError) -> CleanupDebt {
    match error {
        ContainmentError::PublishedCleanup {
            stale_path, source, ..
        } => CleanupDebt {
            name: entry.name.clone(),
            final_path: entry.final_path.clone(),
            stale_path,
            detail: source.to_string(),
        },
        other => CleanupDebt {
            name: entry.name.clone(),
            final_path: entry.final_path.clone(),
            stale_path: entry.final_path.clone(),
            detail: other.to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use clinker_plan::config::IfExistsPolicy;

    use super::{OutputStagingRegistry, PublicationOutcome};

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

        assert!(
            registry
                .commit_all_if_complete(true)
                .expect("skip commit")
                .is_none()
        );
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
    fn second_post_rename_failure_reports_the_exact_visible_set() {
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

        let outcome = registry
            .commit_all_with_fault(1)
            .expect("publication outcome");
        let PublicationOutcome::Incomplete {
            published,
            visible_unsynchronized,
            unpublished,
            error,
            ..
        } = outcome
        else {
            panic!("fault must report an incomplete publication");
        };
        assert!(error.contains("visible"), "{error}");
        assert_eq!(published, vec![("first".to_owned(), first.clone())]);
        assert_eq!(
            visible_unsynchronized,
            vec![("second".to_owned(), second.clone())]
        );
        assert!(unpublished.is_empty());
        assert_eq!(std::fs::read(&first).expect("first final"), b"new first\n");
        assert_eq!(
            std::fs::read(&second).expect("second final"),
            b"new second\n"
        );
        assert!(registry.partials().is_empty());
    }

    #[test]
    fn failure_between_promotions_preserves_unvisited_final_and_can_resume() {
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

        let outcome = registry
            .commit_all_with_pre_rename_fault(1)
            .expect("typed partial outcome");
        let PublicationOutcome::Incomplete {
            published,
            visible_unsynchronized,
            unpublished,
            ..
        } = outcome
        else {
            panic!("injected boundary fault must be incomplete");
        };
        assert_eq!(published, vec![("first".to_owned(), first.clone())]);
        assert!(visible_unsynchronized.is_empty());
        assert_eq!(unpublished.len(), 1);
        assert_eq!(unpublished[0].final_path, second);
        assert_eq!(std::fs::read(&first).unwrap(), b"new first\n");
        assert_eq!(std::fs::read(&second).unwrap(), b"old second\n");

        let resumed = registry.commit_all().expect("resume remaining publication");
        assert!(resumed.is_complete());
        assert_eq!(std::fs::read(&first).unwrap(), b"new first\n");
        assert_eq!(std::fs::read(&second).unwrap(), b"new second\n");
    }

    #[test]
    fn global_ledger_rejects_collisions_with_both_producer_names() {
        let root = tempfile::tempdir().expect("temporary output root");
        let final_path = root.path().join("shared.csv");
        let registry = OutputStagingRegistry::default();
        let first = final_path.clone();
        registry
            .stage_output("primary", IfExistsPolicy::Overwrite, false, move |_| {
                Ok(first.clone())
            })
            .expect("first claim");
        let second = final_path.clone();
        let error = registry
            .stage_output(
                "metadata sidecar",
                IfExistsPolicy::Overwrite,
                false,
                move |_| Ok(second.clone()),
            )
            .expect_err("duplicate destination must fail");
        let rendered = error.to_string();
        assert!(rendered.contains("primary"), "{rendered}");
        assert!(rendered.contains("metadata sidecar"), "{rendered}");
    }

    #[test]
    fn post_publication_cleanup_failure_is_reported_as_debt() {
        let root = tempfile::tempdir().expect("temporary output root");
        let final_path = root.path().join("result.csv");
        let registry = OutputStagingRegistry::default();
        let bare = final_path.clone();
        let (_, mut file) = registry
            .stage_output("out", IfExistsPolicy::Overwrite, false, move |_| {
                Ok(bare.clone())
            })
            .expect("stage output");
        file.write_all(b"published\n").expect("write quarantine");
        drop(file);

        let outcome = registry
            .commit_all_with_cleanup_fault(0)
            .expect("typed publication outcome");
        let PublicationOutcome::Complete {
            published,
            cleanup_debt,
        } = outcome
        else {
            panic!("cleanup failure occurs after successful publication");
        };
        assert_eq!(published, vec![("out".to_owned(), final_path.clone())]);
        assert_eq!(cleanup_debt.len(), 1);
        assert_eq!(cleanup_debt[0].final_path, final_path);
        assert!(cleanup_debt[0].stale_path.exists());
        assert_eq!(
            std::fs::read(&cleanup_debt[0].final_path).expect("published final"),
            b"published\n"
        );
    }
}

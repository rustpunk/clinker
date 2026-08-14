//! Stable typed command tree and exit/output contract.

use std::ffi::OsString;
use std::io::{self, Write};
use std::path::PathBuf;
use std::process::ExitCode;

use clap::error::ErrorKind;
use clap::{ArgAction, Args, Parser, Subcommand, ValueEnum};

use crate::bundle::{self, AssemblyRequest, BuildRequest};
use crate::decision::{self, DecisionRequest};
use crate::error::{GateError, sanitize};
use crate::evidence;
use crate::filesystem::{
    self, ProvisionRequest, RunProfileRequest, SelfTestRequest, TeardownRequest,
};
use crate::inventory;
use crate::limits::{MAX_DECISION_RECORDS, MAX_DIAGNOSTIC_BYTES};
use crate::recovery;
use crate::release::{self, CandidateRequest};

#[path = "boundary.rs"]
mod boundary;
#[path = "gate.rs"]
mod gate;
#[doc(hidden)]
pub use gate::apply_nofile_floor_with;
#[path = "github.rs"]
pub mod github;
#[path = "publication.rs"]
pub mod publication;
#[path = "repository.rs"]
mod repository;
#[path = "workflow.rs"]
mod workflow;

/// Execute the CLI from an explicit argv iterator.
pub fn run_from<I, T>(arguments: I) -> ExitCode
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let cli = match Cli::try_parse_from(arguments) {
        Ok(cli) => cli,
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::DisplayHelp | ErrorKind::DisplayVersion
            ) =>
        {
            let _ = io::stdout().write_all(error.to_string().as_bytes());
            return ExitCode::SUCCESS;
        }
        Err(error) => {
            write_error(&GateError::usage(error.to_string()));
            return ExitCode::from(2);
        }
    };

    match execute(cli) {
        Ok(message) => {
            let mut stdout = io::stdout().lock();
            if stdout.write_all(message.as_bytes()).is_err() || stdout.flush().is_err() {
                return ExitCode::from(2);
            }
            ExitCode::SUCCESS
        }
        Err(error) => {
            let code = error.class().code();
            write_error(&error);
            ExitCode::from(code)
        }
    }
}

fn write_error(error: &GateError) {
    let diagnostic = format!("error: clinker-release-policy: {}\n", error.diagnostic());
    let diagnostic = sanitize(&diagnostic, MAX_DIAGNOSTIC_BYTES + 64);
    let _ = io::stderr().write_all(diagnostic.as_bytes());
}

fn execute(cli: Cli) -> Result<String, GateError> {
    match cli.domain {
        Domain::Decision(DecisionDomain {
            operation: DecisionOperation::Validate(arguments),
        }) => {
            let request = arguments.preflight()?;
            decision::validate(&request)?;
            Ok("release decision validation passed\n".to_owned())
        }
        Domain::Decision(DecisionDomain {
            operation: DecisionOperation::VerifyPhase4Capabilities(arguments),
        }) => {
            decision::verify_phase4_capabilities(&arguments.workspace_root)?;
            Ok("Phase 4 dependency capabilities verified\n".to_owned())
        }
        Domain::Evidence(EvidenceDomain {
            operation: EvidenceOperation::Validate(arguments),
        }) => {
            let kind = match arguments.kind {
                EvidenceKind::Candidate => evidence::EvidenceKind::Candidate,
                EvidenceKind::Publication => evidence::EvidenceKind::Publication,
            };
            publication::validate_evidence_file(kind, &arguments.schema, &arguments.manifest)?;
            Ok("release evidence validation passed\n".to_owned())
        }
        Domain::Evidence(EvidenceDomain {
            operation: EvidenceOperation::AssertComplete(arguments),
        }) => {
            gate::assert_complete(&arguments.manifest)?;
            Ok("Release completion evidence verified\n".to_owned())
        }
        Domain::Inventory(InventoryDomain {
            operation: InventoryOperation::Check(arguments),
        }) => {
            let repo_root =
                arguments
                    .repo_root
                    .unwrap_or(std::env::current_dir().map_err(|error| {
                        GateError::io("resolve current repository directory", &error)
                    })?);
            let (_, inventory) = inventory::load(&repo_root, arguments.inventory.as_deref())?;
            if arguments.print_json {
                inventory::render_json(&inventory)
            } else {
                Ok(format!(
                    "Release inventory valid: {}, {} targets.\n",
                    inventory.version,
                    inventory.targets.len()
                ))
            }
        }
        Domain::Filesystem(FilesystemDomain {
            operation: FilesystemOperation::ProvisionAndRun(arguments),
        }) => filesystem::provision_and_run(&ProvisionRequest {
            profile: arguments.profile,
            evidence: arguments.evidence,
        }),
        Domain::Filesystem(FilesystemDomain {
            operation: FilesystemOperation::RunProfile(arguments),
        }) => filesystem::run_profile(&RunProfileRequest {
            profile: arguments.profile,
            mount_root: arguments.mount_root,
            evidence: arguments.evidence,
            package_observations: arguments.package_observations,
            protocol_observations: arguments.protocol_observations,
        }),
        Domain::Filesystem(FilesystemDomain {
            operation: FilesystemOperation::Teardown(arguments),
        }) => filesystem::teardown(&TeardownRequest {
            profile: arguments.profile,
            evidence: arguments.evidence,
        }),
        Domain::Filesystem(FilesystemDomain {
            operation: FilesystemOperation::SelfTest(arguments),
        }) => filesystem::self_test(&SelfTestRequest {
            workflow: arguments.workflow,
        }),
        Domain::Release(ReleaseDomain {
            operation: ReleaseOperation::BuildBundle(arguments),
        }) => {
            let repo_root = std::env::current_dir()
                .map_err(|error| GateError::io("resolve current repository directory", &error))?;
            let (paths, inventory) = inventory::load(&repo_root, None)?;
            bundle::build(
                &paths.repo_root,
                &inventory,
                &BuildRequest {
                    target: arguments.target,
                    source_sha: arguments.source_sha,
                    output_dir: arguments.output_dir,
                },
            )
        }
        Domain::Release(ReleaseDomain {
            operation: ReleaseOperation::Verify(arguments),
        }) => match (*arguments).preflight()? {
            VerifyRequest::Assembly(request) => {
                let repo_root = std::env::current_dir().map_err(|error| {
                    GateError::io("resolve current repository directory", &error)
                })?;
                let (_, inventory) = inventory::load(&repo_root, None)?;
                bundle::verify_assembly(&inventory, &request)
            }
            VerifyRequest::Candidate(request) => {
                let repo_root = std::env::current_dir().map_err(|error| {
                    GateError::io("resolve current repository directory", &error)
                })?;
                release::verify_candidate(&repo_root, &request)
            }
        },
        Domain::Release(ReleaseDomain {
            operation: ReleaseOperation::StageCandidateDraft(arguments),
        }) => {
            let repo_root = std::env::current_dir()
                .map_err(|error| GateError::io("resolve current repository directory", &error))?;
            let mut transport = github::ChildGitHubTransport::from_environment();
            release::stage_candidate_draft(&repo_root, &arguments.into_request()?, &mut transport)
        }
        Domain::Workflow(WorkflowDomain {
            operation: WorkflowOperation::Verify,
        }) => {
            let repo_root = std::env::current_dir()
                .map_err(|error| GateError::io("resolve current repository directory", &error))?;
            workflow::verify(&repo_root)?;
            Ok("Release workflow trust verification passed\n".to_owned())
        }
        Domain::Repository(RepositoryDomain {
            operation: RepositoryOperation::Verify(arguments),
        }) => {
            let repo_root = std::env::current_dir()
                .map_err(|error| GateError::io("resolve current repository directory", &error))?;
            match arguments.preflight()? {
                RepositoryVerifyRequest::ConfigOnly => {
                    repository::verify_configuration(&repo_root)?;
                    Ok("Release repository configuration verification passed\n".to_owned())
                }
                RepositoryVerifyRequest::Readback { repository } => {
                    repository::verify_readback(&repo_root, &repository)?;
                    Ok("Release repository controls verified\n".to_owned())
                }
                RepositoryVerifyRequest::Apply(request) => {
                    repository::apply_and_verify(&repo_root, &request)?;
                    Ok("Release repository controls applied and verified\n".to_owned())
                }
            }
        }
        Domain::Gate(GateDomain {
            operation: GateOperation::Run(arguments),
        }) => match arguments.preflight()? {
            GateRunRequest::PreCandidate(request) => {
                gate::run_pre_candidate(&request)?;
                Ok("Pre-candidate release policy passed with incomplete evidence\n".to_owned())
            }
            GateRunRequest::Final(request) => {
                gate::run_final(&request)?;
                Ok("Final release evidence reconciliation passed\n".to_owned())
            }
        },
        Domain::Boundary(BoundaryDomain {
            operation: BoundaryOperation::Audit(arguments),
        }) => {
            boundary::audit(arguments.scope.into(), &arguments.root)?;
            let scope = match arguments.scope {
                BoundaryScope::Dependency => "dependency",
                BoundaryScope::RustOnly => "Rust-only",
            };
            Ok(format!("{scope} boundary audit passed\n"))
        }
        Domain::Recovery(RecoveryDomain {
            operation: RecoveryOperation::ValidateReceipt(arguments),
        }) => {
            let repository_root = std::env::current_dir()
                .map_err(|error| GateError::io("resolve current repository directory", &error))?;
            recovery::validate_receipt(&arguments.summary, &repository_root)?;
            Ok("Phase 3 recovery receipt validation passed\n".to_owned())
        }
        Domain::Publication(arguments) => {
            let PublicationDomain { operation } = *arguments;
            let mut transport = github::ChildGitHubTransport::from_environment();
            match operation {
                PublicationOperation::CreateCandidateTag(arguments) => {
                    publication::create_candidate_tag(&arguments.into_request(), &mut transport)
                }
                PublicationOperation::ResolveProtectedRef(arguments) => {
                    publication::resolve_protected_ref(&arguments.into_request(), &mut transport)
                }
                PublicationOperation::Dispatch(arguments) => {
                    publication::dispatch(&arguments.into_request(), &mut transport)
                }
                PublicationOperation::ApprovalTarget(arguments) => {
                    publication::approval_target(&publication::ApprovalTargetRequest {
                        repository: arguments.repo,
                        publication_evidence: arguments.publication_evidence,
                        evidence_schema: arguments.evidence_schema,
                    })
                }
                PublicationOperation::BeginInspection(arguments) => {
                    publication::begin_inspection(&arguments.into_request(), &mut transport)
                }
                PublicationOperation::CompleteInspection(arguments) => {
                    publication::complete_inspection(&arguments.into_request(), &mut transport)
                }
                PublicationOperation::VerifyApproval(arguments) => {
                    publication::verify_approval(&arguments.into_request(), &mut transport)
                }
                PublicationOperation::WaitAndVerify(arguments) => {
                    publication::wait_and_verify(&arguments.into_request(), &mut transport)
                }
                PublicationOperation::ProtectedPublish(arguments) => {
                    publication::protected_publish(&arguments.into_request()?, &mut transport)
                }
            }
        }
    }
}

#[derive(Debug, Parser)]
#[command(
    name = "clinker-release-policy",
    version,
    about = "Fail-closed release policy"
)]
struct Cli {
    #[command(subcommand)]
    domain: Domain,
}

#[derive(Debug, Subcommand)]
enum Domain {
    /// Validate policy decisions and their authority chain.
    Decision(DecisionDomain),
    /// Validate or transition release evidence.
    Evidence(EvidenceDomain),
    /// Validate and materialize the canonical release inventory.
    Inventory(InventoryDomain),
    /// Qualify exact disposable remote-filesystem profiles.
    Filesystem(FilesystemDomain),
    /// Build or verify release artifacts.
    Release(ReleaseDomain),
    /// Verify repository workflow trust policy.
    Workflow(WorkflowDomain),
    /// Verify committed and live repository controls.
    Repository(RepositoryDomain),
    /// Run staged release eligibility and completion gates.
    Gate(GateDomain),
    /// Audit dependency or Rust-only executable boundaries.
    Boundary(BoundaryDomain),
    /// Validate the exact Phase 3 recovery sign-off receipt.
    Recovery(RecoveryDomain),
    /// Perform authenticated candidate and protected-publication operations.
    Publication(Box<PublicationDomain>),
}

#[derive(Debug, Args)]
struct PublicationDomain {
    #[command(subcommand)]
    operation: PublicationOperation,
}

#[derive(Debug, Subcommand)]
enum PublicationOperation {
    /// Create the one authorized immutable candidate tag.
    CreateCandidateTag(PublicationCandidateAuthorizationArgs),
    /// Reread and peel the authorized protected tag.
    ResolveProtectedRef(PublicationCandidateAuthorizationArgs),
    /// Dispatch the protected publication workflow exactly once.
    Dispatch(PublicationDispatchArgs),
    /// Print the exact run, job, and environment requiring inspection.
    ApprovalTarget(PublicationApprovalTargetArgs),
    /// Record authenticated inspection start.
    BeginInspection(PublicationInspectionArgs),
    /// Record authenticated inspection completion.
    CompleteInspection(PublicationInspectionArgs),
    /// Verify a manual protected-environment approval.
    VerifyApproval(PublicationVerifyArgs),
    /// Wait read-only and verify the immutable public release.
    WaitAndVerify(PublicationWaitArgs),
    /// Internal protected-environment worker.
    #[command(hide = true)]
    ProtectedPublish(PublicationProtectedPublishArgs),
}

#[derive(Debug, Args)]
struct PublicationCandidateAuthorizationArgs {
    #[arg(long)]
    repo: String,
    #[arg(long)]
    authorization_record: PathBuf,
    #[arg(long)]
    authorization_schema: PathBuf,
    #[arg(long)]
    deadline_seconds: u64,
}

impl PublicationCandidateAuthorizationArgs {
    fn into_request(self) -> publication::CandidateAuthorizationRequest {
        publication::CandidateAuthorizationRequest {
            repository: self.repo,
            authorization_record: self.authorization_record,
            authorization_schema: self.authorization_schema,
            deadline_seconds: self.deadline_seconds,
        }
    }
}

#[derive(Debug, Args)]
struct PublicationDispatchArgs {
    #[arg(long)]
    repo: String,
    #[arg(long)]
    workflow: String,
    #[arg(long)]
    decision_dir: PathBuf,
    #[arg(long)]
    authorization_record: PathBuf,
    #[arg(long)]
    authorization_schema: PathBuf,
    #[arg(long)]
    decision_record: PathBuf,
    #[arg(long)]
    decision_schema: PathBuf,
    #[arg(long)]
    approval_record: PathBuf,
    #[arg(long)]
    candidate_evidence: PathBuf,
    #[arg(long)]
    evidence_schema: PathBuf,
    #[arg(long)]
    publication_evidence: PathBuf,
    #[arg(long)]
    discovery_deadline_seconds: u64,
}

impl PublicationDispatchArgs {
    fn into_request(self) -> publication::DispatchRequest {
        publication::DispatchRequest {
            repository: self.repo,
            workflow: self.workflow,
            decision_dir: self.decision_dir,
            authorization_record: self.authorization_record,
            authorization_schema: self.authorization_schema,
            decision_record: self.decision_record,
            decision_schema: self.decision_schema,
            approval_record: self.approval_record,
            candidate_evidence: self.candidate_evidence,
            evidence_schema: self.evidence_schema,
            publication_evidence: self.publication_evidence,
            discovery_deadline_seconds: self.discovery_deadline_seconds,
        }
    }
}

#[derive(Debug, Args)]
struct PublicationApprovalTargetArgs {
    #[arg(long)]
    repo: String,
    #[arg(long)]
    publication_evidence: PathBuf,
    #[arg(long)]
    evidence_schema: PathBuf,
}

#[derive(Debug, Args)]
struct PublicationInspectionArgs {
    #[arg(long)]
    repo: String,
    #[arg(long)]
    publication_evidence: PathBuf,
    #[arg(long)]
    evidence_schema: PathBuf,
    #[arg(long)]
    expected_state: String,
    #[arg(long)]
    expected_revision: u64,
}

impl PublicationInspectionArgs {
    fn into_request(self) -> publication::InspectionRequest {
        publication::InspectionRequest {
            repository: self.repo,
            publication_evidence: self.publication_evidence,
            evidence_schema: self.evidence_schema,
            expected_state: self.expected_state,
            expected_revision: self.expected_revision,
        }
    }
}

#[derive(Debug, Args)]
struct PublicationVerificationAuthorityArgs {
    #[arg(long)]
    repo: String,
    #[arg(long)]
    decision_dir: PathBuf,
    #[arg(long)]
    authorization_record: PathBuf,
    #[arg(long)]
    authorization_schema: PathBuf,
    #[arg(long)]
    decision_record: PathBuf,
    #[arg(long)]
    decision_schema: PathBuf,
    #[arg(long)]
    approval_record: PathBuf,
    #[arg(long)]
    candidate_evidence: PathBuf,
    #[arg(long)]
    evidence_schema: PathBuf,
    #[arg(long)]
    publication_evidence: PathBuf,
    #[arg(long)]
    expected_state: String,
    #[arg(long)]
    expected_revision: u64,
}

#[derive(Debug, Args)]
struct PublicationVerifyArgs {
    #[command(flatten)]
    authority: PublicationVerificationAuthorityArgs,
    #[arg(long)]
    deadline_seconds: u64,
}

impl PublicationVerifyArgs {
    fn into_request(self) -> publication::VerificationRequest {
        self.authority.into_request(self.deadline_seconds)
    }
}

#[derive(Debug, Args)]
struct PublicationWaitArgs {
    #[command(flatten)]
    authority: PublicationVerificationAuthorityArgs,
    #[arg(long)]
    run_deadline_seconds: u64,
}

impl PublicationWaitArgs {
    fn into_request(self) -> publication::VerificationRequest {
        self.authority.into_request(self.run_deadline_seconds)
    }
}

impl PublicationVerificationAuthorityArgs {
    fn into_request(self, deadline_seconds: u64) -> publication::VerificationRequest {
        publication::VerificationRequest {
            repository: self.repo,
            decision_dir: self.decision_dir,
            authorization_record: self.authorization_record,
            authorization_schema: self.authorization_schema,
            decision_record: self.decision_record,
            decision_schema: self.decision_schema,
            approval_record: self.approval_record,
            candidate_evidence: self.candidate_evidence,
            evidence_schema: self.evidence_schema,
            publication_evidence: self.publication_evidence,
            expected_state: self.expected_state,
            expected_revision: self.expected_revision,
            deadline_seconds,
        }
    }
}

#[derive(Debug, Args)]
struct PublicationProtectedPublishArgs {
    #[arg(long)]
    repo: String,
    #[arg(long)]
    candidate_tag: String,
    #[arg(long)]
    candidate_authorization_blob_sha: String,
    #[arg(long)]
    candidate_authorization_sha256: String,
    #[arg(long)]
    candidate_decision_blob_sha: String,
    #[arg(long)]
    candidate_evidence_blob_sha: String,
    #[arg(long)]
    source_sha: String,
    #[arg(long)]
    build_workflow_sha: String,
    #[arg(long)]
    publish_workflow_ref: String,
    #[arg(long)]
    publish_workflow_sha: String,
    #[arg(long)]
    candidate_release_id: String,
    #[arg(long)]
    approval_payload_blob_sha: String,
    #[arg(long)]
    approval_record_sha256: String,
    #[arg(long)]
    approval_mode: String,
    #[arg(long)]
    authorization_schema: PathBuf,
    #[arg(long)]
    decision_schema: PathBuf,
    #[arg(long)]
    evidence_schema: PathBuf,
    #[arg(long)]
    decision_dir: PathBuf,
    #[arg(long, default_value_t = 600)]
    deadline_seconds: u64,
}

impl PublicationProtectedPublishArgs {
    fn into_request(self) -> Result<publication::ProtectedPublishRequest, GateError> {
        Ok(publication::ProtectedPublishRequest {
            repository: self.repo,
            candidate_tag: self.candidate_tag,
            candidate_authorization_blob_sha: self.candidate_authorization_blob_sha,
            candidate_authorization_sha256: self.candidate_authorization_sha256,
            candidate_decision_blob_sha: self.candidate_decision_blob_sha,
            candidate_evidence_blob_sha: self.candidate_evidence_blob_sha,
            source_sha: self.source_sha,
            build_workflow_sha: self.build_workflow_sha,
            publish_workflow_ref: self.publish_workflow_ref,
            publish_workflow_sha: self.publish_workflow_sha,
            candidate_release_id: self.candidate_release_id,
            approval_payload_blob_sha: self.approval_payload_blob_sha,
            approval_record_sha256: self.approval_record_sha256,
            approval_mode: self.approval_mode,
            authorization_schema: self.authorization_schema,
            decision_schema: self.decision_schema,
            evidence_schema: self.evidence_schema,
            decision_dir: self.decision_dir,
            context: publication::WorkflowContext::from_environment()?,
            deadline_seconds: self.deadline_seconds,
        })
    }
}

#[derive(Debug, Args)]
struct GateDomain {
    #[command(subcommand)]
    operation: GateOperation,
}

#[derive(Debug, Subcommand)]
enum GateOperation {
    /// Run one exact fail-closed gate stage.
    Run(Box<GateRunArgs>),
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum GateStage {
    PreCandidate,
    Final,
}

#[derive(Debug, Args)]
struct GateRunArgs {
    #[arg(long, value_enum)]
    stage: GateStage,
    #[arg(long)]
    rust_command_deadline_seconds: Option<u64>,
    #[arg(long)]
    repository_controls_evidence: Option<PathBuf>,
    #[arg(long)]
    authorization_record: Option<PathBuf>,
    #[arg(long)]
    authorization_schema: Option<PathBuf>,
    #[arg(long)]
    decision_record: Option<PathBuf>,
    #[arg(long)]
    decision_schema: Option<PathBuf>,
    #[arg(long)]
    pre_candidate_manifest: Option<PathBuf>,
    #[arg(long)]
    candidate_evidence: Option<PathBuf>,
    #[arg(long)]
    publication_evidence: Option<PathBuf>,
    #[arg(long)]
    evidence_manifest: Option<PathBuf>,
}

enum GateRunRequest {
    PreCandidate(gate::PreCandidateRequest),
    Final(gate::FinalRequest),
}

impl GateRunArgs {
    fn preflight(self) -> Result<GateRunRequest, GateError> {
        match self.stage {
            GateStage::PreCandidate => {
                if self.authorization_record.is_some()
                    || self.authorization_schema.is_some()
                    || self.decision_record.is_some()
                    || self.decision_schema.is_some()
                    || self.pre_candidate_manifest.is_some()
                    || self.candidate_evidence.is_some()
                    || self.publication_evidence.is_some()
                {
                    return Err(GateError::usage(
                        "final-stage inputs cannot be combined with --stage pre-candidate",
                    ));
                }
                Ok(GateRunRequest::PreCandidate(gate::PreCandidateRequest {
                    command_deadline_seconds: required(
                        self.rust_command_deadline_seconds,
                        "--rust-command-deadline-seconds",
                    )?,
                    repository_controls_evidence: required(
                        self.repository_controls_evidence,
                        "--repository-controls-evidence",
                    )?,
                    evidence_manifest: required(self.evidence_manifest, "--evidence-manifest")?,
                }))
            }
            GateStage::Final => {
                if self.rust_command_deadline_seconds.is_some()
                    || self.repository_controls_evidence.is_some()
                {
                    return Err(GateError::usage(
                        "pre-candidate inputs cannot be combined with --stage final",
                    ));
                }
                Ok(GateRunRequest::Final(gate::FinalRequest {
                    authorization_record: required(
                        self.authorization_record,
                        "--authorization-record",
                    )?,
                    authorization_schema: required(
                        self.authorization_schema,
                        "--authorization-schema",
                    )?,
                    decision_record: required(self.decision_record, "--decision-record")?,
                    decision_schema: required(self.decision_schema, "--decision-schema")?,
                    pre_candidate_manifest: required(
                        self.pre_candidate_manifest,
                        "--pre-candidate-manifest",
                    )?,
                    candidate_evidence: required(self.candidate_evidence, "--candidate-evidence")?,
                    publication_evidence: required(
                        self.publication_evidence,
                        "--publication-evidence",
                    )?,
                    evidence_manifest: required(self.evidence_manifest, "--evidence-manifest")?,
                }))
            }
        }
    }
}

#[derive(Debug, Args)]
struct BoundaryDomain {
    #[command(subcommand)]
    operation: BoundaryOperation,
}

#[derive(Debug, Subcommand)]
enum BoundaryOperation {
    /// Audit one explicitly selected boundary.
    Audit(BoundaryAuditArgs),
}

#[derive(Debug, Args)]
struct BoundaryAuditArgs {
    #[arg(long, value_enum)]
    scope: BoundaryScope,
    #[arg(long)]
    root: PathBuf,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum BoundaryScope {
    Dependency,
    RustOnly,
}

#[derive(Debug, Args)]
struct RecoveryDomain {
    #[command(subcommand)]
    operation: RecoveryOperation,
}

#[derive(Debug, Subcommand)]
enum RecoveryOperation {
    /// Validate one strict fenced Phase 3 recovery receipt.
    ValidateReceipt(RecoveryValidateReceiptArgs),
}

#[derive(Debug, Args)]
struct RecoveryValidateReceiptArgs {
    /// Phase 03-51 summary containing the unique receipt block.
    #[arg(long)]
    summary: PathBuf,
}

impl From<BoundaryScope> for boundary::Scope {
    fn from(value: BoundaryScope) -> Self {
        match value {
            BoundaryScope::Dependency => Self::Dependency,
            BoundaryScope::RustOnly => Self::RustOnly,
        }
    }
}

#[derive(Debug, Args)]
struct RepositoryDomain {
    #[command(subcommand)]
    operation: RepositoryOperation,
}

#[derive(Debug, Subcommand)]
enum RepositoryOperation {
    /// Verify committed configuration or authenticated repository controls.
    Verify(RepositoryVerifyArgs),
}

#[derive(Debug, Args)]
struct RepositoryVerifyArgs {
    #[arg(long)]
    config_only: bool,
    #[arg(long)]
    repo: Option<String>,
    #[arg(long)]
    apply_approved: Option<PathBuf>,
    #[arg(long)]
    environment_policy: Option<PathBuf>,
    #[arg(long)]
    publication_policy: Option<PathBuf>,
    #[arg(long)]
    evidence_manifest: Option<PathBuf>,
}

enum RepositoryVerifyRequest {
    ConfigOnly,
    Readback { repository: String },
    Apply(repository::ApplyRequest),
}

impl RepositoryVerifyArgs {
    fn preflight(self) -> Result<RepositoryVerifyRequest, GateError> {
        let apply_flags = [
            self.apply_approved.is_some(),
            self.environment_policy.is_some(),
            self.publication_policy.is_some(),
            self.evidence_manifest.is_some(),
        ];
        if self.config_only {
            if self.repo.is_some() || apply_flags.into_iter().any(|present| present) {
                return Err(GateError::usage(
                    "--config-only cannot be combined with authenticated repository flags",
                ));
            }
            return Ok(RepositoryVerifyRequest::ConfigOnly);
        }
        let repository = required(self.repo, "--repo")?;
        if !apply_flags.into_iter().any(|present| present) {
            return Ok(RepositoryVerifyRequest::Readback { repository });
        }
        if !apply_flags.into_iter().all(|present| present) {
            return Err(GateError::usage(
                "authenticated apply requires --apply-approved, --environment-policy, --publication-policy, and --evidence-manifest",
            ));
        }
        Ok(RepositoryVerifyRequest::Apply(repository::ApplyRequest {
            repository,
            release_rules: required(self.apply_approved, "--apply-approved")?,
            environment_policy: required(self.environment_policy, "--environment-policy")?,
            publication_policy: required(self.publication_policy, "--publication-policy")?,
            evidence_manifest: required(self.evidence_manifest, "--evidence-manifest")?,
        }))
    }
}

#[derive(Debug, Args)]
struct WorkflowDomain {
    #[command(subcommand)]
    operation: WorkflowOperation,
}

#[derive(Debug, Subcommand)]
enum WorkflowOperation {
    /// Verify every repository workflow against the typed trust contract.
    Verify,
}

#[derive(Debug, Args)]
struct FilesystemDomain {
    #[command(subcommand)]
    operation: FilesystemOperation,
}

#[derive(Debug, Subcommand)]
enum FilesystemOperation {
    /// Provision, exercise, and teardown one disposable profile.
    ProvisionAndRun(FilesystemProvisionArgs),
    /// Exercise one already mounted exact profile.
    RunProfile(FilesystemRunProfileArgs),
    /// Unconditionally cleanup and finalize one profile.
    Teardown(FilesystemTeardownArgs),
    /// Validate direct-CI topology and internal policy invariants.
    SelfTest(FilesystemSelfTestArgs),
}

#[derive(Debug, Args)]
struct FilesystemProvisionArgs {
    #[arg(long)]
    profile: String,
    #[arg(long)]
    evidence: PathBuf,
}

#[derive(Debug, Args)]
struct FilesystemRunProfileArgs {
    #[arg(long)]
    profile: String,
    #[arg(long)]
    mount_root: PathBuf,
    #[arg(long)]
    evidence: PathBuf,
    #[arg(long)]
    package_observations: PathBuf,
    #[arg(long)]
    protocol_observations: PathBuf,
}

#[derive(Debug, Args)]
struct FilesystemTeardownArgs {
    #[arg(long)]
    profile: String,
    #[arg(long)]
    evidence: PathBuf,
}

#[derive(Debug, Args)]
struct FilesystemSelfTestArgs {
    #[arg(long, default_value = ".github/workflows/ci.yml")]
    workflow: PathBuf,
}

#[derive(Debug, Args)]
struct InventoryDomain {
    #[command(subcommand)]
    operation: InventoryOperation,
}

#[derive(Debug, Subcommand)]
enum InventoryOperation {
    /// Validate the complete four-target release inventory.
    Check(InventoryCheckArgs),
}

#[derive(Debug, Args)]
struct InventoryCheckArgs {
    /// Inventory file; defaults to release/inventory.toml.
    #[arg(long)]
    inventory: Option<PathBuf>,
    /// Repository root; defaults to the current directory.
    #[arg(long)]
    repo_root: Option<PathBuf>,
    /// Print the materialized inventory as deterministic JSON.
    #[arg(long)]
    print_json: bool,
}

#[derive(Debug, Args)]
struct ReleaseDomain {
    #[command(subcommand)]
    operation: ReleaseOperation,
}

#[derive(Debug, Subcommand)]
enum ReleaseOperation {
    /// Build one deterministic suite archive and sidecars.
    BuildBundle(ReleaseBuildBundleArgs),
    /// Verify a complete assembly or artifact-derived candidate.
    Verify(Box<ReleaseVerifyArgs>),
    /// Internal private-draft staging worker.
    #[command(hide = true)]
    StageCandidateDraft(Box<ReleaseStageDraftArgs>),
}

#[derive(Debug, Args)]
struct ReleaseStageDraftArgs {
    #[arg(long)]
    repo: String,
    #[arg(long)]
    candidate_tag: String,
    #[arg(long)]
    source_sha: String,
    #[arg(long)]
    asset_dir: PathBuf,
    #[arg(long, default_value_t = 600)]
    deadline_seconds: u64,
}

impl ReleaseStageDraftArgs {
    fn into_request(self) -> Result<release::StageDraftRequest, GateError> {
        Ok(release::StageDraftRequest {
            repository: self.repo,
            candidate_tag: self.candidate_tag,
            source_sha: self.source_sha,
            asset_dir: self.asset_dir,
            context: release::StageWorkflowContext::from_environment()?,
            deadline_seconds: self.deadline_seconds,
        })
    }
}

#[derive(Debug, Args)]
struct ReleaseBuildBundleArgs {
    /// Supported Rust target triple.
    #[arg(long)]
    target: String,
    /// Full lowercase source commit SHA.
    #[arg(long)]
    source_sha: String,
    /// Destination directory for the archive and sidecars.
    #[arg(long)]
    output_dir: PathBuf,
}

#[derive(Debug, Args)]
struct ReleaseVerifyArgs {
    /// Compatibility assembly form used by verify-release.sh.
    #[arg(value_enum)]
    mode: Option<ReleaseVerifyMode>,

    #[arg(long)]
    asset_dir: Option<PathBuf>,
    #[arg(long)]
    draft_dir: Option<PathBuf>,
    #[arg(long)]
    repository: Option<String>,
    #[arg(long)]
    workflow: Option<String>,
    #[arg(long = "ref")]
    release_ref: Option<String>,
    #[arg(long)]
    source_sha: Option<String>,

    #[arg(long)]
    repo: Option<String>,
    #[arg(long)]
    decision_dir: Option<PathBuf>,
    #[arg(long)]
    authorization_record: Option<PathBuf>,
    #[arg(long)]
    authorization_schema: Option<PathBuf>,
    #[arg(long)]
    decision_record: Option<PathBuf>,
    #[arg(long)]
    decision_schema: Option<PathBuf>,
    #[arg(long)]
    candidate_evidence: Option<PathBuf>,
    #[arg(long)]
    evidence_schema: Option<PathBuf>,
    #[arg(long)]
    require_private: bool,
    #[arg(long)]
    fresh_download: bool,
    #[arg(long)]
    evidence_kind: Option<CandidateEvidenceKind>,
    #[arg(long)]
    evidence_manifest: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ReleaseVerifyMode {
    Assemble,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CandidateEvidenceKind {
    Candidate,
}

enum VerifyRequest {
    Assembly(AssemblyRequest),
    Candidate(CandidateRequest),
}

impl ReleaseVerifyArgs {
    fn preflight(self) -> Result<VerifyRequest, GateError> {
        if matches!(self.mode, Some(ReleaseVerifyMode::Assemble)) {
            if self.repo.is_some()
                || self.decision_dir.is_some()
                || self.authorization_record.is_some()
                || self.authorization_schema.is_some()
                || self.decision_record.is_some()
                || self.decision_schema.is_some()
                || self.candidate_evidence.is_some()
                || self.evidence_schema.is_some()
                || self.require_private
                || self.fresh_download
                || self.evidence_kind.is_some()
                || self.evidence_manifest.is_some()
            {
                return Err(GateError::usage(
                    "artifact-derived candidate flags are incompatible with assemble",
                ));
            }
            return Ok(VerifyRequest::Assembly(AssemblyRequest {
                asset_dir: required(self.asset_dir, "--asset-dir")?,
                draft_dir: self.draft_dir,
                repository: required(self.repository, "--repository")?,
                workflow: required(self.workflow, "--workflow")?,
                release_ref: required(self.release_ref, "--ref")?,
                source_sha: required(self.source_sha, "--source-sha")?,
            }));
        }
        for (present, flag) in [
            (self.asset_dir.is_some(), "--asset-dir"),
            (self.draft_dir.is_some(), "--draft-dir"),
            (self.repository.is_some(), "--repository"),
            (self.workflow.is_some(), "--workflow"),
            (self.release_ref.is_some(), "--ref"),
            (self.source_sha.is_some(), "--source-sha"),
        ] {
            if present {
                return Err(GateError::usage(format!(
                    "unexpected argument {flag} for artifact-derived verification"
                )));
            }
        }
        if !self.require_private || !self.fresh_download {
            return Err(GateError::usage(
                "artifact-derived verification requires --require-private and --fresh-download",
            ));
        }
        let producer = self.candidate_evidence.is_none()
            && matches!(self.evidence_kind, Some(CandidateEvidenceKind::Candidate))
            && self.evidence_manifest.is_some();
        let readback = self.candidate_evidence.is_some()
            && self.evidence_kind.is_none()
            && self.evidence_manifest.is_none();
        if !producer && !readback {
            return Err(GateError::usage(
                "choose exactly one candidate producer or candidate readback flag shape",
            ));
        }
        if producer
            && (self.decision_dir.is_some()
                || self.decision_record.is_some()
                || self.decision_schema.is_some())
        {
            return Err(GateError::usage(
                "candidate production accepts authorization and observed draft state, not a post-build decision",
            ));
        }
        if readback
            && (self.decision_dir.is_none()
                || self.decision_record.is_none()
                || self.decision_schema.is_none())
        {
            return Err(GateError::usage(
                "candidate readback requires --decision-dir, --decision-record, and --decision-schema",
            ));
        }
        Ok(VerifyRequest::Candidate(CandidateRequest {
            repository: required(self.repo, "--repo")?,
            decision_dir: self.decision_dir,
            authorization_record: required(self.authorization_record, "--authorization-record")?,
            authorization_schema: required(self.authorization_schema, "--authorization-schema")?,
            decision_record: self.decision_record,
            decision_schema: self.decision_schema,
            candidate_evidence: self.candidate_evidence,
            evidence_schema: required(self.evidence_schema, "--evidence-schema")?,
            evidence_manifest: self.evidence_manifest,
        }))
    }
}

fn required<T>(value: Option<T>, flag: &str) -> Result<T, GateError> {
    value.ok_or_else(|| GateError::usage(format!("missing required argument {flag}")))
}

#[derive(Debug, Args)]
struct DecisionDomain {
    #[command(subcommand)]
    operation: DecisionOperation,
}

#[derive(Debug, Subcommand)]
enum DecisionOperation {
    /// Validate strict decision and candidate-authorization records.
    Validate(DecisionValidateArgs),
    /// Verify the fixed Phase 4 dependency capability contract.
    VerifyPhase4Capabilities(DecisionPhase4CapabilityArgs),
}

#[derive(Debug, Args)]
struct DecisionPhase4CapabilityArgs {
    /// Root of the Clinker Cargo workspace.
    #[arg(long)]
    workspace_root: PathBuf,
}

#[derive(Debug, Args)]
struct DecisionValidateArgs {
    /// release decision record schema.
    #[arg(long)]
    schema: Option<PathBuf>,
    /// Decision record JSON. May be repeated.
    #[arg(long, action = ArgAction::Append)]
    record: Vec<PathBuf>,
    /// Candidate authorization schema.
    #[arg(long)]
    authorization_schema: Option<PathBuf>,
    /// Candidate authorization record.
    #[arg(long)]
    authorization_record: Option<PathBuf>,
    /// Candidate evidence to bind to accepted candidate authority.
    #[arg(long)]
    candidate_evidence: Option<PathBuf>,
    /// Require a decision identifier. May be repeated.
    #[arg(long, action = ArgAction::Append)]
    require_id: Vec<String>,
    /// Require the exact authorization identifier.
    #[arg(long)]
    require_authorization_id: Option<String>,
    /// Require a status-authorized authorization record.
    #[arg(long)]
    require_authorized: bool,
    /// Require the complete eight-record decision set.
    #[arg(long)]
    require_complete: bool,
    /// Require every supplied decision to be accepted.
    #[arg(long)]
    require_accepted: bool,
}

impl DecisionValidateArgs {
    fn preflight(self) -> Result<DecisionRequest, GateError> {
        let has_records = !self.record.is_empty();
        let has_authorization = self.authorization_record.is_some();
        if !has_records && !has_authorization {
            return Err(GateError::usage(
                "at least one --record or --authorization-record is required",
            ));
        }
        if self.record.len() > MAX_DECISION_RECORDS {
            return Err(GateError::usage(format!(
                "--record may appear at most {MAX_DECISION_RECORDS} times"
            )));
        }
        if has_records != self.schema.is_some() {
            return Err(GateError::usage(
                "--schema and at least one --record must be supplied together",
            ));
        }
        if has_authorization != self.authorization_schema.is_some() {
            return Err(GateError::usage(
                "--authorization-schema and --authorization-record must be supplied together",
            ));
        }
        if self.candidate_evidence.is_some() && (!has_records || !has_authorization) {
            return Err(GateError::usage(
                "--candidate-evidence requires decision and authorization records",
            ));
        }
        if (!self.require_id.is_empty() || self.require_complete || self.require_accepted)
            && !has_records
        {
            return Err(GateError::usage(
                "decision requirements need at least one --record",
            ));
        }
        if (self.require_authorized || self.require_authorization_id.is_some())
            && !has_authorization
        {
            return Err(GateError::usage(
                "authorization requirements need --authorization-record",
            ));
        }
        let unique_ids = self
            .require_id
            .iter()
            .collect::<std::collections::BTreeSet<_>>();
        if unique_ids.len() != self.require_id.len() {
            return Err(GateError::usage("--require-id values must be unique"));
        }
        Ok(DecisionRequest {
            schema: self.schema,
            records: self.record,
            authorization_schema: self.authorization_schema,
            authorization_record: self.authorization_record,
            candidate_evidence: self.candidate_evidence,
            require_ids: self.require_id,
            require_authorization_id: self.require_authorization_id,
            require_authorized: self.require_authorized,
            require_complete: self.require_complete,
            require_accepted: self.require_accepted,
        })
    }
}

#[derive(Debug, Args)]
struct EvidenceDomain {
    #[command(subcommand)]
    operation: EvidenceOperation,
}

#[derive(Debug, Subcommand)]
enum EvidenceOperation {
    /// Validate candidate or publication evidence without modification.
    Validate(EvidenceValidateArgs),
    /// Accept only sole-producer final completion evidence.
    AssertComplete(EvidenceAssertCompleteArgs),
}

#[derive(Debug, Args)]
struct EvidenceAssertCompleteArgs {
    #[arg(long)]
    manifest: PathBuf,
}

#[derive(Debug, Args)]
struct EvidenceValidateArgs {
    /// Strict evidence kind.
    #[arg(long)]
    kind: EvidenceKind,
    /// release evidence schema.
    #[arg(long)]
    schema: PathBuf,
    /// Evidence manifest to validate without modification.
    #[arg(long)]
    manifest: PathBuf,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum EvidenceKind {
    Candidate,
    Publication,
}

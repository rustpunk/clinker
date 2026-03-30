//! Git remote provider abstraction — GitHub, GitLab, Bitbucket.
//!
//! Spec: clinker-kiln-git-addendum.md §G7.1.

use crate::ops::GitError;

/// Pull request creation parameters.
#[derive(Clone, Debug)]
pub struct PrParams {
    /// Source branch name.
    pub source_branch: String,
    /// Target branch name.
    pub target_branch: String,
    /// PR title.
    pub title: String,
    /// PR description/body (markdown).
    pub body: String,
    /// Whether to create as a draft.
    pub draft: bool,
}

/// Result of PR creation.
#[derive(Clone, Debug)]
pub struct PrResult {
    /// PR number.
    pub number: u64,
    /// URL to the created PR.
    pub url: String,
}

/// Supported remote providers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProviderKind {
    GitHub,
    GitLab,
    Bitbucket,
    Unknown,
}

impl ProviderKind {
    pub fn label(self) -> &'static str {
        match self {
            Self::GitHub => "GitHub",
            Self::GitLab => "GitLab",
            Self::Bitbucket => "Bitbucket",
            Self::Unknown => "Unknown",
        }
    }
}

/// Auto-detect provider from a git remote URL.
pub fn detect_provider(remote_url: &str) -> ProviderKind {
    let url_lower = remote_url.to_lowercase();
    if url_lower.contains("github.com") {
        ProviderKind::GitHub
    } else if url_lower.contains("gitlab.com") || url_lower.contains("gitlab.") {
        ProviderKind::GitLab
    } else if url_lower.contains("bitbucket.org") || url_lower.contains("bitbucket.") {
        ProviderKind::Bitbucket
    } else {
        ProviderKind::Unknown
    }
}

/// Extract owner/repo from a remote URL.
///
/// Handles both SSH (`git@github.com:owner/repo.git`) and HTTPS
/// (`https://github.com/owner/repo.git`) formats.
pub fn parse_remote_url(url: &str) -> Option<(String, String)> {
    // SSH format: git@github.com:owner/repo.git
    if url.contains('@') && url.contains(':') {
        let after_colon = url.split(':').last()?;
        let path = after_colon.trim_end_matches(".git");
        let parts: Vec<&str> = path.splitn(2, '/').collect();
        if parts.len() == 2 {
            return Some((parts[0].to_string(), parts[1].to_string()));
        }
    }

    // HTTPS format: https://github.com/owner/repo.git
    if url.contains("://") {
        let after_host = url.split("://").nth(1)?;
        let path = after_host.split('/').skip(1).collect::<Vec<_>>().join("/");
        let path = path.trim_end_matches(".git");
        let parts: Vec<&str> = path.splitn(2, '/').collect();
        if parts.len() == 2 {
            return Some((parts[0].to_string(), parts[1].to_string()));
        }
    }

    None
}

/// Get the remote URL for a repository.
pub fn get_remote_url(repo_path: &std::path::Path) -> Result<String, GitError> {
    let output = std::process::Command::new("git")
        .args(["remote", "get-url", "origin"])
        .current_dir(repo_path)
        .output()
        .map_err(|e| GitError::Cli(e.to_string()))?;

    if !output.status.success() {
        return Err(GitError::Operation("no remote 'origin' configured".to_string()));
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// Get the default branch for a remote (usually main or master).
pub fn get_default_branch(repo_path: &std::path::Path) -> Result<String, GitError> {
    // Try symbolic-ref for origin/HEAD
    let output = std::process::Command::new("git")
        .args(["symbolic-ref", "refs/remotes/origin/HEAD"])
        .current_dir(repo_path)
        .output()
        .map_err(|e| GitError::Cli(e.to_string()))?;

    if output.status.success() {
        let full_ref = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if let Some(branch) = full_ref.strip_prefix("refs/remotes/origin/") {
            return Ok(branch.to_string());
        }
    }

    // Fallback: check if "main" or "master" exists
    let output = std::process::Command::new("git")
        .args(["branch", "--list", "main", "master"])
        .current_dir(repo_path)
        .output()
        .map_err(|e| GitError::Cli(e.to_string()))?;

    let branches = String::from_utf8_lossy(&output.stdout);
    for line in branches.lines() {
        let name = line.trim().trim_start_matches('*').trim();
        if name == "main" || name == "master" {
            return Ok(name.to_string());
        }
    }

    Ok("main".to_string()) // Default fallback
}

/// Create a PR using the `gh` CLI (GitHub) or platform-specific tools.
///
/// For now, this uses `gh pr create` for GitHub. GitLab and Bitbucket
/// support will be added when those providers are needed.
pub fn create_pr(
    repo_path: &std::path::Path,
    params: &PrParams,
    provider: ProviderKind,
) -> Result<PrResult, GitError> {
    match provider {
        ProviderKind::GitHub => create_github_pr(repo_path, params),
        _ => Err(GitError::Operation(format!(
            "PR creation not yet supported for {}",
            provider.label()
        ))),
    }
}

/// Create a GitHub PR via `gh` CLI.
fn create_github_pr(repo_path: &std::path::Path, params: &PrParams) -> Result<PrResult, GitError> {
    let mut args = vec![
        "pr".to_string(),
        "create".to_string(),
        "--title".to_string(),
        params.title.clone(),
        "--body".to_string(),
        params.body.clone(),
        "--base".to_string(),
        params.target_branch.clone(),
        "--head".to_string(),
        params.source_branch.clone(),
    ];

    if params.draft {
        args.push("--draft".to_string());
    }

    let arg_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();

    let output = std::process::Command::new("gh")
        .args(&arg_refs)
        .current_dir(repo_path)
        .output()
        .map_err(|e| GitError::Cli(format!("gh not found: {e}. Install GitHub CLI to create PRs.")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(GitError::Operation(format!("gh pr create failed: {stderr}")));
    }

    let url = String::from_utf8_lossy(&output.stdout).trim().to_string();

    // Extract PR number from URL
    let number = url
        .split('/')
        .last()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);

    Ok(PrResult { number, url })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_provider() {
        assert_eq!(detect_provider("git@github.com:user/repo.git"), ProviderKind::GitHub);
        assert_eq!(detect_provider("https://github.com/user/repo"), ProviderKind::GitHub);
        assert_eq!(detect_provider("git@gitlab.com:user/repo.git"), ProviderKind::GitLab);
        assert_eq!(detect_provider("https://gitlab.internal.co/user/repo"), ProviderKind::GitLab);
        assert_eq!(detect_provider("git@bitbucket.org:user/repo.git"), ProviderKind::Bitbucket);
        assert_eq!(detect_provider("https://some-random-host.com/repo"), ProviderKind::Unknown);
    }

    #[test]
    fn test_parse_remote_url_ssh() {
        let (owner, repo) = parse_remote_url("git@github.com:rustpunk/clinker.git").unwrap();
        assert_eq!(owner, "rustpunk");
        assert_eq!(repo, "clinker");
    }

    #[test]
    fn test_parse_remote_url_https() {
        let (owner, repo) = parse_remote_url("https://github.com/rustpunk/clinker.git").unwrap();
        assert_eq!(owner, "rustpunk");
        assert_eq!(repo, "clinker");
    }

    #[test]
    fn test_parse_remote_url_no_git_suffix() {
        let (owner, repo) = parse_remote_url("https://github.com/rustpunk/clinker").unwrap();
        assert_eq!(owner, "rustpunk");
        assert_eq!(repo, "clinker");
    }
}

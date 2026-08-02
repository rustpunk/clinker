//! Bounded argv-only GitHub API transport.

use std::collections::{BTreeMap, VecDeque};
use std::ffi::OsString;
use std::path::PathBuf;
use std::time::Duration;

use serde_json::Value;

use crate::child::{self, ChildSpec, Termination};
use crate::error::GateError;
use crate::limits::DEFAULT_CHILD_OUTPUT_BYTES;

/// Supported GitHub API verbs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    /// Read an immutable remote observation.
    Get,
    /// Create a remote object or dispatch.
    Post,
    /// Change the exact authorized remote object.
    Patch,
}

impl Method {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
            Self::Patch => "PATCH",
        }
    }
}

/// One explicit API request. Fields are emitted as individual argv entries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    /// HTTP method.
    pub method: Method,
    /// Repository-relative API endpoint.
    pub endpoint: String,
    /// Form fields in stable key order.
    pub fields: BTreeMap<String, String>,
    /// Explicit HTTP headers in stable key order.
    pub headers: BTreeMap<String, String>,
    /// Return raw bounded bytes instead of parsing JSON.
    pub raw_response: bool,
    /// Optional regular file supplied as the request body.
    pub input_file: Option<PathBuf>,
    /// Per-call wall-clock deadline.
    pub deadline: Duration,
}

impl Request {
    /// Construct a request without fields.
    #[must_use]
    pub fn new(method: Method, endpoint: impl Into<String>, deadline: Duration) -> Self {
        Self {
            method,
            endpoint: endpoint.into(),
            fields: BTreeMap::new(),
            headers: BTreeMap::new(),
            raw_response: false,
            input_file: None,
            deadline,
        }
    }

    /// Add one explicit form field.
    #[must_use]
    pub fn field(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.fields.insert(key.into(), value.into());
        self
    }

    /// Add one explicit HTTP header.
    #[must_use]
    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    /// Request bounded raw response bytes.
    #[must_use]
    pub fn raw(mut self) -> Self {
        self.raw_response = true;
        self
    }

    /// Supply a local regular file as the request body.
    #[must_use]
    pub fn input_file(mut self, path: PathBuf) -> Self {
        self.input_file = Some(path);
        self
    }
}

/// Bounded JSON observation returned by the transport.
#[derive(Debug, Clone, PartialEq)]
pub struct Response {
    /// Parsed JSON response, or null for a successful empty response.
    pub body: Value,
    /// Bounded raw response bytes when requested.
    pub raw: Option<Vec<u8>>,
}

/// GitHub API boundary used by publication policy.
pub trait GitHubTransport {
    /// Execute one exact request.
    fn send(&mut self, request: &Request) -> Result<Response, GateError>;
}

/// Production transport implemented through the bounded child runner.
pub struct ChildGitHubTransport {
    program: PathBuf,
    environment: BTreeMap<OsString, OsString>,
}

impl ChildGitHubTransport {
    /// Build the production transport from the allowlisted process environment.
    #[must_use]
    pub fn from_environment() -> Self {
        let mut environment = BTreeMap::new();
        for name in ["GH_TOKEN", "GITHUB_TOKEN", "NO_COLOR", "PATH"] {
            if let Some(value) = std::env::var_os(name) {
                environment.insert(OsString::from(name), value);
            }
        }
        Self {
            program: PathBuf::from("gh"),
            environment,
        }
    }
}

impl GitHubTransport for ChildGitHubTransport {
    fn send(&mut self, request: &Request) -> Result<Response, GateError> {
        if !request.endpoint.starts_with("repos/")
            && !request
                .endpoint
                .starts_with("https://uploads.github.com/repos/")
        {
            return Err(policy("GitHub endpoint must be repository-relative"));
        }
        let mut arguments = vec![
            OsString::from("api"),
            OsString::from("--method"),
            OsString::from(request.method.as_str()),
            OsString::from(&request.endpoint),
        ];
        for (key, value) in &request.fields {
            arguments.push(OsString::from("-f"));
            arguments.push(OsString::from(format!("{key}={value}")));
        }
        for (key, value) in &request.headers {
            arguments.push(OsString::from("-H"));
            arguments.push(OsString::from(format!("{key}: {value}")));
        }
        if let Some(path) = &request.input_file {
            let metadata = std::fs::symlink_metadata(path)
                .map_err(|error| GateError::io("inspect GitHub API input", &error))?;
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(policy(
                    "GitHub API input must be a regular non-symlink file",
                ));
            }
            arguments.push(OsString::from("--input"));
            arguments.push(path.as_os_str().to_os_string());
        }
        let result = child::run(ChildSpec {
            program: self.program.clone(),
            arguments,
            environment: self.environment.clone(),
            timeout: request.deadline,
            output_limit: DEFAULT_CHILD_OUTPUT_BYTES,
        })?;
        if result.termination == Termination::TimedOut {
            return Err(policy("GitHub API request exceeded its deadline"));
        }
        if result.termination != Termination::Exited(Some(0)) {
            return Err(policy("GitHub API request failed"));
        }
        if result.stdout_truncated || result.stderr_truncated {
            return Err(policy(
                "GitHub API response exceeded the bounded output limit",
            ));
        }
        if request.raw_response {
            return Ok(Response {
                body: Value::Null,
                raw: Some(result.stdout),
            });
        }
        let body = if result.stdout.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&result.stdout)
                .map_err(|_| policy("GitHub API returned malformed JSON"))?
        };
        Ok(Response { body, raw: None })
    }
}

/// One deterministic request/response exchange for contract tests.
#[derive(Debug, Clone)]
pub struct ScriptedExchange {
    /// Request that must be observed exactly.
    pub request: Request,
    /// Response or stable failure returned to the caller.
    pub response: Result<Response, &'static str>,
}

/// Deterministic fake transport which rejects missing, extra, or reordered calls.
#[derive(Debug, Default)]
pub struct ScriptedTransport {
    exchanges: VecDeque<ScriptedExchange>,
    observed: Vec<Request>,
}

impl ScriptedTransport {
    /// Create a deterministic script.
    #[must_use]
    pub fn new(exchanges: impl IntoIterator<Item = ScriptedExchange>) -> Self {
        Self {
            exchanges: exchanges.into_iter().collect(),
            observed: Vec::new(),
        }
    }

    /// Requests observed so far.
    #[must_use]
    pub fn observed(&self) -> &[Request] {
        &self.observed
    }

    /// True only when every scripted exchange was consumed.
    #[must_use]
    pub fn is_exhausted(&self) -> bool {
        self.exchanges.is_empty()
    }
}

impl GitHubTransport for ScriptedTransport {
    fn send(&mut self, request: &Request) -> Result<Response, GateError> {
        self.observed.push(request.clone());
        let exchange = self
            .exchanges
            .pop_front()
            .ok_or_else(|| policy("unexpected GitHub API request"))?;
        if exchange.request != *request {
            return Err(policy(
                "GitHub API request did not match the scripted authority",
            ));
        }
        exchange
            .response
            .map_err(|detail| policy(format!("scripted GitHub API failure: {detail}")))
    }
}

fn policy(detail: impl Into<String>) -> GateError {
    GateError::policy("publication.github", detail)
}

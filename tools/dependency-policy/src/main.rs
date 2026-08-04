use std::env;
use std::path::PathBuf;
use std::process::ExitCode;

use clinker_dependency_policy::{Scope, check_repository};

const USAGE: &str = "usage: clinker-dependency-policy --scope <core|clinker-net|clinker-lineage|final> [--root PATH]";

enum RunOutcome {
    Checked(Scope),
    Help,
}

fn main() -> ExitCode {
    match run() {
        Ok(RunOutcome::Checked(scope)) => {
            println!("Dependency policy passed: {scope}");
            ExitCode::SUCCESS
        }
        Ok(RunOutcome::Help) => {
            println!("{USAGE}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("Dependency policy failed: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<RunOutcome, String> {
    let mut scope = None;
    let mut root = None;
    let mut arguments = env::args_os().skip(1);

    while let Some(argument) = arguments.next() {
        match argument.to_str() {
            Some("--scope") => {
                let value = arguments
                    .next()
                    .ok_or_else(|| "--scope requires a value".to_owned())?;
                let value = value
                    .to_str()
                    .ok_or_else(|| "--scope must be valid UTF-8".to_owned())?;
                let parsed = value.parse::<Scope>().map_err(|error| error.to_string())?;
                if scope.replace(parsed).is_some() {
                    return Err("--scope may be supplied only once".to_owned());
                }
            }
            Some("--root") => {
                let parsed = PathBuf::from(
                    arguments
                        .next()
                        .ok_or_else(|| "--root requires a value".to_owned())?,
                );
                if root.replace(parsed).is_some() {
                    return Err("--root may be supplied only once".to_owned());
                }
            }
            Some("--help" | "-h") => return Ok(RunOutcome::Help),
            Some(other) => return Err(format!("unexpected argument {other:?}")),
            None => return Err("arguments must be valid UTF-8".to_owned()),
        }
    }

    let scope = scope.ok_or_else(|| "--scope is required".to_owned())?;
    let root = match root {
        Some(path) => path,
        None => {
            env::current_dir().map_err(|error| format!("cannot read current directory: {error}"))?
        }
    };
    check_repository(&root, scope).map_err(|error| error.to_string())?;
    Ok(RunOutcome::Checked(scope))
}

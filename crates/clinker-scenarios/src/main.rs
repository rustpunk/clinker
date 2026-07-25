//! Generates the input data for the scenario corpus under `examples/scenarios/`.
//!
//! Scenario inputs are not committed — they are reproducible bytes, and a diff
//! on them carries no reviewable information. Run this once after cloning to
//! populate every scenario's `data/` directory, then run any scenario with the
//! `clinker` CLI as its README describes.
//!
//! The scenario test harness does not depend on this binary: it generates into
//! a temporary directory through the library API, so running the suite never
//! mutates the working tree.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use clinker_scenarios::{GENERATOR_VERSION, Materialized, REGISTRY, materialize, scenario};

#[derive(Parser)]
#[command(
    name = "clinker-scenarios",
    about = "Generate input data for the examples/scenarios corpus",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Write each scenario's input files into its `data/` directory.
    Gen {
        /// Generate only this scenario (directory name, e.g. `01-storefront-orders`).
        #[arg(long)]
        scenario: Option<String>,
        /// Rewrite inputs even when the on-disk digest already matches.
        #[arg(long)]
        force: bool,
        /// Root holding the scenario directories.
        #[arg(long, default_value = "examples/scenarios")]
        root: PathBuf,
    },
    /// Print the scenario ladder.
    List,
}

fn main() -> ExitCode {
    match Cli::parse().command {
        Command::List => {
            for s in REGISTRY {
                println!("{:<28} {}", s.id, s.summary);
            }
            ExitCode::SUCCESS
        }
        Command::Gen {
            scenario: only,
            force,
            root,
        } => run_gen(only.as_deref(), force, &root),
    }
}

fn run_gen(only: Option<&str>, force: bool, root: &std::path::Path) -> ExitCode {
    let selected: Vec<_> = match only {
        Some(id) => match scenario(id) {
            Some(s) => vec![s],
            None => {
                eprintln!("unknown scenario '{id}'. Known scenarios:");
                for s in REGISTRY {
                    eprintln!("  {}", s.id);
                }
                return ExitCode::FAILURE;
            }
        },
        None => REGISTRY.iter().collect(),
    };

    for s in selected {
        let data = (s.generate)();
        let dir = root.join(s.id).join("data");
        match materialize(&data, &dir, force) {
            Ok(Materialized::Written) => {
                println!(
                    "{}: wrote {} file(s) to {} [v{GENERATOR_VERSION} {}]",
                    s.id,
                    data.files().len(),
                    dir.display(),
                    &data.digest()[..12]
                );
            }
            Ok(Materialized::UpToDate) => {
                println!("{}: up to date ({})", s.id, dir.display());
            }
            Err(e) => {
                eprintln!("{}: failed to write {}: {e}", s.id, dir.display());
                return ExitCode::FAILURE;
            }
        }
    }
    ExitCode::SUCCESS
}

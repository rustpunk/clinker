use std::process::ExitCode;

fn main() -> ExitCode {
    clinker_release_policy::cli::run_from(std::env::args_os())
}

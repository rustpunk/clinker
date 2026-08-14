//! Compiles one C file, unconditionally.
//!
//! This build script is the negative control for the `Build portability` CI job.
//! That job proves the workspace needs no C compiler by building it with every
//! C-compiler environment variable pointed at a program that fails — but a job
//! that only ever builds compliant code cannot distinguish "nothing compiles C"
//! from "the mechanism stopped working". This crate is the code that must fail,
//! and the job asserts that it does.
//!
//! It is never built by the workspace: the root manifest excludes it, so
//! `cargo build --workspace` and `cargo tree --workspace` do not see it.

fn main() {
    println!("cargo::rerun-if-changed=probe.c");
    cc::Build::new()
        .file("probe.c")
        .compile("clinker_no_c_gate_probe");
}

//! Negative control for the no-C-toolchain gate. Everything this crate does
//! happens in `build.rs`, which compiles `probe.c` through the `cc` crate.
//!
//! The library is empty because the gate is a build-time property: with a working
//! C compiler the crate builds, and under the `Build portability` job's
//! environment it must not. Nothing needs to be callable for that to be true, and
//! an exported function here would have no consumer.

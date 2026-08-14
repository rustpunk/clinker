//! Negative control for the no-C-toolchain gate. See `build.rs`.
//!
//! The library itself is empty on purpose. Everything this crate exists to prove
//! happens in its build script, before any Rust here is compiled: with a working
//! C compiler the crate builds, and with the `Build portability` job's environment
//! it must not.

/// Links the symbol `probe.c` provides, so the compiled object is not dead weight
/// the linker can discard without noticing it was never produced.
unsafe extern "C" {
    fn clinker_no_c_gate_probe() -> core::ffi::c_int;
}

/// Calls into the C translation unit the build script compiled.
///
/// Never invoked by the gate, which fails at build time. It exists so the C object
/// is genuinely linked rather than merely produced and discarded.
pub fn probe() -> i32 {
    unsafe { clinker_no_c_gate_probe() }
}

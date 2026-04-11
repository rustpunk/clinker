# Installation

Clinker is a single static binary with no runtime dependencies. Download it,
put it on your `PATH`, and you are ready to go.

## Binaries

Clinker ships two binaries:

- **`clinker`** -- the pipeline executor. This is the main tool you use to
  validate and run pipelines against data.
- **`cxl`** -- the CXL expression checker, evaluator, and formatter. Use it
  during development to test expressions interactively, check types, and
  format CXL blocks.

## Verify installation

After placing the binaries on your `PATH`, confirm they work:

```bash
clinker --version
```

```
clinker 0.1.0
```

```bash
cxl --version
```

```
cxl 0.1.0
```

Both commands should print a version string and exit. If you see
`command not found`, check that the directory containing the binaries is in
your `PATH`.

## Building from source

Clinker requires **Rust 1.91+** (edition 2024). If you have a Rust toolchain
installed, build and install both binaries directly from the repository:

```bash
# Clone the repository
git clone https://github.com/rustpunk/clinker.git
cd clinker

# Install the pipeline executor
cargo install --path crates/clinker

# Install the CXL expression tool
cargo install --path crates/cxl-cli
```

This compiles release-optimized binaries and places them in `~/.cargo/bin/`,
which is typically already on your `PATH`.

To verify the build:

```bash
cargo test --workspace
```

This runs the full test suite (approximately 1100 tests) and confirms
everything is working correctly on your system.

## Rust toolchain

The repository includes a `rust-toolchain.toml` that pins the exact Rust
version. If you use `rustup`, it will automatically download the correct
toolchain when you build.

| Requirement     | Value       |
|-----------------|-------------|
| Rust edition    | 2024        |
| Minimum version | 1.91        |
| C dependencies  | None        |

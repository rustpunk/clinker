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

A working C compiler is not one of the requirements: nothing in the build
graph runs one. TLS for the `rest` source and the OTLP exporter goes through
rustls with the [graviola] provider, which ships as Rust and inline assembly
rather than the C and per-architecture assembly that other providers build,
and content hashing uses blake3's pure-Rust SIMD paths. CI builds the
workspace with every C-compiler environment variable pointed at a program that
fails, so a dependency that starts needing one is caught rather than noticed
by whoever first builds without a compiler installed.

Graviola supports `x86_64` and `aarch64`. Those are the architectures Clinker
is built and tested on; another one needs a different rustls provider, and the
ones available today build C.

[graviola]: https://github.com/ctz/graviola/

## Optional capabilities

Three parts of Clinker are Cargo features of the `clinker` crate. All three are
**on by default** — a downloaded binary, or one built with plain
`cargo install --path crates/clinker`, has every one of them, and nothing in
this documentation assumes otherwise.

| Feature   | What it adds                                                      |
|-----------|-------------------------------------------------------------------|
| `rest`    | The `rest` source transport (`transport: rest` on a source node).  |
| `otlp`    | OTLP/HTTP export of logs, metrics and spans to a collector.        |
| `lineage` | OpenLineage emission: `--lineage` and `--lineage-events`.          |

They exist for deployments that want a smaller binary or a narrower dependency
graph — a build with no `rest` and no `otlp` links no HTTP client and no TLS
stack at all:

```bash
# File sources and lineage only: no network transport is compiled in.
cargo install --path crates/clinker --no-default-features --features lineage
```

A build without a capability still parses every construct the full one does.
What it will not do is run one silently: a pipeline that declares a `rest`
source, a `clinker.toml` that sets `observability.otlp.endpoint`, or a
`--lineage` flag is refused at validation — before any source is opened or any
output written — with a diagnostic that names what was asked for and says the
capability is not in this binary.

Turning `otlp` off also stops telemetry being *recorded*: the fixed arena is
reserved for an exporter to drain, so with no exporter there is nothing to
reserve it for, and the `--machine ndjson-v1` terminal then carries no
`observability` field at all.

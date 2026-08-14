# Pure-Rust Build: Findings

The record behind the workspace's no-C-toolchain guarantee: what was investigated,
what was chosen, what was measured, and what is still unknown. Written to close the
feasibility investigation that gated the migration, whose findings until now existed
only in issue comments and commit messages.

Sections follow that investigation's success criteria one for one, so a reader can
see which were met, which were met differently than asked, and which were not met at
all. Where a criterion was not discharged as written, the reason is stated here
rather than left to be inferred from its absence.

## 1. blake3 under `pure`: does anything invoke a C compiler?

No. `cc` appears in the lockfile as blake3's build-dependency and is compiled as an
ordinary Rust crate, but under the `pure` feature the workspace pins, blake3's build
script never invokes it.

The original framing of this check — "`cargo tree -i cc` is empty" — is not
satisfiable and was the wrong test. `cc` is an unconditional build-dependency of
blake3; its presence in the graph says nothing about whether a C compiler runs. The
property that matters is behavioral, so it is verified behaviorally: the
`Build portability` CI job builds with every C-compiler environment variable pointed
at a program that fails. A build script that invokes one fails there; one that only
declares the dependency does not.

Confirmed independently while measuring §3: with `CC=/bin/false`, a crate depending
on blake3 with default features fails to build (exit 101) and the same crate with
`pure` builds clean (exit 0). The feature is doing exactly what the manifest comment
says it does.

## 2. Ring-free TLS providers

`rustls-graviola` was chosen. It is published by the rustls maintainer, has no
`build.rs` at all, and embeds its assembly through Rust `global_asm!` from formally
verified s2n-bignum sources rather than compiling it with `cc`.

Rejected, with reasons:

- **`ring`** — the incumbent, and the reason the guarantee was false. Its build
  script compiles C and per-architecture assembly unconditionally.
- **`rustls-rustcrypto`** — a 0.0.2 alpha, last published 2024, self-described
  experimental, never independently audited, implementing only a subset of cipher
  suites. Not viable regardless of the C question.
- **`aws-lc-rs`** — builds C. Excluded by the pillar, not on its merits.

Accepted consequences, both deliberate:

- **Narrowed CPU support.** Graviola targets `x86_64` and `aarch64` only, and on
  `x86_64` requires AES-NI, AVX2, ADX, BMI2, and PCLMULQDQ — a runtime feature
  check, roughly 2014-and-later hardware. Every release target is covered, but a
  published `x86_64` binary now refuses to run on a pre-2014 CPU where it previously
  worked. A third architecture would need a different provider, and the ones
  available today build C.
- **Graviola is young.** Pinned rather than floated for that reason.

The provider is installed per agent in `crates/clinker-net/src/tls.rs`, never through
`CryptoProvider::install_default`, which is process-global and may only be called
once — a property a library has no right to spend on behalf of its embedder.

`ureq` is configured with `rustls-no-provider` plus `rustls-webpki-roots`, which is
ureq's `rustls` feature minus its ring dependency. Naming the two halves keeps the
Mozilla root store byte-identical while dropping ring; `rustls-no-provider` does not
drop the webpki roots, so no separate roots crate is needed.

**Cross-cutting finding for the SQL driver work.** The maintained synchronous MySQL
client's TLS features all terminate in C-compiling crates whatever provider is
installed — its `rustls` feature additionally enables a deprecated webpki crate that
pins ring. MySQL-over-TLS cannot be C-free through that crate, and no choice of
provider rescues it. Postgres carries no such constraint.

## 3. Performance deltas

### Hashing: measured, and it is not free

`pure` is a real cost. The manifest comment previously claimed it was "not a
performance compromise"; that claim was wrong and has been corrected.

Measured on one host — AMD Ryzen 9 7950X3D, rustc 1.96.0, single-threaded, streaming
1 MiB chunks to match `COPY_CHUNK_BYTES` in
`crates/clinker-channel/src/staging_copy.rs`, best of three passes over 2 GiB:

| build | dispatch | MiB/s | vs default |
|---|---|---:|---:|
| default | assembly + AVX-512 | 7933 | 1.00 |
| default, `no_avx512` | assembly + AVX2 | 5284 | 0.67 |
| **`pure`** (shipped) | Rust intrinsics + AVX2 | **4516** | **0.57** |
| `pure`, `no_avx2` | Rust intrinsics + SSE4.1 | 2414 | 0.30 |

The cost decomposes into two independent parts, which matters because only one of
them is CPU-dependent:

- **~15% is assembly versus Rust intrinsics** at the same instruction set (5284 →
  4516). This is paid on every x86_64 CPU.
- **~33% more is AVX-512**, which `pure` has no equivalent for (7933 → 5284). This
  is paid only where the hardware would have offered it.

So the headline 43% is close to the worst case, seen on an AVX-512-capable CPU. A
machine without AVX-512 pays only the first part.

The fourth row is a control rather than a result: `pure` with AVX2 forced off halves
throughput, which confirms `pure` really is dispatching SIMD at runtime rather than
falling back to scalar. That was the load-bearing claim in the manifest comment, and
it holds.

**What this does not cover.** One CPU, one microarchitecture, one compiler version,
and hashing in isolation. It says nothing about the end-to-end staged-copy path,
where hashing shares the run with disk. Note that `verify: blake3` versus `none` does
not isolate hashing either: `copy_into` hashes unconditionally, and the verify branch
adds a full re-read plus a second hash, so that delta is I/O-dominated. An
end-to-end envelope at 0.5–2 GB belongs with the large-file qualification work, not
here.

The measurement was taken in a throwaway crate outside the repository, because the
comparison cannot live in the tree: Cargo unions features per package-version per
resolve, so two blake3 edges in one manifest collapse into a single `pure` build unit
and both arms measure the same code. Two separate resolves are required, and the
non-`pure` arm needs a C toolchain — which the workspace's own CI is built to refuse.
It is therefore not reproducible in CI by construction, and is recorded as dated
evidence rather than as a gate.

### TLS: not dischargeable, and closed on that basis

The investigation asked for REST throughput and handshake latency against `ring`.
That comparison is not available to this repository, and the criterion is closed
unmet rather than quietly dropped.

Measuring the `ring` arm requires reintroducing `ring` to a resolvable graph and
building it, which needs a C toolchain. Both are precisely what the pillar exists to
prevent: it would take a detached crate carrying `ring` in its own lockfile plus a CI
job deliberately exempted from the no-C environment. And no delta of any magnitude
could change the outcome, because `ring` is disqualified on how it builds, not on how
fast it runs. Paying four approval-gated capabilities for a number that cannot move a
decision is not a trade worth making.

**Consequence, stated plainly: there is no TLS performance envelope.** A future
change to the TLS path has no baseline to be measured against, and no regression in
it would be detected. This is recorded in
[80_OPEN_QUESTIONS.md](80_OPEN_QUESTIONS.md).

An absolute handshake-latency baseline for graviola alone — no `ring` arm — is
possible and would be the useful artifact, but it needs a loopback TLS server, which
means a `rustls` dev-dependency and a committed certificate fixture with an expiry.
That is a scoped piece of work with its own approval, deliberately not smuggled in
here.

## 4. The enforcing gate

`deny.toml` is not the gate and does not claim to be. A name blocklist cannot
distinguish a build script that runs a C compiler from one that merely declares `cc`
and never invokes it, and `blake3` under `pure` is exactly the second case — banning
`cc` by name would refuse a graph that compiles no C. `cmake` remains banned by name
because, unlike `cc`, its presence in a graph is not separable from running it.

The gate is the `Build portability` job in `.github/workflows/ci.yml`:

1. Build the workspace and every test and benchmark target with `CC`, `CXX`, `AR`,
   and their target-triple variants pointed at `/bin/false`. This is the scope the
   README and installation guide claim, so the claim and the check now describe the
   same thing.
2. Build `tools/no-c-gate-fixture` — a crate excluded from the workspace whose build
   script compiles a C file through the real `cc` crate — with a working toolchain,
   where it must succeed, and then under the failing environment, where it must fail
   *in `cc`*. A fixture that cannot build at all, or that fails while fetching, would
   otherwise report a working gate while proving nothing.
3. Resolve `cargo tree --workspace --all-features --target all --invert ring` and
   fail if it produces output. `--target all` because a ring edge reachable only from
   the Windows or macOS resolution is still a ring edge.

Step 2 is what the investigation's "documented mechanism rather than a name
blocklist" asks for, taken one step further: the mechanism is not only documented but
continuously demonstrated. Step 3 reads the command's own exit status rather than
piping it into `grep`, which reports grep's status and would turn a broken
`cargo tree` into a silent pass.

The fixture is isolated by construction. It has its own `[workspace]` table and is
listed in the root manifest's `exclude`, so it is absent from the root lockfile that
the dependency-policy digest covers, and invisible to `cargo tree --workspace`. The
one crate in the repository that compiles C cannot contaminate the graph it exists to
protect.

## 5. Status of the original criteria

| Criterion | Status |
|---|---|
| blake3 `pure` C-invocation answered with build evidence | Met, behaviorally rather than by graph inspection (§1) |
| Two or more ring-free TLS candidates evaluated, with rejection reasons | Met (§2) |
| Perf deltas quantified for hashing and TLS | Hashing met (§3); TLS closed as not dischargeable, with the gap recorded |
| Gate design documented, distinguishing vendored-C metadata from real compilation | Met, and implemented (§4) |
| Findings document committed and linked | This document |

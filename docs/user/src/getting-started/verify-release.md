# Verify a Release

Verify both the archive checksum and its build attestation before running a
downloaded Clinker binary. A checksum detects changed bytes; the attestation
also binds those bytes to this repository and its release workflow.

## Download one platform archive

Replace `vX.Y.Z` and the archive name with the release you intend to install:

```bash
gh release download vX.Y.Z \
  --repo rustpunk/clinker \
  --pattern 'clinker-vX.Y.Z-x86_64-unknown-linux-gnu.tar.gz*'
```

Each native archive has a sibling `.sha256` file. The release also contains a
`SHA256SUMS` inventory covering all supported archives.

## Check the SHA-256 digest

On Linux:

```bash
sha256sum --check clinker-vX.Y.Z-x86_64-unknown-linux-gnu.tar.gz.sha256
```

On macOS:

```bash
shasum -a 256 -c clinker-vX.Y.Z-aarch64-apple-darwin.tar.gz.sha256
```

On Windows PowerShell:

```powershell
$archive = "clinker-vX.Y.Z-x86_64-pc-windows-msvc.zip"
$expected = (Get-Content "$archive.sha256").Split()[0].ToLowerInvariant()
$actual = (Get-FileHash -Algorithm SHA256 $archive).Hash.ToLowerInvariant()
if ($actual -ne $expected) { throw "Clinker archive checksum mismatch" }
```

Stop if the checksum command fails. Do not extract or run the archive.

## Verify build provenance

With a current GitHub CLI and network access:

```bash
gh attestation verify \
  clinker-vX.Y.Z-x86_64-unknown-linux-gnu.tar.gz \
  --repo rustpunk/clinker
```

The verification must identify `rustpunk/clinker` as the source repository and
the release archive itself as the attested subject. An attestation proves where
and how the bytes were built; it is not a claim that the program is free of
security defects.

If either checksum or provenance verification fails, keep the archive
quarantined and report the release tag, archive name, and failing command.

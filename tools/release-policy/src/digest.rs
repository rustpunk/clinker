//! Exact Git blob SHA-1 and canonical evidence SHA-256 helpers.

use sha1::{Digest as _, Sha1};
use sha2::Sha256;

/// Hash bytes using Git's exact `blob {len}\0{bytes}` object identity.
#[must_use]
pub fn git_blob_sha1_hex(bytes: &[u8]) -> String {
    let length = bytes.len().to_string();
    let mut hasher = Sha1::new();
    hasher.update(b"blob ");
    hasher.update(length.as_bytes());
    hasher.update([0]);
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

/// Hash exact bytes using SHA-256.
#[must_use]
pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

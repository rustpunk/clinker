//! Exact Git blob SHA-1 and canonical evidence SHA-256 helpers.

use std::io::{self, Read};

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

/// Hash a byte stream using SHA-256 without retaining the stream in memory.
///
/// # Errors
///
/// Returns an I/O error if the stream cannot be read to completion.
pub fn sha256_reader(mut reader: impl Read) -> io::Result<String> {
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

/// Hash a byte stream while enforcing the maximum admitted byte count.
///
/// The returned length is derived from bytes actually read rather than file
/// metadata, so a concurrent mutation cannot bypass a pre-read size check.
///
/// # Errors
///
/// Returns an I/O error if the stream cannot be read or exceeds `byte_limit`.
pub fn sha256_reader_bounded(mut reader: impl Read, byte_limit: u64) -> io::Result<(u64, String)> {
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    let mut length = 0_u64;
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        length = length
            .checked_add(read as u64)
            .ok_or_else(|| io::Error::other("SHA-256 input length overflowed"))?;
        if length > byte_limit {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "SHA-256 input exceeded its byte limit",
            ));
        }
        hasher.update(&buffer[..read]);
    }
    Ok((length, format!("{:x}", hasher.finalize())))
}

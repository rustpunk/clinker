//! Fail-closed release and repository policy checks.

#![forbid(unsafe_code)]
#![deny(clippy::correctness)]

pub mod bundle;
pub mod canonical;
pub mod child;
pub mod cli;
pub mod decision;
pub mod digest;
pub mod error;
pub mod evidence;
pub mod filesystem;
pub mod inventory;
pub mod limits;
pub mod release;

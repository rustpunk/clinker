//! The `rest` finite-pull source, behind the `transport` feature.
//!
//! Split in two by what each half decides. [`continuation`] decides which
//! server-offered URL the reader is allowed to follow next — origins, `Link`
//! headers and redirect targets, no client involved — and [`source`] is the
//! request loop that acts on that verdict. Both are gated: continuation policy
//! answers a question only the request loop asks, so a build without the
//! transport has nothing to ask it.

#[cfg(feature = "transport")]
mod continuation;
#[cfg(feature = "transport")]
mod source;

#[cfg(feature = "transport")]
pub(crate) use source::RestRecordSource;

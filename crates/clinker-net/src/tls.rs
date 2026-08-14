//! The TLS configuration every agent in this crate is built with.
//!
//! One definition rather than one per transport: the provider choice below is
//! load-bearing, and a second copy of it is a copy that can be edited alone.

/// Build the TLS configuration for a `ureq` agent.
///
/// The crypto provider is set on the agent rather than through
/// `CryptoProvider::install_default`, which writes a process-global that
/// refuses every call after the first — a library deciding TLS for the whole
/// process, including code that never asked it to.
///
/// ureq falls back to ring when no provider is configured. The workspace
/// builds ureq with ring's feature off, which turns that fallback into a
/// panic, so this is what stops it being reached — not a preference between
/// two working providers.
///
/// Root certificates are left at ureq's default, the compiled-in Mozilla
/// webpki set, which is what these transports verified against before the
/// provider was named explicitly.
pub(crate) fn config() -> ureq::tls::TlsConfig {
    ureq::tls::TlsConfig::builder()
        .provider(ureq::tls::TlsProvider::Rustls)
        .unversioned_rustls_crypto_provider(
            std::sync::Arc::new(rustls_graviola::default_provider()),
        )
        .build()
}

// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Shared rustls crypto-provider initialization.
//!
//! This crate provides a single, idempotent function to install a process-wide
//! rustls [`CryptoProvider`](rustls::crypto::CryptoProvider) selected by
//! compile-time feature flags.
//!
//! | Feature          | Backend          | Use-case                           |
//! |------------------|------------------|------------------------------------|
//! | `crypto-ring`    | `ring`           | Default, backward-compatible       |
//! | `crypto-aws-lc`  | `aws-lc-rs`      | AWS environments, broader algos    |
//! | `crypto-openssl` | `rustls-openssl` | Regulated / FIPS environments      |
//! | `crypto-symcrypt`| `rustls-symcrypt`| Microsoft/SymCrypt-aligned backend |

use std::sync::OnceLock;

/// Installs the configured rustls crypto provider (once, idempotently).
///
/// When multiple `crypto-*` features are enabled (e.g. `--all-features`), the
/// priority order is: openssl > aws-lc > ring > symcrypt.
///
/// This function is safe to call from any number of sites — the actual
/// installation happens at most once per process.
///
/// # Errors
///
/// Returns `Err` if no `crypto-*` feature is enabled.
pub fn ensure_crypto_provider() -> Result<(), String> {
    static INIT: OnceLock<Result<(), String>> = OnceLock::new();

    INIT.get_or_init(|| {
        cfg_if::cfg_if! {
            if #[cfg(feature = "crypto-openssl")] {
                let _ = rustls_openssl::default_provider().install_default();
                Ok(())
            } else if #[cfg(feature = "crypto-aws-lc")] {
                let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
                Ok(())
            } else if #[cfg(feature = "crypto-ring")] {
                let _ = rustls::crypto::ring::default_provider().install_default();
                Ok(())
            } else if #[cfg(feature = "crypto-symcrypt")] {
                let _ = rustls_symcrypt::default_symcrypt_provider().install_default();
                Ok(())
            } else {
                Err(
                    "TLS support requires one of the crypto features: \
                     crypto-ring, crypto-aws-lc, crypto-openssl, or crypto-symcrypt"
                        .to_string(),
                )
            }
        }
    })
    .clone()
}

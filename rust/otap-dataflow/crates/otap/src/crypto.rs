// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Cryptographic provider initialization for rustls.
//!
//! This module delegates to [`otap_df_crypto`] which centralizes the
//! installation of the process-wide rustls
//! [`CryptoProvider`](rustls::crypto::CryptoProvider) based on compile-time
//! feature flags.
//!
//! | Feature          | Backend          | Use-case                           |
//! |------------------|------------------|------------------------------------|
//! | `crypto-ring`    | `ring`           | Default, backward-compatible       |
//! | `crypto-aws-lc`  | `aws-lc-rs`      | AWS environments, broader algos    |
//! | `crypto-openssl` | `rustls-openssl` | Regulated / FIPS environments      |
//! | `crypto-symcrypt`| `rustls-symcrypt`| Microsoft/SymCrypt-aligned backend |

/// Installs the selected rustls `CryptoProvider` as the process-wide default.
///
/// See [`otap_df_crypto::ensure_crypto_provider`] for details.
///
/// # Errors
///
/// Returns `Err` if no `crypto-*` feature is enabled.
pub fn install_crypto_provider() -> Result<(), String> {
    otap_df_crypto::ensure_crypto_provider()
}

/// Idempotent crypto provider installation (intended for test setup).
///
/// Equivalent to [`install_crypto_provider`] but discards the result, which is
/// convenient in test helpers where a provider should simply be present.
pub fn ensure_crypto_provider() {
    let _ = install_crypto_provider();
}

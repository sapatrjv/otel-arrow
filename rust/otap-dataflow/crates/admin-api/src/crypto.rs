// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Re-exports the shared crypto-provider initialization.

/// Installs the configured rustls crypto provider (once, idempotently).
///
/// Delegates to [`otap_df_crypto::ensure_crypto_provider`].
pub fn ensure_crypto_provider() -> Result<(), String> {
    otap_df_crypto::ensure_crypto_provider()
}

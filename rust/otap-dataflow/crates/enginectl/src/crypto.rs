// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Crypto-provider bootstrap for the admin SDK client.
//!
//! Delegates to the shared implementation in `otap-df-admin-api`.

use crate::error::CliError;

/// Install the configured Rustls crypto provider once for the process.
pub(crate) fn ensure_crypto_provider() -> Result<(), CliError> {
    otap_df_admin_api::crypto::ensure_crypto_provider().map_err(CliError::config)
}

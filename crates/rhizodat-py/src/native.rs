//! The native stores a Python Unit of Work can hand to the coordinator.
//!
//! Each real Rust adapter (PostgreSQL, drevo, …) is wrapped as a [`NativeStore`]
//! so the coordinator drives them all through one path: register the
//! participant, open the branch inside the commit's runtime, run the protocol
//! with no Python callbacks involved. A store that cannot open its branch
//! reports a [`ConnectError`] carrying the driver's retryability, which the
//! coordinator turns into a retryable-aware `AbortedError`.

use std::error::Error;
use std::sync::Arc;

use async_trait::async_trait;
use rhizodat_core::store::{BoxError, Branch, Participant};

/// A store's branch could not be opened; nothing was staged or journaled.
pub(crate) struct ConnectError {
    pub error: BoxError,
    pub retryable: bool,
}

/// One real Rust store, ready to join a meta-transaction.
#[async_trait]
pub(crate) trait NativeStore: Send + Sync {
    /// The store name (the key of its branch and participant).
    fn name(&self) -> String;
    /// The store's participant, shared for the finish phase and recovery.
    fn participant(&self) -> Arc<dyn Participant>;
    /// Open a branch with a fresh transaction, inside the commit's runtime.
    async fn open_branch(&self) -> Result<Box<dyn Branch>, ConnectError>;
}

/// The PostgreSQL adapter as a native store.
pub(crate) struct Postgres(pub Arc<rhizodat_postgres::PostgresStore>);

#[async_trait]
impl NativeStore for Postgres {
    fn name(&self) -> String {
        self.0.name().to_owned()
    }

    fn participant(&self) -> Arc<dyn Participant> {
        self.0.participant()
    }

    async fn open_branch(&self) -> Result<Box<dyn Branch>, ConnectError> {
        match self.0.branch().await {
            Ok(branch) => Ok(Box::new(branch)),
            Err(error) => Err(ConnectError {
                retryable: rhizodat_postgres::is_retryable(&error),
                error: Box::new(error) as BoxError,
            }),
        }
    }
}

/// The ClickHouse adapter as a native store. Its branch is one-phase and opens
/// without I/O (the HTTP client is lazy), so `open_branch` cannot fail.
pub(crate) struct ClickHouse(pub Arc<rhizodat_clickhouse::ClickHouseStore>);

#[async_trait]
impl NativeStore for ClickHouse {
    fn name(&self) -> String {
        self.0.name().to_owned()
    }

    fn participant(&self) -> Arc<dyn Participant> {
        self.0.participant()
    }

    async fn open_branch(&self) -> Result<Box<dyn Branch>, ConnectError> {
        Ok(Box::new(self.0.branch()))
    }
}

/// The drevo adapter as a native store.
pub(crate) struct Drevo(pub Arc<rhizodat_drevo::DrevoStore>);

#[async_trait]
impl NativeStore for Drevo {
    fn name(&self) -> String {
        self.0.name().to_owned()
    }

    fn participant(&self) -> Arc<dyn Participant> {
        self.0.participant()
    }

    async fn open_branch(&self) -> Result<Box<dyn Branch>, ConnectError> {
        match self.0.branch().await {
            Ok(branch) => Ok(Box::new(branch)),
            Err(error) => Err(ConnectError {
                retryable: self.0.is_retryable(error_ref(&error)),
                error,
            }),
        }
    }
}

/// Borrow a `BoxError` as a `&dyn Error`, for a store's `is_retryable`.
fn error_ref(error: &BoxError) -> &(dyn Error + 'static) {
    &**error
}

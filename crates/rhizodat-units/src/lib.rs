//! A unit of work of native stores, committed by the core: the glue every
//! binding shares (the Python module `rhizodat_native` and the C ABI for Go).
//!
//! Each real Rust adapter (PostgreSQL, ClickHouse, drevo, Iceberg) is wrapped as
//! a [`NativeStore`] so the coordinator drives them all through one path:
//! register the participant, open the branch inside the commit's runtime, run
//! the protocol. The `*Pending` stores carry a unit of work's own buffered
//! writes and load them into the branch they open. A store that cannot open its
//! branch reports a [`ConnectError`] carrying the driver's retryability.
//!
//! [`commit`] runs a unit of work made only of native stores; a binding that
//! mixes in branches of its own (the Python one) drives the coordinator itself.

use std::error::Error;
use std::sync::Arc;

use async_trait::async_trait;
use rhizodat_core::clock::HybridLogicalClock;
use rhizodat_core::coordinator::{CommitError, Coordinator, Outcome};
use rhizodat_core::journal::{BranchRecord, Journal};
use rhizodat_core::store::{BoxError, Branch, Participant, Participants};
use rhizodat_core::timer::TokioTimer;
use uuid::Uuid;

/// A store's branch could not be opened; nothing was staged or journaled.
pub struct ConnectError {
    /// The driver's error.
    pub error: BoxError,
    /// Whether a re-run of the whole commit may succeed.
    pub retryable: bool,
}

/// One real Rust store, ready to join a meta-transaction.
#[async_trait]
pub trait NativeStore: Send + Sync {
    /// The store name (the key of its branch and participant).
    fn name(&self) -> String;
    /// The store's participant, shared for the finish phase and recovery.
    fn participant(&self) -> Arc<dyn Participant>;
    /// Open a branch with a fresh transaction, inside the commit's runtime.
    async fn open_branch(&self) -> Result<Box<dyn Branch>, ConnectError>;
}

/// The PostgreSQL adapter as a native store.
pub struct Postgres(pub Arc<rhizodat_postgres::PostgresStore>);

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

/// The PostgreSQL adapter as a native store that also carries a Unit of Work's
/// own rows: `open_branch` opens the transaction and loads the buffered rows, so
/// they are written at stage inside the two-phase commit. The rows are cloned on
/// open, so a retry of the whole commit opens a fresh branch with the same rows.
pub struct PostgresPending {
    /// The store.
    pub store: Arc<rhizodat_postgres::PostgresStore>,
    /// The unit of work's rows for application tables.
    pub rows: rhizodat_postgres::PostgresRows,
    /// The unit of work's entity parts.
    pub parts: rhizodat_postgres::parts::PostgresParts,
}

#[async_trait]
impl NativeStore for PostgresPending {
    fn name(&self) -> String {
        self.store.name().to_owned()
    }

    fn participant(&self) -> Arc<dyn Participant> {
        self.store.participant()
    }

    async fn open_branch(&self) -> Result<Box<dyn Branch>, ConnectError> {
        match self.store.branch().await {
            Ok(mut branch) => {
                branch.load(self.rows.clone());
                branch.load_parts(self.parts.clone());
                Ok(Box::new(branch))
            }
            Err(error) => Err(ConnectError {
                retryable: rhizodat_postgres::is_retryable(&error),
                error: Box::new(error) as BoxError,
            }),
        }
    }
}

/// The ClickHouse adapter as a native store. Its branch is one-phase and opens
/// without I/O (the HTTP client is lazy), so `open_branch` cannot fail.
pub struct ClickHouse(pub Arc<rhizodat_clickhouse::ClickHouseStore>);

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
pub struct Drevo(pub Arc<rhizodat_drevo::DrevoStore>);

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

/// The drevo adapter as a native store that also carries a Unit of Work's own
/// graph writes: `open_branch` opens the transaction and loads the buffered
/// statements, so they run at stage inside the two-phase commit. The writes are
/// cloned on open, so a retry of the whole commit opens a fresh branch with the
/// same statements.
pub struct DrevoPending {
    /// The store.
    pub store: Arc<rhizodat_drevo::DrevoStore>,
    /// The unit of work's graph statements.
    pub writes: rhizodat_drevo::DrevoWrites,
    /// The unit of work's entity parts.
    pub parts: rhizodat_drevo::parts::DrevoParts,
}

#[async_trait]
impl NativeStore for DrevoPending {
    fn name(&self) -> String {
        self.store.name().to_owned()
    }

    fn participant(&self) -> Arc<dyn Participant> {
        self.store.participant()
    }

    async fn open_branch(&self) -> Result<Box<dyn Branch>, ConnectError> {
        match self.store.branch().await {
            Ok(mut branch) => {
                branch.load(self.writes.clone());
                branch.load_parts(self.parts.clone());
                Ok(Box::new(branch))
            }
            Err(error) => Err(ConnectError {
                retryable: self.store.is_retryable(error_ref(&error)),
                error,
            }),
        }
    }
}

/// Borrow a `BoxError` as a `&dyn Error`, for a store's `is_retryable`.
fn error_ref(error: &BoxError) -> &(dyn Error + 'static) {
    &**error
}

/// An Iceberg catalog connected on first use, inside the commit's runtime.
///
/// Connecting to a REST catalog is I/O, and its HTTP client is bound to the
/// runtime that made it, so a native Iceberg store keeps only its DSN and each
/// commit connects once on its own current-thread runtime, shared by the branch
/// and the participant of that commit.
#[derive(Clone)]
pub struct IcebergLake {
    /// The catalog DSN.
    pub dsn: Arc<str>,
    /// The store name.
    pub name: Arc<str>,
    /// The store, connected on first use.
    pub store: Arc<tokio::sync::OnceCell<rhizodat_iceberg::IcebergStore>>,
}

impl IcebergLake {
    /// A lake for one commit (not yet connected).
    #[must_use]
    pub fn new(dsn: Arc<str>, name: Arc<str>) -> Self {
        Self {
            dsn,
            name,
            store: Arc::new(tokio::sync::OnceCell::new()),
        }
    }

    /// The store, connecting on the first call.
    ///
    /// # Errors
    ///
    /// When the catalog cannot be reached.
    pub async fn connect(&self) -> Result<&rhizodat_iceberg::IcebergStore, BoxError> {
        self.store
            .get_or_try_init(|| rhizodat_iceberg::IcebergStore::connect(&self.dsn, &self.name))
            .await
    }
}

/// The Iceberg adapter as a native store, with a Unit of Work's own record
/// batches (none for a bare store): `open_branch` connects and loads the batches,
/// so stage writes them as Parquet files and the participant appends them after
/// the decision. The batches are cloned on open, so a retry of the whole commit
/// opens a fresh branch with the same batches.
pub struct IcebergPending {
    /// The lake, connected inside the commit.
    pub lake: IcebergLake,
    /// The unit of work's record batches for application tables.
    pub batches: rhizodat_iceberg::IcebergBatches,
    /// The unit of work's entity parts.
    pub parts: rhizodat_iceberg::parts::IcebergParts,
}

#[async_trait]
impl NativeStore for IcebergPending {
    fn name(&self) -> String {
        self.lake.name.to_string()
    }

    fn participant(&self) -> Arc<dyn Participant> {
        Arc::new(IcebergLakeParticipant(self.lake.clone()))
    }

    async fn open_branch(&self) -> Result<Box<dyn Branch>, ConnectError> {
        match self.lake.connect().await {
            Ok(store) => {
                let mut branch = store.branch();
                branch.load(self.batches.clone());
                branch.load_parts(self.parts.clone());
                Ok(Box::new(branch))
            }
            Err(error) => Err(ConnectError {
                retryable: rhizodat_iceberg::is_retryable(error_ref(&error)),
                error,
            }),
        }
    }
}

/// The Iceberg participant over a lake that connects on first use.
struct IcebergLakeParticipant(IcebergLake);

#[async_trait]
impl Participant for IcebergLakeParticipant {
    async fn finish(
        &self,
        branch: &BranchRecord,
        dat_txn: Uuid,
        dat_tx_time: i64,
    ) -> Result<(), BoxError> {
        let store = self.0.connect().await?;
        store
            .participant()
            .finish(branch, dat_txn, dat_tx_time)
            .await
    }

    async fn is_applied(&self, branch: &BranchRecord, dat_txn: Uuid) -> Result<bool, BoxError> {
        let store = self.0.connect().await?;
        store.participant().is_applied(branch, dat_txn).await
    }

    async fn abort(&self, branch: &BranchRecord) -> Result<(), BoxError> {
        let store = self.0.connect().await?;
        store.participant().abort(branch).await
    }

    async fn prepared_gids(&self) -> Result<Vec<String>, BoxError> {
        // One-phase: an Iceberg store never holds prepared transactions.
        Ok(Vec::new())
    }

    fn is_retryable(&self, error: &(dyn Error + 'static)) -> bool {
        rhizodat_iceberg::is_retryable(error)
    }
}

/// A current-thread tokio runtime for one native call: it drives the async
/// adapters and the connection tasks they spawn on the calling thread, so a
/// native commit stays single-threaded.
///
/// # Errors
///
/// When the runtime cannot be built (the operating system refuses its I/O driver).
pub fn runtime() -> std::io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
}

/// Why a unit of work of native stores did not commit.
#[derive(Debug)]
pub enum UnitError {
    /// A store's branch could not be opened: nothing was staged or journaled.
    Connect(ConnectError),
    /// The commit protocol's own outcome (aborted, in doubt, invalid).
    Commit(CommitError),
}

impl std::fmt::Debug for ConnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectError")
            .field("error", &self.error.to_string())
            .field("retryable", &self.retryable)
            .finish()
    }
}

/// Commit `stores` (each with its buffered writes) as the meta-transaction
/// `dat_txn`, through `journal` when there are several, inside the caller's
/// runtime. Every branch is opened first, so each connection lives for the
/// whole commit; the real timer backs the retry policy.
///
/// # Errors
///
/// [`UnitError::Connect`] when a branch cannot be opened, and
/// [`UnitError::Commit`] with the protocol's outcome otherwise.
pub async fn commit(
    journal: Option<Arc<dyn Journal>>,
    clock: Arc<HybridLogicalClock>,
    dat_txn: Uuid,
    stores: &[Arc<dyn NativeStore>],
) -> Result<Outcome, UnitError> {
    let mut participants = Participants::new();
    for store in stores {
        participants = participants.with(store.name(), store.participant());
    }
    let mut branches: Vec<Box<dyn Branch>> = Vec::with_capacity(stores.len());
    for store in stores {
        branches.push(store.open_branch().await.map_err(UnitError::Connect)?);
    }
    Coordinator::new(
        journal,
        participants,
        clock,
        Arc::new(TokioTimer::default()),
    )
    .commit(dat_txn, &mut branches)
    .await
    .map_err(UnitError::Commit)
}

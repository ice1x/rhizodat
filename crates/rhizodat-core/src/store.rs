//! What the coordinator needs from a store.
//!
//! A [`Branch`] is one store's part of one meta-transaction and lives as long
//! as the Unit of Work. A [`Participant`] is the store as seen after the
//! decision and by recovery, and outlives Units of Work.

use std::error::Error;

use async_trait::async_trait;
use uuid::Uuid;

use crate::journal::BranchRecord;

/// Any store or driver error.
pub type BoxError = Box<dyn Error + Send + Sync + 'static>;

/// One store's part of one meta-transaction.
#[async_trait]
pub trait Branch: Send {
    /// The store name.
    fn store(&self) -> &str;

    /// Whether the store prepares (2PC) or applies its payload after the decision.
    fn two_phase(&self) -> bool;

    /// Slow, lock-free work. Returns a payload to keep in the journal, if any.
    async fn stage(&mut self, dat_txn: Uuid, dat_tx_time: i64)
    -> Result<Option<Vec<u8>>, BoxError>;

    /// Lock-holding prepare (two-phase branches only).
    async fn prepare(&mut self, gid: &str) -> Result<(), BoxError>;

    /// Native commit when this is the only branch.
    async fn commit_one_phase(&mut self) -> Result<(), BoxError>;

    /// Undo before the decision: roll back an open or prepared transaction.
    async fn rollback(&mut self) -> Result<(), BoxError>;

    /// Whether re-running the Unit of Work may get past `error`.
    fn is_retryable(&self, error: &(dyn Error + 'static)) -> bool;
}

/// A store after the decision and during recovery.
#[async_trait]
pub trait Participant: Send + Sync {
    /// Make the branch visible (`COMMIT PREPARED`, snapshot commit, deduplicated
    /// insert). Idempotent; an unknown gid means already resolved.
    async fn finish(
        &self,
        branch: &BranchRecord,
        dat_txn: Uuid,
        dat_tx_time: i64,
    ) -> Result<(), BoxError>;

    /// Whether the branch is already visible, to resolve an ambiguous commit.
    async fn is_applied(&self, branch: &BranchRecord, dat_txn: Uuid) -> Result<bool, BoxError>;

    /// Roll back a prepared gid or drop staged data. Idempotent.
    async fn abort(&self, branch: &BranchRecord) -> Result<(), BoxError>;

    /// Prepared transactions currently held by the store.
    async fn prepared_gids(&self) -> Result<Vec<String>, BoxError>;

    /// Whether trying `finish` again may succeed.
    fn is_retryable(&self, error: &(dyn Error + 'static)) -> bool;
}

/// The participants of every store a coordinator or recovery may meet, by
/// store name, in registration order (the order recovery sweeps them).
#[derive(Clone, Default)]
pub struct Participants(Vec<(String, std::sync::Arc<dyn Participant>)>);

impl std::fmt::Debug for Participants {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(self.0.iter().map(|(name, _)| name))
            .finish()
    }
}

impl Participants {
    /// No participants.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register (or replace) the participant of `store`.
    #[must_use]
    pub fn with(
        mut self,
        store: impl Into<String>,
        participant: std::sync::Arc<dyn Participant>,
    ) -> Self {
        let store = store.into();
        self.0.retain(|(name, _)| *name != store);
        self.0.push((store, participant));
        self
    }

    /// The participant of `store`.
    #[must_use]
    pub fn get(&self, store: &str) -> Option<&std::sync::Arc<dyn Participant>> {
        self.0
            .iter()
            .find(|(name, _)| name == store)
            .map(|(_, p)| p)
    }

    /// Store names and participants in registration order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &std::sync::Arc<dyn Participant>)> {
        self.0.iter().map(|(name, p)| (name.as_str(), p))
    }
}

impl FromIterator<(String, std::sync::Arc<dyn Participant>)> for Participants {
    fn from_iter<I: IntoIterator<Item = (String, std::sync::Arc<dyn Participant>)>>(
        iter: I,
    ) -> Self {
        iter.into_iter()
            .fold(Self::new(), |all, (name, p)| all.with(name, p))
    }
}

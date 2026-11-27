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

/// A write based on a version of an entity that is no longer the latest: the
/// version precondition a branch checks under the entity's lock at stage.
///
/// Not retryable: re-running the Unit of Work would write the same stale
/// entity again. Read it again and redo the change.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VersionConflict {
    /// The entity type.
    pub type_name: String,
    /// The entity.
    pub dat_id: Uuid,
    /// The version the write was based on (its `dat_tx_time`).
    pub expected: i64,
    /// The latest version's `dat_tx_time`; `None`: no live version.
    pub actual: Option<i64>,
}

impl VersionConflict {
    /// `Err` unless `actual` is `expected`.
    ///
    /// # Errors
    ///
    /// The conflict, when the latest live version is not the expected one.
    pub fn check(
        type_name: &str,
        dat_id: Uuid,
        expected: i64,
        actual: Option<i64>,
    ) -> Result<(), Self> {
        if actual == Some(expected) {
            return Ok(());
        }
        Err(Self {
            type_name: type_name.to_owned(),
            dat_id,
            expected,
            actual,
        })
    }
}

impl std::fmt::Display for VersionConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {}: written from version {}, but ",
            self.type_name, self.dat_id, self.expected
        )?;
        match self.actual {
            None => write!(f, "it is deleted"),
            Some(actual) => write!(f, "the latest is {actual}"),
        }
    }
}

impl Error for VersionConflict {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_version_conflict_reads_like_the_reference() {
        let id = Uuid::from_u128(7);
        assert_eq!(VersionConflict::check("t.T", id, 5, Some(5)), Ok(()));
        let moved = VersionConflict::check("t.T", id, 5, Some(9)).unwrap_err();
        assert_eq!(
            moved.to_string(),
            format!("t.T {id}: written from version 5, but the latest is 9")
        );
        let gone = VersionConflict::check("t.T", id, 5, None).unwrap_err();
        assert!(gone.to_string().ends_with("but it is deleted"), "{gone}");
        assert_eq!((gone.expected, gone.actual), (5, None));
        let boxed: BoxError = Box::new(moved.clone());
        assert_eq!(boxed.downcast_ref::<VersionConflict>(), Some(&moved));
    }
}

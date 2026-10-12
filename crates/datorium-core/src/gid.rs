//! Global ids of meta-transaction branches: `dat:<dat_txn>:<store>`.
//!
//! A gid names one branch in the store's own list of prepared transactions
//! (PostgreSQL `pg_prepared_xacts`, drevo `drevo.tx.list_prepared`), so
//! recovery can tell datorium's branches from anyone else's.

use uuid::Uuid;

const PREFIX: &str = "dat:";

/// The gid of the branch of `dat_txn` in `store`.
#[must_use]
pub fn gid_for(dat_txn: Uuid, store: &str) -> String {
    format!("{PREFIX}{}:{store}", dat_txn.hyphenated())
}

/// `(dat_txn, store)` for a gid written by [`gid_for`], `None` for anything else.
///
/// Only the spelling `gid_for` writes is ours: the same UUID in upper case, in
/// braces or without hyphens is a foreign gid.
#[must_use]
pub fn parse_gid(gid: &str) -> Option<(Uuid, String)> {
    let (txn, store) = gid.strip_prefix(PREFIX)?.split_once(':')?;
    if store.is_empty() {
        return None;
    }
    let parsed = Uuid::try_parse(txn).ok()?;
    (parsed.hyphenated().to_string() == txn).then(|| (parsed, store.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_a_store_name_with_colons() {
        let txn = Uuid::from_u128(0x0192_0000_0000_7000_8000_0000_0000_00ab);
        let gid = gid_for(txn, "orders:pg");
        assert_eq!(gid, "dat:01920000-0000-7000-8000-0000000000ab:orders:pg");
        assert_eq!(parse_gid(&gid), Some((txn, "orders:pg".to_owned())));
    }

    #[test]
    fn foreign_gids_are_not_ours() {
        for gid in ["foreign-xid", "dat:", "dat:x:pg", "dat::pg"] {
            assert_eq!(parse_gid(gid), None, "{gid}");
        }
    }
}

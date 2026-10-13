//! The datorium protocol core.
//!
//! One implementation of the parts that must behave identically in every
//! language binding: identity (UUIDv7), the hybrid logical clock, branch gids,
//! the journal state machine, connection strings, the commit protocol and
//! recovery. The Python package in this
//! repository is the executable specification; both implementations pass the
//! vectors in `conformance/` bit for bit.

pub mod clock;
pub mod coordinator;
pub mod dsn;
pub mod gid;
pub mod identity;
pub mod journal;
pub mod recovery;
pub mod retry;
pub mod rows;
pub mod staged;
pub mod store;
pub mod timer;

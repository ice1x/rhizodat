//! The datorium protocol core.
//!
//! One implementation of the parts that must behave identically in every
//! language binding: identity (UUIDv7), the hybrid logical clock, branch gids,
//! the journal state machine and connection strings. The Python package in this
//! repository is the executable specification; both implementations pass the
//! vectors in `conformance/` bit for bit.

pub mod clock;
pub mod dsn;
pub mod gid;
pub mod identity;
pub mod journal;

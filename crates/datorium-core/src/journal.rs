//! The journal state machine of a meta-transaction.
//!
//! ```text
//! ACTIVE ──► PREPARED ──► COMMITTED ──► DONE
//!    │           │
//!    └───────────┴──► ABORTED
//! ```
//!
//! `COMMITTED` is the atomic commit point; `ABORTED` and `DONE` are final.
//! Every change is a compare-and-set from an expected state, so a coordinator
//! deciding `COMMITTED` and a recovery worker deciding `ABORTED` never both win.

use std::fmt;
use std::str::FromStr;

/// A journal state, stored as its upper-case name.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum State {
    /// Branches are being staged; nothing is decided.
    Active,
    /// Every two-phase branch is prepared; `dat_tx_time` is assigned.
    Prepared,
    /// The decision to commit: the atomic commit point.
    Committed,
    /// Rolled back (final).
    Aborted,
    /// Every branch is finished (final).
    Done,
}

impl State {
    /// Every state, in the order of the conformance vectors.
    pub const ALL: [State; 5] = [
        State::Active,
        State::Prepared,
        State::Committed,
        State::Aborted,
        State::Done,
    ];

    /// The stored name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            State::Active => "ACTIVE",
            State::Prepared => "PREPARED",
            State::Committed => "COMMITTED",
            State::Aborted => "ABORTED",
            State::Done => "DONE",
        }
    }

    /// True for `ABORTED` and `DONE`, which never change again.
    #[must_use]
    pub const fn is_final(self) -> bool {
        matches!(self, State::Aborted | State::Done)
    }

    /// The states reachable from `self` in one compare-and-set.
    #[must_use]
    pub const fn successors(self) -> &'static [State] {
        match self {
            State::Active => &[State::Prepared, State::Aborted],
            State::Prepared => &[State::Committed, State::Aborted],
            State::Committed => &[State::Done],
            State::Aborted | State::Done => &[],
        }
    }
}

impl fmt::Display for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A stored state name that is not one of [`State::ALL`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnknownState(pub String);

impl fmt::Display for UnknownState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unknown journal state {:?}", self.0)
    }
}

impl std::error::Error for UnknownState {}

impl FromStr for State {
    type Err = UnknownState;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        State::ALL
            .into_iter()
            .find(|state| state.as_str() == s)
            .ok_or_else(|| UnknownState(s.to_owned()))
    }
}

/// An edge that is never legal, whatever is stored.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransitionError {
    /// `to` is not a successor of `from`.
    Illegal {
        /// The expected (current) state.
        from: State,
        /// The requested state.
        to: State,
    },
    /// `PREPARED` without the `dat_tx_time` assigned at stage.
    MissingTxTime,
}

impl fmt::Display for TransitionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransitionError::Illegal { from, to } => {
                write!(f, "illegal journal transition {from} -> {to}")
            }
            TransitionError::MissingTxTime => {
                f.write_str("PREPARED needs the dat_tx_time assigned at stage")
            }
        }
    }
}

impl std::error::Error for TransitionError {}

/// Check the edge `expected -> new` before any journal writes it.
///
/// # Errors
///
/// [`TransitionError::Illegal`] for an edge outside the state machine,
/// [`TransitionError::MissingTxTime`] for `PREPARED` without `dat_tx_time`.
pub fn check_transition(
    expected: State,
    new: State,
    dat_tx_time: Option<i64>,
) -> Result<(), TransitionError> {
    if !expected.successors().contains(&new) {
        return Err(TransitionError::Illegal {
            from: expected,
            to: new,
        });
    }
    if new == State::Prepared && dat_tx_time.is_none() {
        return Err(TransitionError::MissingTxTime);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn final_states_have_no_successors() {
        for s in State::ALL {
            assert_eq!(s.is_final(), s.successors().is_empty(), "{s}");
        }
    }

    #[test]
    fn errors_name_the_edge() {
        let err = check_transition(State::Done, State::Active, Some(1)).unwrap_err();
        assert_eq!(err.to_string(), "illegal journal transition DONE -> ACTIVE");
    }
}

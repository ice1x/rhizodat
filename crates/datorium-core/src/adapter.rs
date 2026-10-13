//! Store adapters as plug-ins: what a store kind declares to join datorium.
//!
//! The core knows no store. Each adapter crate exports an [`Adapter`]: its
//! kind, the DSN schemes it owns with their rules, whether it can hold the
//! journal, and its native query language. An application builds a
//! [`Registry`] from the adapters it links (the file journal's scheme is always
//! there) and parses connection strings with it; an adapter parses its own DSNs
//! with [`Adapter::parse`]. The rules match the Python reference
//! (`datorium.core.adapter`), so both accept and reject the same strings.

use std::fmt;

use crate::dsn::{Dsn, DsnError, parse_with};

/// Whether a DSN must, may or must not carry a host or a path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Presence {
    /// It must be there.
    Required,
    /// It may be there.
    Optional,
    /// It must not be there.
    None,
}

/// One DSN scheme and its rules.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Scheme {
    /// The scheme, lower-case (`postgresql`, `drevo+bolt`, …).
    pub name: &'static str,
    /// How the adapter reaches the store (`bolt`, `rest`, …), if it has several ways.
    pub transport: Option<&'static str>,
    /// The port used when the DSN names a host without one.
    pub default_port: Option<u16>,
    /// Whether a host is required, optional or forbidden.
    pub host: Presence,
    /// Whether a path is required, optional or forbidden.
    pub path: Presence,
}

/// A store kind as datorium sees it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Adapter {
    /// The store kind (`postgres`, `drevo`, …): lower-case letters, digits, `_`.
    pub kind: &'static str,
    /// The DSN schemes the adapter owns.
    pub schemes: &'static [Scheme],
    /// Whether the store can hold the journal (a shared ACID store).
    pub holds_journal: bool,
    /// One line on the store's native query language, for clients such as MCP.
    pub query_help: Option<&'static str>,
}

impl Adapter {
    /// Parse a DSN of one of this adapter's own schemes.
    ///
    /// # Errors
    ///
    /// [`DsnError`] for another scheme or a DSN that breaks the scheme's rules.
    pub fn parse(&self, raw: &str) -> Result<Dsn, DsnError> {
        let find = |name: &str| {
            self.schemes
                .iter()
                .find(|s| s.name == name)
                .map(|s| (self.kind, *s))
        };
        let names: Vec<&str> = self.schemes.iter().map(|s| s.name).collect();
        parse_with(raw, &find, &names)
    }
}

/// The file journal of the Python reference (`file:///abs/path`, SQLite, for
/// development); every [`Registry`] knows its scheme.
pub const FILE: Adapter = Adapter {
    kind: "file",
    schemes: &[Scheme {
        name: "file",
        transport: None,
        default_port: None,
        host: Presence::None,
        path: Presence::Required,
    }],
    holds_journal: true,
    query_help: None,
};

/// An adapter the registry refuses: ambiguous or invalid.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegistryError(String);

impl fmt::Display for RegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for RegistryError {}

/// The adapters an application links, by kind and by scheme.
#[derive(Clone, Debug)]
pub struct Registry {
    adapters: Vec<Adapter>,
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}

fn valid_kind(kind: &str) -> bool {
    let mut chars = kind.chars();
    (1..=32).contains(&kind.len())
        && chars.next().is_some_and(|c| c.is_ascii_lowercase())
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

fn valid_scheme(name: &str) -> bool {
    let mut chars = name.chars();
    (1..=32).contains(&name.len())
        && chars.next().is_some_and(|c| c.is_ascii_lowercase())
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || "+.-".contains(c))
}

impl Registry {
    /// A registry with the file journal's scheme only.
    #[must_use]
    pub fn new() -> Self {
        Self {
            adapters: vec![FILE],
        }
    }

    /// `self` with `adapter` registered.
    ///
    /// # Errors
    ///
    /// [`RegistryError`] as for [`Registry::register`].
    pub fn with(mut self, adapter: Adapter) -> Result<Self, RegistryError> {
        self.register(adapter)?;
        Ok(self)
    }

    /// Register `adapter`; nothing changes if it is refused.
    ///
    /// # Errors
    ///
    /// [`RegistryError`] for an invalid kind or scheme name, no scheme, a
    /// scheme declared twice, or a kind or scheme another adapter owns.
    pub fn register(&mut self, adapter: Adapter) -> Result<(), RegistryError> {
        let refuse = |m: String| Err(RegistryError(m));
        if !valid_kind(adapter.kind) {
            return refuse(format!(
                "invalid adapter kind {:?}: use a-z, 0-9 and _",
                adapter.kind
            ));
        }
        if self.adapter(adapter.kind).is_some() {
            return refuse(format!(
                "an adapter for {:?} is already registered",
                adapter.kind
            ));
        }
        if adapter.schemes.is_empty() {
            return refuse(format!("adapter {:?} declares no DSN scheme", adapter.kind));
        }
        for (i, s) in adapter.schemes.iter().enumerate() {
            if !valid_scheme(s.name) {
                return refuse(format!(
                    "invalid DSN scheme {:?} for adapter {:?}",
                    s.name, adapter.kind
                ));
            }
            if adapter.schemes[..i].iter().any(|o| o.name == s.name) {
                return refuse(format!(
                    "adapter {:?} declares {}:// twice",
                    adapter.kind, s.name
                ));
            }
            if let Some((owner, _)) = self.scheme(s.name) {
                return refuse(format!(
                    "{}:// already belongs to the {:?} adapter",
                    s.name, owner.kind
                ));
            }
        }
        self.adapters.push(adapter);
        Ok(())
    }

    /// Every registered adapter, in registration order.
    #[must_use]
    pub fn adapters(&self) -> &[Adapter] {
        &self.adapters
    }

    /// The adapter of a store kind.
    #[must_use]
    pub fn adapter(&self, kind: &str) -> Option<&Adapter> {
        self.adapters.iter().find(|a| a.kind == kind)
    }

    /// The adapter and rules of a (lower-case) DSN scheme.
    #[must_use]
    pub fn scheme(&self, name: &str) -> Option<(&Adapter, &Scheme)> {
        self.adapters
            .iter()
            .find_map(|a| a.schemes.iter().find(|s| s.name == name).map(|s| (a, s)))
    }

    /// Parse a DSN of any registered scheme.
    ///
    /// # Errors
    ///
    /// [`DsnError`] for an unregistered scheme or a DSN that breaks its rules.
    pub fn parse(&self, raw: &str) -> Result<Dsn, DsnError> {
        let find = |name: &str| self.scheme(name).map(|(a, s)| (a.kind, *s));
        let names: Vec<&str> = self
            .adapters
            .iter()
            .flat_map(|a| a.schemes.iter().map(|s| s.name))
            .collect();
        parse_with(raw, &find, &names)
    }
}

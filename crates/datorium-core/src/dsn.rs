//! Connection strings (DSNs) for stores and the journal.
//!
//! One URL syntax for every store, parsed without any driver:
//!
//! | DSN | store | transport |
//! |---|---|---|
//! | `postgresql://user:pass@host:5432/db?sslmode=…` | postgres | — |
//! | `clickhouse://user:pass@host:8123/db` | clickhouse | — |
//! | `drevo:///abs/path.drevo` | drevo | embedded |
//! | `drevo+mem://` (tests only) | drevo | memory |
//! | `drevo+bolt://user:pass@host:7687` | drevo | bolt |
//! | `iceberg+rest://host:8181/warehouse` | iceberg | rest |
//! | `iceberg+sql://…`, `iceberg+glue://…` | iceberg | sql, glue |
//! | `file:///abs/path` | file | — |
//!
//! `postgres://` is an alias. Schemes match case-insensitively; user, password
//! and path are percent-decoded. Passwords and secret-looking query parameters
//! never appear in `Debug` / `Display`; [`Dsn::unredacted`] returns the original
//! string for a driver. Splitting follows Python's `urllib.parse.urlsplit`, the
//! reference, so both implementations agree on every string.

use std::fmt;

/// The connection string is malformed or names an unsupported store. The
/// message never contains a secret.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DsnError(String);

impl fmt::Display for DsnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for DsnError {}

struct Scheme {
    name: &'static str,
    store: &'static str,
    transport: Option<&'static str>,
    default_port: Option<u16>,
    host_required: bool,
}

const fn scheme(
    name: &'static str,
    store: &'static str,
    transport: Option<&'static str>,
    default_port: Option<u16>,
    host_required: bool,
) -> Scheme {
    Scheme {
        name,
        store,
        transport,
        default_port,
        host_required,
    }
}

const SCHEMES: [Scheme; 10] = [
    scheme("postgresql", "postgres", None, Some(5432), false),
    scheme("postgres", "postgres", None, Some(5432), false),
    scheme("clickhouse", "clickhouse", None, Some(8123), true),
    scheme("drevo", "drevo", Some("embedded"), None, false),
    scheme("drevo+mem", "drevo", Some("memory"), None, false),
    scheme("drevo+bolt", "drevo", Some("bolt"), Some(7687), true),
    scheme("iceberg+rest", "iceberg", Some("rest"), None, true),
    scheme("iceberg+sql", "iceberg", Some("sql"), None, false),
    scheme("iceberg+glue", "iceberg", Some("glue"), None, false),
    scheme("file", "file", None, None, false),
];
const SECRET_MARKERS: [&str; 5] = ["pass", "secret", "token", "key", "credential"];
const MASK: &str = "***";

/// A parsed connection string.
#[derive(Clone, PartialEq, Eq)]
pub struct Dsn {
    store: &'static str,
    transport: Option<&'static str>,
    host: Option<String>,
    port: Option<u16>,
    user: Option<String>,
    password: Option<String>,
    path: Option<String>,
    params: Vec<(String, String)>,
    raw: String,
}

impl Dsn {
    /// `postgres`, `clickhouse`, `drevo`, `iceberg` or `file`.
    #[must_use]
    pub fn store(&self) -> &str {
        self.store
    }

    /// How a store is reached when it has several ways (`bolt`, `rest`, …).
    #[must_use]
    pub fn transport(&self) -> Option<&str> {
        self.transport
    }

    /// The host name, lower-cased.
    #[must_use]
    pub fn host(&self) -> Option<&str> {
        self.host.as_deref()
    }

    /// The port, or the store's default when a host is given.
    #[must_use]
    pub fn port(&self) -> Option<u16> {
        self.port
    }

    /// The percent-decoded user name.
    #[must_use]
    pub fn user(&self) -> Option<&str> {
        self.user.as_deref()
    }

    /// The percent-decoded password. Pass it to drivers only.
    #[must_use]
    pub fn password(&self) -> Option<&str> {
        self.password.as_deref()
    }

    /// The percent-decoded path, with its leading slash.
    #[must_use]
    pub fn path(&self) -> Option<&str> {
        self.path.as_deref()
    }

    /// The path without its leading slash: database, warehouse or catalog name.
    #[must_use]
    pub fn database(&self) -> Option<&str> {
        self.path
            .as_deref()
            .map(|p| p.trim_start_matches('/'))
            .filter(|d| !d.is_empty())
    }

    /// Query parameters in the order given, values decoded.
    #[must_use]
    pub fn params(&self) -> &[(String, String)] {
        &self.params
    }

    /// The original string, secrets included. Pass it to drivers only.
    #[must_use]
    pub fn unredacted(&self) -> &str {
        &self.raw
    }

    /// The string with the password and secret-looking parameters masked.
    #[must_use]
    pub fn redacted(&self) -> String {
        let cleaned = clean(&self.raw);
        let parts = split(&cleaned);
        let mut netloc = parts.netloc.to_owned();
        if self.password.is_some() {
            let (userinfo, hostinfo) = parts.netloc.rsplit_once('@').unwrap_or(("", parts.netloc));
            let name = userinfo.split_once(':').map_or(userinfo, |(n, _)| n);
            netloc = format!("{name}:{MASK}@{hostinfo}");
        }
        let query: Vec<String> = parse_query(parts.query)
            .into_iter()
            .map(|(k, v)| {
                let shown = if is_secret(&k) { MASK } else { v.as_str() };
                format!("{k}={shown}")
            })
            .collect();
        let out = format!(
            "{}://{netloc}{}",
            parts.scheme.to_ascii_lowercase(),
            parts.path
        );
        if query.is_empty() {
            out
        } else {
            format!("{out}?{}", query.join("&"))
        }
    }
}

impl fmt::Display for Dsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.redacted())
    }
}

impl fmt::Debug for Dsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Dsn({:?})", self.redacted())
    }
}

fn is_secret(key: &str) -> bool {
    let lowered = key.to_lowercase();
    SECRET_MARKERS.iter().any(|m| lowered.contains(m))
}

struct Parts<'a> {
    scheme: &'a str,
    netloc: &'a str,
    path: &'a str,
    query: &'a str,
}

fn split_scheme(url: &str) -> (&str, &str) {
    if let Some((scheme, rest)) = url.split_once(':') {
        let mut chars = scheme.chars();
        let valid = chars.next().is_some_and(|c| c.is_ascii_alphabetic())
            && chars.all(|c| c.is_ascii_alphanumeric() || "+-.".contains(c));
        if valid {
            return (scheme, rest);
        }
    }
    ("", url)
}

/// `urlsplit`'s cleanup: leading C0 controls and spaces stripped, tabs and newlines dropped.
fn clean(url: &str) -> String {
    url.trim_start_matches(|c: char| c <= ' ')
        .chars()
        .filter(|c| !matches!(c, '\t' | '\r' | '\n'))
        .collect()
}

/// `urlsplit` without its whitespace cleanup (callers pass the cleaned string).
fn split(url: &str) -> Parts<'_> {
    let (scheme, mut rest) = split_scheme(url);
    let mut netloc = "";
    if let Some(after) = rest.strip_prefix("//") {
        let end = after.find(['/', '?', '#']).unwrap_or(after.len());
        netloc = &after[..end];
        rest = &after[end..];
    }
    let rest = rest.split_once('#').map_or(rest, |(r, _)| r);
    let (path, query) = rest.split_once('?').unwrap_or((rest, ""));
    Parts {
        scheme,
        netloc,
        path,
        query,
    }
}

/// Python's `unquote`: `%XX` decoded as UTF-8, invalid sequences replaced.
fn unquote(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let (Some(h), Some(l)) = (hex(bytes[i + 1]), hex(bytes[i + 2]))
        {
            out.push(h << 4 | l);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex(b: u8) -> Option<u8> {
    char::from(b)
        .to_digit(16)
        .and_then(|d| u8::try_from(d).ok())
}

/// Python's `parse_qsl(query, keep_blank_values=True)`.
fn parse_query(query: &str) -> Vec<(String, String)> {
    query
        .split('&')
        .filter(|field| !field.is_empty())
        .map(|field| {
            let (k, v) = field.split_once('=').unwrap_or((field, ""));
            (unquote(&k.replace('+', " ")), unquote(&v.replace('+', " ")))
        })
        .collect()
}

/// The scheme part only, so an error never echoes credentials.
fn scheme_hint(raw: &str) -> String {
    match raw.split_once("://") {
        Some((scheme, _)) => format!("{scheme}://"),
        None => raw.split_once(':').map_or(raw, |(s, _)| s).to_owned(),
    }
}

fn error(message: impl Into<String>) -> DsnError {
    DsnError(message.into())
}

/// Parse a connection string.
///
/// # Errors
///
/// [`DsnError`] for an unsupported scheme, a bad port, a missing or unexpected
/// host or path, or a repeated query parameter.
pub fn parse_dsn(raw: &str) -> Result<Dsn, DsnError> {
    let cleaned = clean(raw);
    let parts = split(&cleaned);
    let name = parts.scheme.to_ascii_lowercase();
    let known = SCHEMES.iter().find(|s| s.name == name);
    let Some(sc) = known.filter(|_| raw.to_lowercase().starts_with(&format!("{name}://"))) else {
        let supported: Vec<String> = SCHEMES.iter().map(|s| format!("{}://", s.name)).collect();
        return Err(error(format!(
            "unsupported connection string {:?}; supported: {}",
            scheme_hint(raw),
            supported.join(", ")
        )));
    };

    let hostinfo = parts
        .netloc
        .rsplit_once('@')
        .map_or(parts.netloc, |(_, h)| h);
    let (host, port) = if let Some((_, bracketed)) = hostinfo.split_once('[') {
        let (h, after) = bracketed
            .split_once(']')
            .ok_or_else(|| error(format!("invalid IPv6 host in {name}:// connection string")))?;
        (h, after.split_once(':').map(|(_, p)| p))
    } else {
        let (h, p) = hostinfo
            .split_once(':')
            .map_or((hostinfo, None), |(h, p)| (h, Some(p)));
        (h, p)
    };
    let port = match port.filter(|p| !p.is_empty()) {
        None => None,
        Some(p) => {
            let invalid = || error(format!("invalid port in {name}:// connection string"));
            if !p.bytes().all(|b| b.is_ascii_digit()) {
                return Err(invalid());
            }
            let n: u32 = p.parse().map_err(|_| invalid())?;
            let n = u16::try_from(n).map_err(|_| invalid())?;
            if n == 0 {
                return Err(error(format!(
                    "invalid port 0 in {name}:// connection string"
                )));
            }
            Some(n)
        }
    };
    let host = Some(host.to_lowercase()).filter(|h| !h.is_empty());
    let path = Some(unquote(parts.path)).filter(|p| !p.is_empty());

    if matches!(sc.name, "drevo" | "file") {
        if !parts.netloc.is_empty() {
            return Err(error(format!(
                "{0}:/// takes a local absolute path, not a host: {0}:///abs/path",
                sc.name
            )));
        }
        if path.is_none() {
            return Err(error(format!(
                "{0}:/// needs a path: {0}:///abs/path",
                sc.name
            )));
        }
    }
    if sc.name == "drevo+mem" && (!parts.netloc.is_empty() || path.is_some()) {
        return Err(error("drevo+mem:// takes no host or path"));
    }
    if sc.host_required && host.is_none() {
        return Err(error(format!("{}:// needs a host", sc.name)));
    }

    let mut params: Vec<(String, String)> = Vec::new();
    for (key, value) in parse_query(parts.query) {
        if params.iter().any(|(k, _)| *k == key) {
            return Err(error(format!(
                "query parameter {key:?} is given more than once"
            )));
        }
        params.push((key, value));
    }

    let (user, password) = match parts.netloc.rsplit_once('@') {
        Some((userinfo, _)) => match userinfo.split_once(':') {
            Some((u, p)) => (u, Some(unquote(p))),
            None => (userinfo, None),
        },
        None => ("", None),
    };

    Ok(Dsn {
        store: sc.store,
        transport: sc.transport,
        port: port.or(if host.is_some() {
            sc.default_port
        } else {
            None
        }),
        host,
        user: Some(unquote(user)).filter(|u| !u.is_empty()),
        password,
        path,
        params,
        raw: raw.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unquote_matches_python() {
        assert_eq!(unquote("a%20b"), "a b");
        assert_eq!(unquote("%zz%4"), "%zz%4");
        assert_eq!(unquote("%C3%A9"), "é");
        assert_eq!(unquote("%ff"), "\u{fffd}");
    }

    #[test]
    fn errors_never_echo_credentials() {
        let err = parse_dsn("mysql://root:hunter2@db/x").unwrap_err();
        assert!(!err.to_string().contains("hunter2"));
        assert!(err.to_string().contains("mysql://"));
    }

    #[test]
    fn secret_parameters_are_masked() {
        let d = parse_dsn("iceberg+rest://c/w?s3.secret-access-key=xyz&region=eu").unwrap();
        assert_eq!(
            d.redacted(),
            "iceberg+rest://c/w?s3.secret-access-key=***&region=eu"
        );
        assert!(!format!("{d:?}").contains("xyz"));
    }

    #[test]
    fn surrounding_whitespace_is_ignored_like_urlsplit() {
        let d = parse_dsn("postgresql://h/d\tb\n").unwrap();
        assert_eq!(d.database(), Some("db"));
        let d = parse_dsn("postgresql://h/db ").unwrap();
        assert_eq!(d.database(), Some("db "));
    }
}

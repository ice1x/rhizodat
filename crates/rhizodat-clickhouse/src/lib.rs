//! ClickHouse for `rhizodat-core` (the `clickhouse` crate), the port of the
//! Python reference's `rhizodat.adapters.clickhouse`: a one-phase branch
//! applied after the decision.
//!
//! ClickHouse has no transactions. A branch therefore only buffers rows:
//!
//! - [`ClickHouseBranch::insert`] collects rows per table; nothing reaches the
//!   server;
//! - `stage` (before prepare, lock-free) checks every batch against the table
//!   schema and the size limit, adds `dat_txn` / `dat_tx_time` to each row and
//!   returns the batches as the journal payload (the shared format of
//!   `rhizodat_core::rows`), so whoever finishes the branch — the coordinator,
//!   or recovery on another host, in either implementation — has the rows;
//! - after the decision the participant inserts each batch, then writes the
//!   applied marker `(dat_txn, dat_tx_time)` into `dat_applied`.
//!
//! Finishing is idempotent without server-side deduplication: before inserting
//! a batch the participant counts the table's rows with this `dat_txn`. None →
//! insert; all → skip; some (a crash left part of an insert) → delete them and
//! insert again. Each insert also carries `insert_deduplication_token` for
//! replicated tables.
//!
//! Rows go to the server as `JSONEachRow` (columns are known only at run
//! time), with `date_time_input_format = best_effort` so ISO 8601 datetimes
//! with offsets are read as such. JSON strings are UTF-8, so a bytes value
//! must be valid UTF-8 (a `String` column); anything else is refused at stage.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use clickhouse::Client;
use rhizodat_core::adapter::{Adapter, Presence, Scheme};
use rhizodat_core::gid::gid_for;
use rhizodat_core::journal::BranchRecord;
use rhizodat_core::rows::{Batch, RowValue, decode_batches, encode_batches};
use rhizodat_core::store::{BoxError, Branch, Participant};
use serde_json::{Value as Json, json};
use uuid::Uuid;

pub mod parts;

/// The ClickHouse store kind: `clickhouse://host:8123/db` (one phase; cannot
/// hold the journal).
pub const ADAPTER: Adapter = Adapter {
    kind: "clickhouse",
    schemes: &[Scheme {
        name: "clickhouse",
        transport: None,
        default_port: Some(8123),
        host: Presence::Required,
        path: Presence::Optional,
    }],
    holds_journal: false,
    query_help: Some("SQL with {name:Type} placeholders"),
};

const RESERVED: [&str; 2] = ["dat_txn", "dat_tx_time"];
/// `TIMEOUT_EXCEEDED`, `TOO_MANY_SIMULTANEOUS_QUERIES`, `SOCKET_TIMEOUT`,
/// `NETWORK_ERROR`, `TABLE_IS_READ_ONLY` (replica lost Keeper), `TOO_MANY_PARTS`,
/// `UNKNOWN_STATUS_OF_INSERT`, `KEEPER_EXCEPTION`.
const RETRYABLE_CODES: [u32; 8] = [159, 202, 209, 210, 242, 252, 319, 999];

/// A setting or a row the adapter refuses.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClickHouseError(String);

impl fmt::Display for ClickHouseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for ClickHouseError {}

fn refuse(message: impl Into<String>) -> BoxError {
    Box::new(ClickHouseError(message.into()))
}

/// The server's error code, parsed from `Code: N.` (the crate exposes no field).
#[must_use]
pub fn error_code(error: &clickhouse::error::Error) -> Option<u32> {
    let clickhouse::error::Error::BadResponse(message) = error else {
        return None;
    };
    let rest = message.strip_prefix("Code: ")?;
    rest.split(|c: char| !c.is_ascii_digit())
        .next()?
        .parse()
        .ok()
}

/// Timeouts, overload, lost connections and Keeper trouble; never schema or
/// syntax errors.
#[must_use]
pub fn is_retryable(error: &(dyn Error + 'static)) -> bool {
    if let Some(ch) = error.downcast_ref::<clickhouse::error::Error>() {
        return match ch {
            clickhouse::error::Error::Network(_) | clickhouse::error::Error::TimedOut => true,
            other => error_code(other).is_some_and(|c| RETRYABLE_CODES.contains(&c)),
        };
    }
    error.is::<std::io::Error>()
}

fn check_table(table: &str) -> Result<(), ClickHouseError> {
    let part = |p: &str| {
        let mut chars = p.chars();
        chars
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
            && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
    };
    let ok = match table.split_once('.') {
        Some((db, name)) => part(db) && part(name),
        None => part(table),
    };
    if ok {
        Ok(())
    } else {
        Err(ClickHouseError(format!(
            "invalid table name {table:?}: [database.]table, letters, digits, _"
        )))
    }
}

fn quote(table: &str) -> String {
    table
        .split('.')
        .map(|p| format!("`{p}`"))
        .collect::<Vec<_>>()
        .join(".")
}

/// One value as `JSONEachRow` input.
fn json(value: &RowValue) -> Result<Json, ClickHouseError> {
    Ok(match value {
        RowValue::Null => Json::Null,
        RowValue::Bool(v) => json!(v),
        RowValue::Int(v) => json!(v),
        RowValue::Float(v) if v.is_finite() => json!(v),
        RowValue::Float(v) if v.is_nan() => json!("nan"),
        RowValue::Float(v) => json!(if *v > 0.0 { "inf" } else { "-inf" }),
        RowValue::Str(v) | RowValue::Decimal(v) | RowValue::Date(v) | RowValue::DateTime(v) => {
            json!(v)
        }
        RowValue::Uuid(v) => json!(v.hyphenated().to_string()),
        RowValue::Bytes(v) => match std::str::from_utf8(v) {
            Ok(s) => json!(s),
            Err(_) => {
                return Err(ClickHouseError(
                    "bytes that are not UTF-8 cannot be sent as JSONEachRow".into(),
                ));
            }
        },
        RowValue::List(v) => Json::Array(v.iter().map(json).collect::<Result<_, _>>()?),
        RowValue::Map(v) => Json::Object(
            v.iter()
                .map(|(k, x)| Ok((k.clone(), json(x)?)))
                .collect::<Result<_, ClickHouseError>>()?,
        ),
    })
}

fn json_each_row(batch: &Batch) -> Result<String, ClickHouseError> {
    let mut body = String::new();
    for row in &batch.rows {
        let object: serde_json::Map<String, Json> = batch
            .columns
            .iter()
            .zip(row)
            .map(|(c, v)| Ok((c.clone(), json(v)?)))
            .collect::<Result<_, ClickHouseError>>()
            .map_err(|e| ClickHouseError(format!("table {:?}: {e}", batch.table)))?;
        body.push_str(&Json::Object(object).to_string());
        body.push('\n');
    }
    Ok(body)
}

/// One ClickHouse database as a rhizodat store.
#[derive(Clone)]
pub struct ClickHouseStore {
    name: String,
    client: Client,
    max_rows_per_table: usize,
    participant: Arc<ClickHouseParticipant>,
}

impl fmt::Debug for ClickHouseStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClickHouseStore")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl ClickHouseStore {
    /// The default limit of rows per table in one branch.
    pub const DEFAULT_MAX_ROWS_PER_TABLE: usize = 100_000;

    /// A store named `name` over a `clickhouse://` DSN (`?secure=true` for HTTPS).
    ///
    /// # Errors
    ///
    /// [`ClickHouseError`] for a DSN of another store.
    pub fn new(dsn: &str, name: &str) -> Result<Self, ClickHouseError> {
        let parsed = ADAPTER
            .parse(dsn)
            .map_err(|e| ClickHouseError(e.to_string()))?;
        if parsed.store() != "clickhouse" {
            return Err(ClickHouseError(format!(
                "not a clickhouse connection string: {parsed}"
            )));
        }
        let secure = parsed.params().iter().any(|(k, v)| {
            k == "secure" && matches!(v.to_lowercase().as_str(), "1" | "true" | "yes")
        });
        let scheme = if secure { "https" } else { "http" };
        let client = Client::default()
            .with_url(format!(
                "{scheme}://{}:{}",
                parsed.host().unwrap_or_default(),
                parsed.port().unwrap_or(8123)
            ))
            .with_user(parsed.user().unwrap_or("default"))
            .with_password(parsed.password().unwrap_or_default())
            .with_database(parsed.database().unwrap_or("default"));
        let participant = Arc::new(ClickHouseParticipant {
            client: client.clone(),
        });
        Ok(Self {
            name: name.to_owned(),
            client,
            max_rows_per_table: Self::DEFAULT_MAX_ROWS_PER_TABLE,
            participant,
        })
    }

    /// The limit of rows per table in one branch (one INSERT each).
    ///
    /// # Panics
    ///
    /// For a limit of 0.
    #[must_use]
    pub fn with_max_rows_per_table(mut self, limit: usize) -> Self {
        assert!(limit >= 1, "max_rows_per_table must be >= 1");
        self.max_rows_per_table = limit;
        self
    }

    /// The store name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The driver's client, for native access outside any meta-transaction.
    #[must_use]
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Create the service table `dat_applied` if it does not exist.
    ///
    /// # Errors
    ///
    /// The driver's error.
    pub async fn install(&self) -> Result<(), clickhouse::error::Error> {
        self.client
            .query(
                "CREATE TABLE IF NOT EXISTS dat_applied (dat_txn UUID, dat_tx_time Int64, \
                 applied_at DateTime64(3) DEFAULT now64(3)) ENGINE = ReplacingMergeTree ORDER BY dat_txn",
            )
            .execute()
            .await
    }

    /// A new branch: an empty buffer of rows.
    #[must_use]
    pub fn branch(&self) -> ClickHouseBranch {
        ClickHouseBranch {
            store: self.name.clone(),
            client: self.client.clone(),
            max_rows_per_table: self.max_rows_per_table,
            buffer: BTreeMap::new(),
            parts: parts::ClickHouseParts::new(),
            staged: None,
        }
    }

    /// The store's participant (one per store, shared).
    #[must_use]
    pub fn participant(&self) -> Arc<ClickHouseParticipant> {
        self.participant.clone()
    }
}

async fn columns(client: &Client, table: &str) -> Result<Vec<String>, clickhouse::error::Error> {
    let (database, name) = match table.split_once('.') {
        Some((db, name)) => (Some(db), name),
        None => (None, table),
    };
    let sql = "SELECT name FROM system.columns WHERE database = if(? = '', currentDatabase(), ?) AND table = ? \
               ORDER BY position";
    let db = database.unwrap_or_default();
    client
        .query(sql)
        .bind(db)
        .bind(db)
        .bind(name)
        .fetch_all::<String>()
        .await
}

async fn count(
    client: &Client,
    table: &str,
    dat_txn: Uuid,
) -> Result<u64, clickhouse::error::Error> {
    client
        .query(&format!(
            "SELECT count() FROM {} WHERE dat_txn = ?",
            quote(table)
        ))
        .bind(dat_txn)
        .fetch_one()
        .await
}

/// Erase, insert every batch and then the marker, skipping what is already
/// there (the port of the reference's `ClickHouseStore.apply`).
///
/// An erase batch names entities (`rows` are their `dat_id`s) whose versions
/// older than this meta-transaction are deleted, before any insert.
async fn apply(
    client: &Client,
    gid: &str,
    batches: &[Batch],
    dat_txn: Uuid,
    dat_tx_time: i64,
) -> Result<(), BoxError> {
    if count(client, "dat_applied", dat_txn).await? > 0 {
        return Ok(());
    }
    for batch in batches.iter().filter(|b| b.erase) {
        let ids: Vec<Uuid> = batch
            .rows
            .iter()
            .map(|row| match row.first() {
                Some(RowValue::Uuid(id)) => Ok(*id),
                other => Err(format!("an erase batch names {other:?}, not a dat_id")),
            })
            .collect::<Result<_, _>>()?;
        client
            .query(&format!(
                "DELETE FROM {} WHERE dat_id IN ? AND dat_tx_time < ?",
                quote(&batch.table)
            ))
            .bind(ids)
            .bind(dat_tx_time)
            .with_setting("lightweight_deletes_sync", "2")
            .execute()
            .await?;
    }
    for (i, batch) in batches.iter().enumerate() {
        if batch.erase {
            continue;
        }
        let present = count(client, &batch.table, dat_txn).await?;
        if present >= batch.rows.len() as u64 {
            continue;
        }
        if present > 0 {
            client
                .query(&format!(
                    "DELETE FROM {} WHERE dat_txn = ?",
                    quote(&batch.table)
                ))
                .bind(dat_txn)
                .with_setting("lightweight_deletes_sync", "2")
                .execute()
                .await?;
        }
        insert(
            client,
            &batch.table,
            &batch.columns,
            json_each_row(batch)?,
            format!("{gid}:{i}"),
        )
        .await?;
    }
    if count(client, "dat_applied", dat_txn).await? == 0 {
        let marker =
            json!({"dat_txn": dat_txn.hyphenated().to_string(), "dat_tx_time": dat_tx_time});
        let columns = ["dat_txn".to_owned(), "dat_tx_time".to_owned()];
        insert(
            client,
            "dat_applied",
            &columns,
            format!("{marker}\n"),
            format!("{gid}:applied"),
        )
        .await?;
    }
    Ok(())
}

async fn insert(
    client: &Client,
    table: &str,
    columns: &[String],
    body: String,
    token: String,
) -> Result<(), BoxError> {
    let names: Vec<String> = columns.iter().map(|c| format!("`{c}`")).collect();
    let sql = format!(
        "INSERT INTO {} ({}) FORMAT JSONEachRow",
        quote(table),
        names.join(", ")
    );
    let mut insert = client
        .insert_formatted_with(sql)
        .with_setting("insert_deduplication_token", token)
        .with_setting("async_insert", "0")
        .with_setting("date_time_input_format", "best_effort");
    insert.send(bytes::Bytes::from(body)).await?;
    insert.end().await?;
    Ok(())
}

/// One Unit of Work's rows for ClickHouse, inserted only if the meta-transaction commits.
pub struct ClickHouseBranch {
    store: String,
    client: Client,
    max_rows_per_table: usize,
    buffer: BTreeMap<String, (Vec<String>, Vec<Vec<RowValue>>)>,
    parts: parts::ClickHouseParts,
    staged: Option<(Uuid, Vec<Batch>, i64)>,
}

impl ClickHouseBranch {
    /// Buffer `rows` (one value per column of `columns`) for `table`.
    ///
    /// # Errors
    ///
    /// [`ClickHouseError`] for a bad table name, a reserved column
    /// (`dat_txn`, `dat_tx_time`), other columns than earlier rows of the table,
    /// or a row of the wrong length.
    pub fn insert(
        &mut self,
        table: &str,
        columns: &[&str],
        rows: Vec<Vec<RowValue>>,
    ) -> Result<(), ClickHouseError> {
        check_table(table)?;
        if rows.is_empty() {
            return Ok(());
        }
        if let Some(reserved) = RESERVED.iter().find(|r| columns.contains(r)) {
            return Err(ClickHouseError(format!(
                "{reserved} is set by rhizodat; leave it out of the rows"
            )));
        }
        if let Some(row) = rows.iter().find(|r| r.len() != columns.len()) {
            return Err(ClickHouseError(format!(
                "a row has {} values for {} columns",
                row.len(),
                columns.len()
            )));
        }
        let columns: Vec<String> = columns.iter().map(ToString::to_string).collect();
        let (known, buffered) = self
            .buffer
            .entry(table.to_owned())
            .or_insert_with(|| (columns.clone(), Vec::new()));
        if *known != columns {
            return Err(ClickHouseError(format!(
                "rows for {table:?} must have the same columns: {known:?}"
            )));
        }
        buffered.extend(rows);
        Ok(())
    }
}

#[async_trait]
impl Branch for ClickHouseBranch {
    fn store(&self) -> &str {
        &self.store
    }

    fn two_phase(&self) -> bool {
        false
    }

    async fn stage(
        &mut self,
        dat_txn: Uuid,
        dat_tx_time: i64,
    ) -> Result<Option<Vec<u8>>, BoxError> {
        // Entity parts first, like the reference: erases, then versions.
        let mut batches = self.part_batches(dat_txn, dat_tx_time)?;
        for (table, (cols, rows)) in &self.buffer {
            if rows.len() > self.max_rows_per_table {
                return Err(refuse(format!(
                    "{} rows for {table:?} exceed max_rows_per_table={}",
                    rows.len(),
                    self.max_rows_per_table
                )));
            }
            let existing = columns(&self.client, table).await?;
            let missing: Vec<&str> = cols
                .iter()
                .map(String::as_str)
                .chain(RESERVED)
                .filter(|c| !existing.iter().any(|e| e == c))
                .collect();
            if !missing.is_empty() {
                return Err(refuse(format!(
                    "table {table:?} has no column(s) {}",
                    missing.join(", ")
                )));
            }
            let mut all: Vec<String> = cols.clone();
            all.extend(RESERVED.map(str::to_owned));
            let rows = rows
                .iter()
                .map(|r| {
                    let mut r = r.clone();
                    r.extend([RowValue::Uuid(dat_txn), RowValue::Int(dat_tx_time)]);
                    r
                })
                .collect();
            let batch = Batch {
                table: table.clone(),
                columns: all,
                rows,
                erase: false,
            };
            json_each_row(&batch)?; // refuse what the server cannot take, before prepare
            batches.push(batch);
        }
        let payload = encode_batches(&batches)?;
        self.staged = Some((dat_txn, batches, dat_tx_time));
        Ok(Some(payload))
    }

    async fn prepare(&mut self, _gid: &str) -> Result<(), BoxError> {
        Err(refuse(
            "a ClickHouse branch is not two-phase; it is applied after the decision",
        ))
    }

    async fn commit_one_phase(&mut self) -> Result<(), BoxError> {
        let (dat_txn, batches, ts) = self
            .staged
            .as_ref()
            .ok_or_else(|| refuse("commit_one_phase before stage"))?;
        apply(
            &self.client,
            &gid_for(*dat_txn, &self.store),
            batches,
            *dat_txn,
            *ts,
        )
        .await
    }

    async fn rollback(&mut self) -> Result<(), BoxError> {
        self.buffer.clear();
        self.parts = parts::ClickHouseParts::new();
        self.staged = None;
        Ok(())
    }

    fn is_retryable(&self, error: &(dyn Error + 'static)) -> bool {
        is_retryable(error)
    }
}

/// Applies staged rows after the decision; nothing to undo before it.
pub struct ClickHouseParticipant {
    client: Client,
}

impl fmt::Debug for ClickHouseParticipant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClickHouseParticipant")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl Participant for ClickHouseParticipant {
    async fn finish(
        &self,
        branch: &BranchRecord,
        dat_txn: Uuid,
        dat_tx_time: i64,
    ) -> Result<(), BoxError> {
        let batches = match &branch.payload {
            Some(payload) => decode_batches(payload)?,
            None => Vec::new(),
        };
        apply(&self.client, &branch.gid, &batches, dat_txn, dat_tx_time).await
    }

    async fn is_applied(&self, _branch: &BranchRecord, dat_txn: Uuid) -> Result<bool, BoxError> {
        Ok(count(&self.client, "dat_applied", dat_txn).await? > 0)
    }

    async fn abort(&self, _branch: &BranchRecord) -> Result<(), BoxError> {
        Ok(()) // rows reach ClickHouse only after a commit decision
    }

    async fn prepared_gids(&self) -> Result<Vec<String>, BoxError> {
        Ok(Vec::new())
    }

    fn is_retryable(&self, error: &(dyn Error + 'static)) -> bool {
        is_retryable(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_names_are_checked_and_quoted() {
        for ok in ["events", "db.events", "_x1"] {
            assert!(check_table(ok).is_ok(), "{ok}");
        }
        for bad in ["", "1x", "a.b.c", "a-b", "x; DROP", "a."] {
            assert!(check_table(bad).is_err(), "{bad}");
        }
        assert_eq!(quote("db.events"), "`db`.`events`");
    }

    #[test]
    fn error_codes_come_from_the_message() {
        let e = clickhouse::error::Error::BadResponse(
            "Code: 252. DB::Exception: Too many parts".into(),
        );
        assert_eq!(error_code(&e), Some(252));
        assert!(is_retryable(&e));
        let e =
            clickhouse::error::Error::BadResponse("Code: 60. DB::Exception: Unknown table".into());
        assert!(!is_retryable(&e));
        assert!(is_retryable(&clickhouse::error::Error::TimedOut));
        assert_eq!(error_code(&clickhouse::error::Error::TimedOut), None);
    }

    #[test]
    fn rows_become_json_each_row() {
        let batch = Batch {
            table: "t".into(),
            columns: vec!["f".into(), "m".into(), "b".into()],
            rows: vec![vec![
                RowValue::Float(f64::NEG_INFINITY),
                RowValue::Map(vec![("k".into(), RowValue::List(vec![RowValue::Int(1)]))]),
                RowValue::Bytes(b"ok".to_vec()),
            ]],
            erase: false,
        };
        assert_eq!(
            json_each_row(&batch).unwrap(),
            "{\"b\":\"ok\",\"f\":\"-inf\",\"m\":{\"k\":[1]}}\n"
        );
        let binary = Batch {
            rows: vec![vec![
                RowValue::Null,
                RowValue::Null,
                RowValue::Bytes(vec![0xff]),
            ]],
            ..batch
        };
        assert!(
            json_each_row(&binary)
                .unwrap_err()
                .to_string()
                .contains("UTF-8")
        );
    }

    #[test]
    fn buffered_rows_are_validated() {
        let store = ClickHouseStore::new("clickhouse://h/db", "ch").unwrap();
        let mut branch = store.branch();
        assert!(
            branch
                .insert(
                    "t",
                    &["a", "dat_txn"],
                    vec![vec![RowValue::Null, RowValue::Null]]
                )
                .is_err()
        );
        assert!(branch.insert("t", &["a"], vec![vec![]]).is_err());
        assert!(
            branch
                .insert("bad name", &["a"], vec![vec![RowValue::Null]])
                .is_err()
        );
        branch
            .insert("t", &["a"], vec![vec![RowValue::Int(1)]])
            .unwrap();
        assert!(
            branch
                .insert("t", &["b"], vec![vec![RowValue::Int(1)]])
                .is_err()
        );
        branch.insert("t", &["a"], Vec::new()).unwrap();
        assert!(ClickHouseStore::new("postgresql://h/db", "ch").is_err());
    }
}

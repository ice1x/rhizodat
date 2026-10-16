//! Apache Iceberg for `rhizodat-core` (iceberg-rust), the port of the Python
//! reference's `rhizodat.adapters.iceberg`: a one-phase branch committed after
//! the decision.
//!
//! Iceberg commits optimistically: a commit swaps the table's metadata pointer
//! to a new snapshot. A branch therefore splits the work:
//!
//! - [`IcebergBranch::append`] buffers Arrow record batches per table;
//! - `stage` (before prepare, lock-free) conforms each table's batches to its
//!   Arrow schema, adds `dat_txn` / `dat_tx_time`, and writes Parquet data
//!   files under the table's location. The files are invisible (no snapshot
//!   references them); the journal payload lists them (the shared format of
//!   `rhizodat_core::staged`, with each file's `DataFile` JSON), so whoever
//!   finishes the branch — the coordinator, or recovery on another host, in
//!   either implementation — can commit them;
//! - after the decision the participant appends each table's files in one
//!   snapshot whose summary carries `dat.txn` and `dat.tx_time`.
//!
//! Finishing is idempotent: a table is skipped when a snapshot is tagged with
//! this `dat_txn` or its current snapshot already references the files (the
//! tag may have been expired). A payload without `DataFile` objects (written
//! by the Python reference) is committed from the Parquet footers, without
//! column statistics. Staged files of an aborted branch are deleted; files
//! left by a crash before the journal record are unreferenced and removed by
//! orphan-file maintenance. Files are never deleted while a snapshot may
//! reference them.
//!
//! Tables written through rhizodat need `dat_txn` (`uuid` or `string`) and
//! `dat_tx_time` (`long`) columns. Only appends to unpartitioned tables are
//! supported for now; catalogs: `iceberg+rest://` over S3, or any
//! [`Catalog`] passed to [`IcebergStore::with_catalog`].

use std::collections::{BTreeMap, HashMap, HashSet};
use std::error::Error;
use std::fmt;
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, FixedSizeBinaryArray, Int64Array, LargeStringArray, RecordBatch, StringArray,
    new_null_array,
};
use arrow_schema::{DataType, SchemaRef};
use async_trait::async_trait;
use futures::TryStreamExt;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, deserialize_data_file_from_json,
    serialize_data_file_to_json,
};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, ErrorKind, TableIdent};
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_storage_opendal::OpenDalStorageFactory;
use parquet::file::metadata::ParquetMetaDataReader;
use parquet::file::properties::WriterProperties;
use rhizodat_core::adapter::{Adapter, Presence, Scheme};
use rhizodat_core::gid::gid_for;
use rhizodat_core::journal::BranchRecord;
use rhizodat_core::staged::{StagedTable, decode_staged, encode_staged};
use rhizodat_core::store::{BoxError, Branch, Participant};
use serde_json::Value as Json;
use uuid::Uuid;

/// The Iceberg store kind: a REST catalog (`iceberg+rest://host/warehouse`);
/// `iceberg+sql://` and `iceberg+glue://` are recognised for the Python
/// reference. One phase; cannot hold the journal.
pub const ADAPTER: Adapter = Adapter {
    kind: "iceberg",
    schemes: &[
        Scheme {
            name: "iceberg+rest",
            transport: Some("rest"),
            default_port: None,
            host: Presence::Required,
            path: Presence::Optional,
        },
        Scheme {
            name: "iceberg+sql",
            transport: Some("sql"),
            default_port: None,
            host: Presence::Optional,
            path: Presence::Optional,
        },
        Scheme {
            name: "iceberg+glue",
            transport: Some("glue"),
            default_port: None,
            host: Presence::Optional,
            path: Presence::Optional,
        },
    ],
    holds_journal: false,
    query_help: Some("a table name with a scan"),
};

const RESERVED: [&str; 2] = ["dat_txn", "dat_tx_time"];
const TXN_PROPERTY: &str = "dat.txn";
const TIME_PROPERTY: &str = "dat.tx_time";
/// HTTP statuses of a catalog that may answer differently later.
const RETRYABLE_STATUSES: [&str; 5] = ["429", "500", "502", "503", "504"];

/// A connection string, a table or rows the adapter refuses.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IcebergError(String);

impl fmt::Display for IcebergError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for IcebergError {}

fn refuse(message: impl Into<String>) -> BoxError {
    Box::new(IcebergError(message.into()))
}

/// Catalog properties for an `iceberg+rest` DSN: `uri`, `warehouse` from the
/// path, and every query parameter but `secure` passed through (`s3.endpoint`,
/// `credential`, ...).
///
/// # Errors
///
/// [`IcebergError`] for a DSN of another store, or a catalog this crate does
/// not support yet (`iceberg+sql`, `iceberg+glue`).
pub fn catalog_properties(dsn: &str) -> Result<HashMap<String, String>, IcebergError> {
    let parsed = ADAPTER
        .parse(dsn)
        .map_err(|e| IcebergError(e.to_string()))?;
    if parsed.store() != "iceberg" {
        return Err(IcebergError(format!(
            "not an iceberg connection string: {parsed}"
        )));
    }
    if parsed.transport() != Some("rest") {
        return Err(IcebergError(format!(
            "the iceberg+{} catalog is not supported by the Rust adapter yet",
            parsed.transport().unwrap_or_default()
        )));
    }
    let mut props: HashMap<String, String> = parsed
        .params()
        .iter()
        .filter(|(k, _)| k != "secure")
        .cloned()
        .collect();
    let secure = parsed
        .params()
        .iter()
        .any(|(k, v)| k == "secure" && matches!(v.to_lowercase().as_str(), "1" | "true" | "yes"));
    let port = parsed.port().map(|p| format!(":{p}")).unwrap_or_default();
    props.insert(
        "uri".to_owned(),
        format!(
            "{}://{}{port}",
            if secure { "https" } else { "http" },
            parsed.host().unwrap_or_default()
        ),
    );
    if let Some(warehouse) = parsed
        .path()
        .map(|p| p.trim_matches('/'))
        .filter(|p| !p.is_empty())
    {
        props.insert("warehouse".to_owned(), warehouse.to_owned());
    }
    Ok(props)
}

fn retryable_iceberg(error: &iceberg::Error) -> bool {
    if error.retryable() {
        return true; // a concurrent commit moved the metadata pointer
    }
    if error.kind() != ErrorKind::Unexpected {
        return false;
    }
    let text = error.to_string();
    // Resolved by is_applied before anything is committed again.
    text.contains("the commit state is unknown")
        || RETRYABLE_STATUSES
            .iter()
            .any(|s| text.contains(&format!("status: {s} ")))
}

/// Commit conflicts, an unknown commit state, an overloaded or unreachable
/// catalog or object store; never schema or validation errors.
#[must_use]
pub fn is_retryable(error: &(dyn Error + 'static)) -> bool {
    let mut current = Some(error);
    while let Some(e) = current {
        if let Some(ice) = e.downcast_ref::<iceberg::Error>()
            && retryable_iceberg(ice)
        {
            return true;
        }
        if let Some(http) = e.downcast_ref::<reqwest::Error>()
            && (http.is_connect() || http.is_timeout() || http.is_request())
        {
            return true;
        }
        if e.downcast_ref::<opendal::Error>()
            .is_some_and(opendal::Error::is_temporary)
            || e.is::<std::io::Error>()
        {
            return true;
        }
        current = e.source();
    }
    false
}

fn ident(table: &str) -> Result<TableIdent, BoxError> {
    let parts: Vec<&str> = table.split('.').collect();
    if parts.len() < 2 || parts.iter().any(|p| p.is_empty()) {
        return Err(refuse(format!(
            "invalid table name {table:?}: namespace.table"
        )));
    }
    Ok(TableIdent::from_strs(parts)?)
}

/// The catalog as the branch and the participant need it.
struct Lake {
    catalog: Arc<dyn Catalog>,
}

impl Lake {
    async fn load(&self, table: &str) -> Result<Table, BoxError> {
        Ok(self.catalog.load_table(&ident(table)?).await?)
    }

    fn tagged(table: &Table, dat_txn: Uuid) -> bool {
        let txn = dat_txn.to_string();
        table
            .metadata()
            .snapshots()
            .any(|s| s.summary().additional_properties.get(TXN_PROPERTY) == Some(&txn))
    }

    /// Whether the current snapshot references every one of `files`.
    async fn referenced(table: &Table, files: &[String]) -> Result<bool, BoxError> {
        if table.metadata().current_snapshot().is_none() {
            return Ok(false);
        }
        let live: HashSet<String> = table
            .scan()
            .build()?
            .plan_files()
            .await?
            .map_ok(|task| task.data_file_path().to_owned())
            .try_collect()
            .await?;
        Ok(files.iter().all(|f| live.contains(f)))
    }

    async fn applied(&self, staged: &StagedTable, dat_txn: Uuid) -> Result<bool, BoxError> {
        let table = self.load(&staged.table).await?;
        Ok(Self::tagged(&table, dat_txn) || Self::referenced(&table, &staged.files).await?)
    }

    async fn delete_files(&self, staged: &StagedTable) -> Result<(), BoxError> {
        let table = self.load(&staged.table).await?;
        for path in &staged.files {
            table.file_io().delete(path).await?;
        }
        Ok(())
    }

    async fn finish(
        &self,
        tables: &[StagedTable],
        dat_txn: Uuid,
        dat_tx_time: i64,
    ) -> Result<(), BoxError> {
        if let Some(staged) = tables.iter().find(|t| !t.erase.is_empty()) {
            return Err(refuse(format!(
                "{}: erasing entity parts is not supported by this adapter yet",
                staged.table
            )));
        }
        for staged in tables {
            let table = self.load(&staged.table).await?;
            if Self::tagged(&table, dat_txn) || Self::referenced(&table, &staged.files).await? {
                continue;
            }
            let files = data_files(&table, staged).await?;
            let properties = HashMap::from([
                (TXN_PROPERTY.to_owned(), dat_txn.to_string()),
                (TIME_PROPERTY.to_owned(), dat_tx_time.to_string()),
            ]);
            let tx = Transaction::new(&table);
            let tx = tx
                .fast_append()
                .add_data_files(files)
                .set_snapshot_properties(properties)
                .apply(tx)?;
            tx.commit(self.catalog.as_ref()).await?;
        }
        Ok(())
    }
}

/// The `DataFile`s of `staged`: from the payload, else from the Parquet footers.
async fn data_files(table: &Table, staged: &StagedTable) -> Result<Vec<DataFile>, BoxError> {
    let md = table.metadata();
    if let Some(objects) = &staged.data_files {
        return objects
            .iter()
            .map(|o| {
                Ok(deserialize_data_file_from_json(
                    &Json::Object(o.clone()).to_string(),
                    md.default_partition_spec_id(),
                    md.default_partition_type(),
                    md.current_schema(),
                )?)
            })
            .collect();
    }
    if !md.default_partition_spec().is_unpartitioned() {
        return Err(refuse(format!(
            "table {:?} is partitioned: its files need DataFile objects in the payload",
            staged.table
        )));
    }
    let mut out = Vec::with_capacity(staged.files.len());
    for path in &staged.files {
        let bytes = table.file_io().new_input(path)?.read().await?;
        let footer = ParquetMetaDataReader::new().parse_and_finish(&bytes)?;
        let rows = u64::try_from(footer.file_metadata().num_rows())
            .map_err(|_| refuse(format!("{path}: a negative row count")))?;
        out.push(
            DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(path.clone())
                .file_format(DataFileFormat::Parquet)
                .record_count(rows)
                .file_size_in_bytes(bytes.len() as u64)
                .partition_spec_id(md.default_partition_spec_id())
                .build()?,
        );
    }
    Ok(out)
}

/// One Iceberg catalog as a rhizodat store.
#[derive(Clone)]
pub struct IcebergStore {
    name: String,
    lake: Arc<Lake>,
    participant: Arc<IcebergParticipant>,
}

impl fmt::Debug for IcebergStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IcebergStore")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl IcebergStore {
    /// A store named `name` over an `iceberg+rest://host:port/<warehouse>` DSN
    /// (`?secure=true` for HTTPS), with S3 file IO.
    ///
    /// # Errors
    ///
    /// [`IcebergError`] for an unsupported DSN, or the catalog's error.
    pub async fn connect(dsn: &str, name: &str) -> Result<Self, BoxError> {
        let props = catalog_properties(dsn)?;
        let catalog = RestCatalogBuilder::default()
            .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
                customized_credential_load: None,
            }))
            .load(name, props)
            .await?;
        Ok(Self::with_catalog(name, Arc::new(catalog)))
    }

    /// A store named `name` over any iceberg-rust catalog.
    #[must_use]
    pub fn with_catalog(name: &str, catalog: Arc<dyn Catalog>) -> Self {
        let lake = Arc::new(Lake { catalog });
        let participant = Arc::new(IcebergParticipant { lake: lake.clone() });
        Self {
            name: name.to_owned(),
            lake,
            participant,
        }
    }

    /// The store name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The catalog, for native access outside any meta-transaction.
    #[must_use]
    pub fn catalog(&self) -> &Arc<dyn Catalog> {
        &self.lake.catalog
    }

    /// Every row of `table` (`namespace.table`) at its current snapshot.
    ///
    /// # Errors
    ///
    /// The catalog's or the scan's error.
    pub async fn scan(&self, table: &str) -> Result<Vec<RecordBatch>, BoxError> {
        let table = self.lake.load(table).await?;
        if table.metadata().current_snapshot().is_none() {
            return Ok(Vec::new());
        }
        Ok(table
            .scan()
            .select_all()
            .build()?
            .to_arrow()
            .await?
            .try_collect()
            .await?)
    }

    /// A new branch: an empty buffer of record batches.
    #[must_use]
    pub fn branch(&self) -> IcebergBranch {
        IcebergBranch {
            store: self.name.clone(),
            lake: self.lake.clone(),
            buffer: BTreeMap::new(),
            staged: None,
            written: Vec::new(),
            commit_attempted: false,
        }
    }

    /// The store's participant (one per store, shared).
    #[must_use]
    pub fn participant(&self) -> Arc<IcebergParticipant> {
        self.participant.clone()
    }
}

/// One Unit of Work's appends to Iceberg, committed only if the meta-transaction commits.
pub struct IcebergBranch {
    store: String,
    lake: Arc<Lake>,
    buffer: BTreeMap<String, Vec<RecordBatch>>,
    staged: Option<(Uuid, i64, Vec<StagedTable>)>,
    written: Vec<StagedTable>,
    commit_attempted: bool,
}

impl IcebergBranch {
    /// Buffer `batch` to append to `table` (`namespace.table`). Columns are
    /// matched by name; table columns the batch lacks are null.
    ///
    /// # Errors
    ///
    /// [`IcebergError`] for a reserved column (`dat_txn`, `dat_tx_time`).
    pub fn append(&mut self, table: &str, batch: RecordBatch) -> Result<(), IcebergError> {
        let schema = batch.schema();
        if let Some(reserved) = RESERVED
            .iter()
            .find(|r| schema.fields().iter().any(|f| f.name() == *r))
        {
            return Err(IcebergError(format!(
                "{reserved} is set by rhizodat; leave it out of the rows"
            )));
        }
        if batch.num_rows() > 0 {
            self.buffer.entry(table.to_owned()).or_default().push(batch);
        }
        Ok(())
    }

    async fn write(
        &self,
        name: &str,
        parts: &[RecordBatch],
        dat_txn: Uuid,
        dat_tx_time: i64,
    ) -> Result<StagedTable, BoxError> {
        let table = self.lake.load(name).await?;
        let md = table.metadata();
        if !md.default_partition_spec().is_unpartitioned() {
            return Err(refuse(format!(
                "table {name:?} is partitioned: not supported by the Rust adapter yet"
            )));
        }
        let schema = md.current_schema().clone();
        let arrow = Arc::new(schema_to_arrow_schema(&schema)?);
        let conformed: Vec<RecordBatch> = parts
            .iter()
            .map(|p| conform(name, &arrow, p, dat_txn, dat_tx_time))
            .collect::<Result<_, _>>()?;
        let names = DefaultFileNameGenerator::new(
            format!("dat-{dat_txn}-{}", self.store),
            None,
            DataFileFormat::Parquet,
        );
        let rolling = RollingFileWriterBuilder::new_with_default_file_size(
            ParquetWriterBuilder::new(WriterProperties::default(), schema),
            table.file_io().clone(),
            DefaultLocationGenerator::new(md)?,
            names,
        );
        let mut writer = DataFileWriterBuilder::new(rolling).build(None).await?;
        for batch in conformed {
            writer.write(batch).await?;
        }
        let files = writer.close().await?;
        let mut paths = Vec::with_capacity(files.len());
        let mut objects = Vec::with_capacity(files.len());
        for f in files {
            paths.push(f.file_path().to_owned());
            let text =
                serialize_data_file_to_json(f, md.default_partition_type(), md.format_version())?;
            match serde_json::from_str(&text)? {
                Json::Object(o) => objects.push(o),
                other => return Err(refuse(format!("a DataFile is not an object: {other}"))),
            }
        }
        Ok(StagedTable {
            table: name.to_owned(),
            files: paths,
            data_files: Some(objects),
            erase: Vec::new(),
        })
    }
}

/// `part` in the table's Arrow schema, with `dat_txn` / `dat_tx_time` filled in.
fn conform(
    table: &str,
    schema: &SchemaRef,
    part: &RecordBatch,
    dat_txn: Uuid,
    dat_tx_time: i64,
) -> Result<RecordBatch, BoxError> {
    let missing: Vec<&str> = RESERVED
        .into_iter()
        .filter(|r| schema.field_with_name(r).is_err())
        .collect();
    if !missing.is_empty() {
        return Err(refuse(format!(
            "table {table:?} has no column(s) {}",
            missing.join(", ")
        )));
    }
    let unknown: Vec<&str> = part
        .schema_ref()
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .filter(|n| schema.field_with_name(n).is_err())
        .collect();
    if !unknown.is_empty() {
        return Err(refuse(format!(
            "table {table:?} has no column(s) {}",
            unknown.join(", ")
        )));
    }
    let n = part.num_rows();
    let unfit = |e: &dyn fmt::Display| refuse(format!("rows do not fit {table:?}: {e}"));
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let column: ArrayRef = match field.name().as_str() {
            "dat_txn" => match field.data_type() {
                DataType::FixedSizeBinary(16) => Arc::new(
                    FixedSizeBinaryArray::try_from_iter(std::iter::repeat_n(dat_txn.as_bytes(), n))
                        .map_err(|e| unfit(&e))?,
                ),
                DataType::Utf8 => Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                    dat_txn.to_string(),
                    n,
                ))),
                DataType::LargeUtf8 => Arc::new(LargeStringArray::from_iter_values(
                    std::iter::repeat_n(dat_txn.to_string(), n),
                )),
                other => {
                    return Err(refuse(format!(
                        "table {table:?}: dat_txn must be uuid or string, not {other}"
                    )));
                }
            },
            "dat_tx_time" => arrow_cast::cast(
                &Int64Array::from_iter_values(std::iter::repeat_n(dat_tx_time, n)),
                field.data_type(),
            )
            .map_err(|e| unfit(&e))?,
            name => match part.column_by_name(name) {
                Some(c) if c.data_type() == field.data_type() => c.clone(),
                Some(c) => arrow_cast::cast(c, field.data_type()).map_err(|e| unfit(&e))?,
                None => new_null_array(field.data_type(), n),
            },
        };
        columns.push(column);
    }
    RecordBatch::try_new(schema.clone(), columns).map_err(|e| unfit(&e))
}

fn record(store: &str, dat_txn: Uuid, tables: &[StagedTable]) -> BranchRecord {
    BranchRecord {
        payload: Some(encode_staged(tables)),
        ..BranchRecord::new(store, gid_for(dat_txn, store), false)
    }
}

#[async_trait]
impl Branch for IcebergBranch {
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
        let mut tables = Vec::with_capacity(self.buffer.len());
        for (name, parts) in &self.buffer {
            let staged = self.write(name, parts, dat_txn, dat_tx_time).await?;
            self.written.push(staged.clone());
            tables.push(staged);
        }
        let payload = encode_staged(&tables);
        self.staged = Some((dat_txn, dat_tx_time, tables));
        Ok(Some(payload))
    }

    async fn prepare(&mut self, _gid: &str) -> Result<(), BoxError> {
        Err(refuse(
            "an Iceberg branch is not two-phase; it is committed after the decision",
        ))
    }

    async fn commit_one_phase(&mut self) -> Result<(), BoxError> {
        let (dat_txn, dat_tx_time, tables) = self
            .staged
            .as_ref()
            .ok_or_else(|| refuse("commit_one_phase before stage"))?;
        self.commit_attempted = true;
        self.lake.finish(tables, *dat_txn, *dat_tx_time).await
    }

    async fn rollback(&mut self) -> Result<(), BoxError> {
        self.buffer.clear();
        if self.written.is_empty() {
            return Ok(());
        }
        if self.commit_attempted
            && let Some((dat_txn, _, tables)) = &self.staged
        {
            let participant = IcebergParticipant {
                lake: self.lake.clone(),
            };
            match participant
                .is_applied(&record(&self.store, *dat_txn, tables), *dat_txn)
                .await
            {
                // Applied, or unknown: keep the files (orphan maintenance removes strays).
                Ok(true) | Err(_) => return Ok(()),
                Ok(false) => {}
            }
        }
        for staged in std::mem::take(&mut self.written) {
            self.lake.delete_files(&staged).await?;
        }
        Ok(())
    }

    fn is_retryable(&self, error: &(dyn Error + 'static)) -> bool {
        is_retryable(error)
    }
}

/// Commits staged files after the decision, or deletes them on abort.
pub struct IcebergParticipant {
    lake: Arc<Lake>,
}

impl fmt::Debug for IcebergParticipant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IcebergParticipant").finish_non_exhaustive()
    }
}

#[async_trait]
impl Participant for IcebergParticipant {
    async fn finish(
        &self,
        branch: &BranchRecord,
        dat_txn: Uuid,
        dat_tx_time: i64,
    ) -> Result<(), BoxError> {
        let tables = decode_staged(branch.payload.as_deref())?;
        self.lake.finish(&tables, dat_txn, dat_tx_time).await
    }

    async fn is_applied(&self, branch: &BranchRecord, dat_txn: Uuid) -> Result<bool, BoxError> {
        let tables = decode_staged(branch.payload.as_deref())?;
        if tables.is_empty() {
            return Ok(false); // without the payload there is nothing to look at: never guess
        }
        for staged in &tables {
            if !self.lake.applied(staged, dat_txn).await? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Delete the staged files (only ever called before a commit decision).
    async fn abort(&self, branch: &BranchRecord) -> Result<(), BoxError> {
        for staged in decode_staged(branch.payload.as_deref())? {
            let table = self.lake.load(&staged.table).await?;
            if !Lake::referenced(&table, &staged.files).await? {
                self.lake.delete_files(&staged).await?;
            }
        }
        Ok(())
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
    fn rest_dsn_becomes_catalog_properties() {
        let props = catalog_properties(
            "iceberg+rest://cat.example:8181/prod?s3.endpoint=http://s3:9000&credential=a:b",
        )
        .unwrap();
        assert_eq!(props["uri"], "http://cat.example:8181");
        assert_eq!(props["warehouse"], "prod");
        assert_eq!(props["s3.endpoint"], "http://s3:9000");
        assert_eq!(props["credential"], "a:b");
        let props = catalog_properties("iceberg+rest://cat.example/?secure=true").unwrap();
        assert_eq!(props["uri"], "https://cat.example");
        assert!(!props.contains_key("warehouse") && !props.contains_key("secure"));
    }

    #[test]
    fn other_catalogs_and_stores_are_refused() {
        for dsn in [
            "iceberg+sql://u:p@db:5432/cat",
            "iceberg+glue://eu-west-1",
            "postgresql://db/x",
        ] {
            assert!(catalog_properties(dsn).is_err(), "{dsn}");
        }
    }

    #[test]
    fn table_names_need_a_namespace() {
        assert!(ident("erp.sales").is_ok());
        assert!(ident("a.b.c").is_ok());
        for bad in ["sales", "", "erp.", ".sales"] {
            assert!(ident(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn conflicts_and_unknown_commit_states_are_retryable() {
        let conflict =
            iceberg::Error::new(ErrorKind::CatalogCommitConflicts, "requirements failed")
                .with_retryable(true);
        assert!(is_retryable(&conflict));
        let unknown = iceberg::Error::new(
            ErrorKind::Unexpected,
            "A server-side gateway timeout occurred; the commit state is unknown.",
        );
        assert!(is_retryable(&unknown));
        let overloaded = iceberg::Error::new(ErrorKind::Unexpected, "unexpected status code")
            .with_context("status", "503 Service Unavailable");
        assert!(is_retryable(&overloaded));
        let wrapped = iceberg::Error::new(ErrorKind::Unexpected, "io failed")
            .with_source(std::io::Error::other("reset"));
        assert!(is_retryable(&wrapped));
        let not_found = iceberg::Error::new(ErrorKind::Unexpected, "unexpected status code")
            .with_context("status", "404 Not Found");
        assert!(!is_retryable(&not_found));
        assert!(!is_retryable(&iceberg::Error::new(
            ErrorKind::DataInvalid,
            "bad schema"
        )));
        assert!(!is_retryable(&IcebergError("refused".into())));
    }
}

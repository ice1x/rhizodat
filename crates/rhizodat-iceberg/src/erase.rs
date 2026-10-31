//! Erasing entities from Iceberg tables, copy-on-write: the port of the Python
//! reference's `tx.delete(dat_id IN ids AND dat_tx_time < t)`.
//!
//! iceberg-rust 0.10 has no delete or overwrite action, and its `TableCommit`
//! cannot be built outside the crate, so the commit is made here, by hand: the
//! data files holding a version of an erased entity older than the
//! meta-transaction are rewritten without those rows, and one `overwrite`
//! snapshot marks them deleted, adds their rewrites (and the branch's own staged
//! files) and keeps every other file. It is committed through the REST catalog
//! protocol (`POST …/namespaces/{ns}/tables/{table}` with the snapshot as a
//! table update), on condition that `main` still points at the snapshot it was
//! built from: a concurrent writer makes it a retryable conflict, never a lost
//! write. Other catalogs refuse erases.
//!
//! Only unpartitioned tables of format version 2 without delete files (the
//! reference's entity part tables) are supported. Older snapshots keep
//! referencing the replaced files until snapshot expiry and orphan-file
//! removal, the usual Iceberg maintenance.

use std::collections::{HashMap, HashSet};

use arrow_array::{
    Array, ArrayRef, BooleanArray, FixedSizeBinaryArray, Int64Array, LargeStringArray, RecordBatch,
    StringArray, new_null_array,
};
use arrow_schema::SchemaRef;
use futures::TryStreamExt;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::expr::Reference;
use iceberg::spec::{
    DataFile, DataFileFormat, Datum, FormatVersion, ManifestContentType, ManifestFile,
    ManifestList, ManifestListWriter, ManifestWriterBuilder, Operation, PrimitiveType, Snapshot,
    SnapshotReference, SnapshotRetention, Summary, Type,
};
use iceberg::table::Table;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{ErrorKind, TableIdent, TableRequirement, TableUpdate};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::WriterProperties;
use rhizodat_core::store::BoxError;
use serde_json::{Value as Json, json};
use uuid::Uuid;

use crate::refuse;

/// A REST catalog's commit endpoint.
pub(crate) struct Rest {
    uri: String,
    warehouse: Option<String>,
    token: Option<String>,
    client: reqwest::Client,
}

impl Rest {
    /// The endpoint of a catalog loaded with `props` (`uri`, `warehouse`, `token`).
    pub(crate) fn new(props: &HashMap<String, String>) -> Self {
        Self {
            uri: props
                .get("uri")
                .cloned()
                .unwrap_or_default()
                .trim_end_matches('/')
                .to_owned(),
            warehouse: props.get("warehouse").cloned(),
            token: props.get("token").cloned(),
            client: reqwest::Client::new(),
        }
    }

    fn authorized(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.token {
            Some(token) => request.bearer_auth(token),
            None => request,
        }
    }

    /// The catalog's path prefix (`overrides.prefix` of `GET /v1/config`).
    async fn prefix(&self) -> Result<Option<String>, BoxError> {
        let mut request = self.client.get(format!("{}/v1/config", self.uri));
        if let Some(warehouse) = &self.warehouse {
            request = request.query(&[("warehouse", warehouse)]);
        }
        let config: Json = self
            .authorized(request)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(config["overrides"]["prefix"]
            .as_str()
            .filter(|p| !p.is_empty())
            .map(str::to_owned))
    }

    /// Commit `updates` to `ident` if `requirements` hold.
    async fn commit(
        &self,
        ident: &TableIdent,
        requirements: &[TableRequirement],
        updates: &[TableUpdate],
    ) -> Result<(), BoxError> {
        let mut url = reqwest::Url::parse(&self.uri)?;
        {
            let mut path = url
                .path_segments_mut()
                .map_err(|()| refuse(format!("not a base URL: {}", self.uri)))?;
            path.push("v1");
            if let Some(prefix) = self.prefix().await? {
                path.extend(prefix.split('/'));
            }
            path.push("namespaces")
                .push(&ident.namespace().join("\u{1f}"))
                .push("tables")
                .push(ident.name());
        }
        let body = json!({
            "identifier": {"namespace": ident.namespace().as_ref(), "name": ident.name()},
            "requirements": requirements,
            "updates": updates,
        });
        let response = self
            .authorized(self.client.post(url))
            .json(&body)
            .send()
            .await?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }
        let text = response.text().await.unwrap_or_default();
        Err(match status.as_u16() {
            409 => Box::new(
                iceberg::Error::new(
                    ErrorKind::CatalogCommitConflicts,
                    format!("{ident}: a concurrent commit moved the table: {text}"),
                )
                .with_retryable(true),
            ),
            500 | 502 | 503 | 504 => Box::new(iceberg::Error::new(
                ErrorKind::Unexpected,
                format!("{ident}: the commit state is unknown (status: {status} ): {text}"),
            )),
            _ => refuse(format!(
                "{ident}: the catalog refused the commit ({status}): {text}"
            )),
        })
    }
}

/// How `dat_id` is stored, to filter it.
#[derive(Clone, Copy)]
enum IdColumn {
    Text,
    Uuid,
}

fn id_column(table: &Table) -> Result<IdColumn, BoxError> {
    let schema = table.metadata().current_schema();
    match schema
        .field_by_name("dat_id")
        .map(|f| f.field_type.as_ref())
    {
        Some(Type::Primitive(PrimitiveType::String)) => Ok(IdColumn::Text),
        Some(Type::Primitive(PrimitiveType::Uuid)) => Ok(IdColumn::Uuid),
        _ => Err(refuse(format!(
            "{}: entities can be erased only from a table with a dat_id column (string or uuid)",
            table.identifier()
        ))),
    }
}

/// Which rows of `batch` to keep: not (`dat_id` in `ids` and `dat_tx_time < bound`).
fn keep(batch: &RecordBatch, ids: &HashSet<Uuid>, bound: i64) -> Result<BooleanArray, BoxError> {
    let column = |name: &str| {
        batch
            .column_by_name(name)
            .ok_or_else(|| refuse(format!("a data file has no {name} column")))
    };
    let (id_col, time_col) = (column("dat_id")?, column("dat_tx_time")?);
    let times = time_col
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| refuse("dat_tx_time is not a long"))?;
    let id_at = |i: usize| -> Option<Uuid> {
        if id_col.is_null(i) {
            return None;
        }
        let any = id_col.as_any();
        if let Some(a) = any.downcast_ref::<StringArray>() {
            return a.value(i).parse().ok();
        }
        if let Some(a) = any.downcast_ref::<LargeStringArray>() {
            return a.value(i).parse().ok();
        }
        any.downcast_ref::<FixedSizeBinaryArray>()
            .and_then(|a| Uuid::from_slice(a.value(i)).ok())
    };
    Ok((0..batch.num_rows())
        .map(|i| {
            let erased = id_at(i).is_some_and(|id| ids.contains(&id))
                && !times.is_null(i)
                && times.value(i) < bound;
            Some(!erased)
        })
        .collect())
}

/// `batch` in the table's Arrow schema (columns by name, cast; missing ones null).
fn align(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch, BoxError> {
    let columns = schema
        .fields()
        .iter()
        .map(|field| match batch.column_by_name(field.name()) {
            Some(column) if column.data_type() == field.data_type() => Ok(column.clone()),
            Some(column) => Ok(arrow_cast::cast(column, field.data_type())?),
            None => Ok(new_null_array(field.data_type(), batch.num_rows())),
        })
        .collect::<Result<Vec<ArrayRef>, BoxError>>()?;
    Ok(RecordBatch::try_new(schema.clone(), columns)?)
}

/// The paths of the live data files that may hold an erased version.
async fn candidates(
    table: &Table,
    kind: IdColumn,
    ids: &HashSet<Uuid>,
    bound: i64,
) -> Result<HashSet<String>, BoxError> {
    let literals: Vec<Datum> = ids
        .iter()
        .map(|id| match kind {
            IdColumn::Text => Datum::string(id.to_string()),
            IdColumn::Uuid => Datum::uuid(*id),
        })
        .collect();
    let filter = Reference::new("dat_id")
        .is_in(literals)
        .and(Reference::new("dat_tx_time").less_than(Datum::long(bound)));
    Ok(table
        .scan()
        .with_filter(filter)
        .build()?
        .plan_files()
        .await?
        .map_ok(|task| task.data_file_path().to_owned())
        .try_collect()
        .await?)
}

/// Whether `table` holds a version of `ids` written by another transaction
/// (the reference's `versions_besides`).
pub(crate) async fn versions_besides(
    table: &Table,
    ids: &[String],
    dat_txn: Uuid,
) -> Result<bool, BoxError> {
    if table.metadata().current_snapshot().is_none() {
        return Ok(false);
    }
    let kind = id_column(table)?;
    let ids: HashSet<Uuid> = ids.iter().map(|i| i.parse()).collect::<Result<_, _>>()?;
    for path in candidates(table, kind, &ids, i64::MAX).await? {
        for batch in read(table, &path).await? {
            let txns = batch.column_by_name("dat_txn");
            let mask = keep(&batch, &ids, i64::MAX)?;
            for i in 0..batch.num_rows() {
                if mask.value(i) {
                    continue; // not a version of an erased entity
                }
                let other = txns.is_none_or(|t| !txn_is(t, i, dat_txn));
                if other {
                    return Ok(true);
                }
            }
        }
    }
    Ok(false)
}

fn txn_is(column: &ArrayRef, i: usize, dat_txn: Uuid) -> bool {
    if column.is_null(i) {
        return false;
    }
    let any = column.as_any();
    if let Some(a) = any.downcast_ref::<StringArray>() {
        return a.value(i).parse::<Uuid>().ok() == Some(dat_txn);
    }
    if let Some(a) = any.downcast_ref::<LargeStringArray>() {
        return a.value(i).parse::<Uuid>().ok() == Some(dat_txn);
    }
    any.downcast_ref::<FixedSizeBinaryArray>()
        .is_some_and(|a| a.value(i) == dat_txn.as_bytes())
}

async fn read(table: &Table, path: &str) -> Result<Vec<RecordBatch>, BoxError> {
    let bytes = table.file_io().new_input(path)?.read().await?;
    Ok(ParquetRecordBatchReaderBuilder::try_new(bytes)?
        .build()?
        .collect::<Result<Vec<_>, _>>()?)
}

/// The manifests of the current snapshot (none without one); delete files
/// (merge-on-read tables) are refused.
async fn current_manifests(table: &Table) -> Result<Vec<ManifestFile>, BoxError> {
    let md = table.metadata();
    let Some(snapshot) = md.current_snapshot() else {
        return Ok(Vec::new());
    };
    let bytes = table
        .file_io()
        .new_input(snapshot.manifest_list())?
        .read()
        .await?;
    let manifests = ManifestList::parse_with_version(&bytes, md.format_version())?
        .entries()
        .to_vec();
    if manifests
        .iter()
        .any(|m| m.content != ManifestContentType::Data)
    {
        return Err(refuse(format!(
            "{}: the table has delete files; entities can be erased only from \
             copy-on-write tables",
            table.identifier()
        )));
    }
    Ok(manifests)
}

/// Rewrite every file holding a version of `ids` older than `bound` without
/// those rows: the replaced paths and the new data files.
async fn rewrite(
    table: &Table,
    ids: &HashSet<Uuid>,
    bound: i64,
    dat_txn: Uuid,
) -> Result<(HashSet<String>, Vec<DataFile>), BoxError> {
    let md = table.metadata();
    let mut replaced: HashSet<String> = HashSet::new();
    let mut kept: Vec<RecordBatch> = Vec::new();
    if md.current_snapshot().is_some() && !ids.is_empty() {
        let kind = id_column(table)?;
        for path in candidates(table, kind, ids, bound).await? {
            let mut removed = false;
            let mut rest_of_file = Vec::new();
            for batch in &read(table, &path).await? {
                let mask = keep(batch, ids, bound)?;
                removed |= mask.true_count() < batch.num_rows();
                rest_of_file.push(arrow_select::filter::filter_record_batch(batch, &mask)?);
            }
            if removed {
                replaced.insert(path);
                kept.extend(rest_of_file.into_iter().filter(|b| b.num_rows() > 0));
            }
        }
    }
    if kept.is_empty() {
        return Ok((replaced, Vec::new()));
    }
    let schema = md.current_schema().clone();
    let arrow = std::sync::Arc::new(schema_to_arrow_schema(&schema)?);
    let names = DefaultFileNameGenerator::new(
        format!("dat-{dat_txn}-rewrite"),
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
    for batch in &kept {
        writer.write(align(batch, &arrow)?).await?;
    }
    Ok((replaced, writer.close().await?))
}

/// The new snapshot's manifests: one with `new_files`, the untouched ones as
/// they are, and every manifest naming a `replaced` file rewritten (that file
/// deleted, the rest existing).
async fn manifests_for(
    table: &Table,
    snapshot_id: i64,
    sequence: i64,
    manifests: Vec<ManifestFile>,
    replaced: &HashSet<String>,
    new_files: Vec<DataFile>,
) -> Result<Vec<ManifestFile>, BoxError> {
    let md = table.metadata();
    let schema = md.current_schema().clone();
    let spec = md.default_partition_spec().as_ref().clone();
    let writer = || -> Result<_, BoxError> {
        let path = format!("{}/metadata/{}-m.avro", md.location(), Uuid::new_v4());
        Ok(ManifestWriterBuilder::new(
            table.file_io().new_output(path)?,
            Some(snapshot_id),
            schema.clone(),
            spec.clone(),
        )
        .build_v2_data())
    };
    let mut out: Vec<ManifestFile> = Vec::with_capacity(manifests.len() + 1);
    if !new_files.is_empty() {
        let mut added = writer()?;
        for file in new_files {
            added.add_file(file, sequence)?;
        }
        out.push(added.write_manifest_file().await?);
    }
    for manifest in manifests {
        let entries = manifest.load_manifest(table.file_io()).await?;
        let alive = || entries.entries().iter().filter(|e| e.is_alive());
        if !alive().any(|e| replaced.contains(e.file_path())) {
            out.push(manifest);
            continue;
        }
        let mut rewritten = writer()?;
        for entry in alive() {
            let data_seq = entry.sequence_number().unwrap_or(manifest.sequence_number);
            let file = entry.data_file().clone();
            if replaced.contains(entry.file_path()) {
                rewritten.add_delete_file(file, data_seq, entry.file_sequence_number)?;
            } else {
                let added_by = entry.snapshot_id().unwrap_or(manifest.added_snapshot_id);
                rewritten.add_existing_file(
                    file,
                    added_by,
                    data_seq,
                    entry.file_sequence_number,
                )?;
            }
        }
        out.push(rewritten.write_manifest_file().await?);
    }
    Ok(out)
}

/// Rewrite the files holding versions of `ids` older than `bound` without them,
/// and commit one snapshot that also adds `added` (the branch's staged files).
/// Nothing is committed when there is nothing to change.
pub(crate) async fn overwrite(
    rest: &Rest,
    table: &Table,
    ids: &[String],
    bound: i64,
    added: Vec<DataFile>,
    dat_txn: Uuid,
    properties: HashMap<String, String>,
) -> Result<(), BoxError> {
    let md = table.metadata();
    let name = table.identifier();
    if md.format_version() != FormatVersion::V2 {
        return Err(refuse(format!(
            "{name}: entities can be erased only from format version 2 tables"
        )));
    }
    if !md.default_partition_spec().is_unpartitioned() {
        return Err(refuse(format!(
            "{name}: entities can be erased only from unpartitioned tables"
        )));
    }
    let ids: HashSet<Uuid> = ids.iter().map(|i| i.parse()).collect::<Result<_, _>>()?;
    let manifests = current_manifests(table).await?;
    let (replaced, rewrites) = rewrite(table, &ids, bound, dat_txn).await?;
    if replaced.is_empty() && added.is_empty() {
        return Ok(());
    }
    let snapshot_id = loop {
        let id = i64::from_le_bytes(Uuid::new_v4().as_bytes()[..8].try_into()?) & i64::MAX;
        if id != 0 && md.snapshot_by_id(id).is_none() {
            break id;
        }
    };
    let sequence = md.next_sequence_number();
    let mut new_files = added;
    new_files.extend(rewrites);
    let list = manifests_for(
        table,
        snapshot_id,
        sequence,
        manifests,
        &replaced,
        new_files,
    )
    .await?;
    let parent = md.current_snapshot().map(|s| s.snapshot_id());
    let list_path = format!(
        "{}/metadata/snap-{snapshot_id}-0-{}.avro",
        md.location(),
        Uuid::new_v4()
    );
    let mut list_writer = ManifestListWriter::v2(
        table.file_io().new_output(&list_path)?.writer().await?,
        snapshot_id,
        parent,
        sequence,
    );
    list_writer.add_manifests(list.into_iter())?;
    list_writer.close().await?;
    let operation = if replaced.is_empty() {
        Operation::Append
    } else {
        Operation::Overwrite
    };
    let snapshot = Snapshot::builder()
        .with_snapshot_id(snapshot_id)
        .with_parent_snapshot_id(parent)
        .with_sequence_number(sequence)
        .with_timestamp_ms(rhizodat_core::identity::system_ms())
        .with_manifest_list(list_path)
        .with_summary(Summary {
            operation,
            additional_properties: properties,
        })
        .with_schema_id(md.current_schema_id())
        .build();
    let requirements = [
        TableRequirement::UuidMatch { uuid: md.uuid() },
        TableRequirement::RefSnapshotIdMatch {
            r#ref: "main".to_owned(),
            snapshot_id: parent,
        },
    ];
    let updates = [
        TableUpdate::AddSnapshot { snapshot },
        TableUpdate::SetSnapshotRef {
            ref_name: "main".to_owned(),
            reference: SnapshotReference::new(
                snapshot_id,
                SnapshotRetention::branch(None, None, None),
            ),
        },
    ];
    rest.commit(name, &requirements, &updates).await
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    fn batch(ids: &[Option<&str>], times: &[Option<i64>]) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("dat_id", DataType::Utf8, true),
            Field::new("dat_tx_time", DataType::Int64, true),
        ]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from(ids.to_vec())),
                Arc::new(Int64Array::from(times.to_vec())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn only_older_versions_of_erased_entities_go() {
        let (a, b) = (Uuid::from_u128(1), Uuid::from_u128(2));
        let (a_text, b_text) = (a.to_string(), b.to_string());
        let rows = batch(
            &[
                Some(&a_text),
                Some(&a_text),
                Some(&b_text),
                None,
                Some("not a uuid"),
            ],
            &[Some(10), Some(20), Some(10), Some(10), Some(10)],
        );
        let mask = keep(&rows, &HashSet::from([a]), 15).unwrap();
        let kept: Vec<bool> = mask.iter().map(Option::unwrap).collect();
        assert_eq!(kept, [false, true, true, true, true]);
    }

    #[test]
    fn aligned_batches_take_the_tables_columns() {
        let target: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("dat_tx_time", DataType::Int64, true),
            Field::new("dat_id", DataType::Utf8, true),
            Field::new("extra", DataType::Utf8, true),
        ]));
        let aligned = align(&batch(&[Some("x")], &[Some(1)]), &target).unwrap();
        assert_eq!(aligned.schema(), target);
        assert!(aligned.column(2).is_null(0));
    }

    #[test]
    fn the_endpoint_comes_from_the_catalog_properties() {
        let rest = Rest::new(&HashMap::from([
            ("uri".to_owned(), "http://h:1/".to_owned()),
            ("token".to_owned(), "t".to_owned()),
        ]));
        assert_eq!(rest.uri, "http://h:1");
        assert_eq!(rest.token.as_deref(), Some("t"));
        assert_eq!(rest.warehouse, None);
    }
}

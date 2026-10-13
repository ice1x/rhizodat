//! datorium-iceberg on two catalogs: iceberg-rust's in-memory catalog over a
//! temporary local warehouse (always), and the compose REST catalog over S3.
//!
//! The REST DSN comes from `DATORIUM_TEST_ICEBERG_REST_DSN`, else from
//! `.env.test`; one from `.env.test` whose server is not running skips the
//! REST half with a hint. Each test gets its own namespace, dropped afterwards.
//! The REST fixture keeps its catalog in SQLite and answers concurrent writes
//! with `SQLITE_BUSY`, so tests take turns on it ([`REST`]).

use std::collections::HashMap;
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{
    Array, ArrayRef, Decimal128Array, FixedSizeBinaryArray, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use datorium_core::clock::HybridLogicalClock;
use datorium_core::coordinator::{Coordinator, Outcome};
use datorium_core::gid::gid_for;
use datorium_core::identity::IdGenerator;
use datorium_core::journal::{BranchRecord, Journal, State};
use datorium_core::recovery::Recovery;
use datorium_core::staged::decode_staged;
use datorium_core::store::{Branch, Participant, Participants};
use datorium_core::timer::TokioTimer;
use datorium_iceberg::{IcebergBranch, IcebergStore, is_retryable};
use datorium_postgres::{PostgresJournal, PostgresStore};
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use uuid::Uuid;

const TS: i64 = 1_760_000_000_000 << 16;

/// One test at a time on the REST catalog.
static REST: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn env_test(key: &str) -> Option<String> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.env.test");
    let text = std::fs::read_to_string(path).ok()?;
    let prefix = format!("{key}=");
    text.lines()
        .filter(|l| !l.starts_with('#'))
        .find_map(|l| l.strip_prefix(&prefix))
        .map(str::to_owned)
}

/// The DSN under `key`, or `None` (skip) when it comes from `.env.test` and nothing listens.
fn dsn(key: &str) -> Option<String> {
    if let Ok(dsn) = std::env::var(key) {
        return Some(dsn);
    }
    let dsn = env_test(key)?;
    let parsed = datorium_iceberg::ADAPTER
        .parse(&dsn)
        .expect("a valid DSN in .env.test");
    let addr = (
        parsed.host().unwrap_or("127.0.0.1"),
        parsed.port().unwrap_or(0),
    )
        .to_socket_addrs()
        .ok()?
        .next()?;
    if TcpStream::connect_timeout(&addr, Duration::from_millis(500)).is_ok() {
        Some(dsn)
    } else {
        eprintln!("skipped: {key} from .env.test is not running (docker compose up -d --wait)");
        None
    }
}

fn new_id() -> Uuid {
    IdGenerator::new().generate().unwrap()
}

fn sales_schema() -> Schema {
    Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "order_id", Type::Primitive(PrimitiveType::Uuid)).into(),
            NestedField::required(2, "sku", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(3, "qty", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(
                4,
                "price",
                Type::Primitive(PrimitiveType::Decimal {
                    precision: 12,
                    scale: 2,
                }),
            )
            .into(),
            NestedField::optional(5, "at", Type::Primitive(PrimitiveType::Timestamptz)).into(),
            NestedField::optional(6, "dat_txn", Type::Primitive(PrimitiveType::Uuid)).into(),
            NestedField::optional(7, "dat_tx_time", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()
        .unwrap()
}

/// `n` sales as the caller builds them: plain Arrow, no Iceberg field ids.
fn sales(n: usize) -> RecordBatch {
    let ids: Vec<[u8; 16]> = (0..n).map(|_| *Uuid::new_v4().as_bytes()).collect();
    let schema = ArrowSchema::new(vec![
        Field::new("order_id", DataType::FixedSizeBinary(16), false),
        Field::new("sku", DataType::Utf8, false),
        Field::new("qty", DataType::Int64, true),
        Field::new("price", DataType::Decimal128(12, 2), true),
        Field::new(
            "at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
    ]);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(FixedSizeBinaryArray::try_from_iter(ids.iter()).unwrap()),
        Arc::new(StringArray::from_iter_values(
            (0..n).map(|i| format!("SKU-{i:04}")),
        )),
        Arc::new(Int64Array::from_iter_values(1..=i64::try_from(n).unwrap())),
        Arc::new(
            Decimal128Array::from_iter_values((0..n).map(|i| 1999 + i as i128))
                .with_precision_and_scale(12, 2)
                .unwrap(),
        ),
        Arc::new(
            TimestampMicrosecondArray::from_iter_values(
                (0..i64::try_from(n).unwrap()).map(|i| 1_760_000_000_000_000 + i),
            )
            .with_timezone("UTC"),
        ),
    ];
    RecordBatch::try_new(Arc::new(schema), columns).unwrap()
}

fn one_column(name: &str, column: ArrayRef) -> RecordBatch {
    let field = Field::new(name, column.data_type().clone(), true);
    RecordBatch::try_new(Arc::new(ArrowSchema::new(vec![field])), vec![column]).unwrap()
}

/// One catalog with a fresh namespace holding `sales`.
struct Lake {
    store: IcebergStore,
    ns: String,
    /// Keeps the local warehouse of the memory catalog alive.
    _warehouse: Option<tempfile::TempDir>,
}

impl Lake {
    async fn memory() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let catalog = MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "memory",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_owned(),
                    format!("file://{}", dir.path().display()),
                )]),
            )
            .await
            .unwrap();
        let store = IcebergStore::with_catalog("lake", Arc::new(catalog));
        Self::setup(store, Some(dir)).await
    }

    async fn rest() -> Option<Self> {
        let dsn = dsn("DATORIUM_TEST_ICEBERG_REST_DSN")?;
        let store = IcebergStore::connect(&dsn, "lake").await.unwrap();
        Some(Self::setup(store, None).await)
    }

    async fn setup(store: IcebergStore, warehouse: Option<tempfile::TempDir>) -> Self {
        let ns = format!("dat_rs_{}", &new_id().simple().to_string()[20..]);
        let lake = Self {
            store,
            ns,
            _warehouse: warehouse,
        };
        lake.catalog()
            .create_namespace(&NamespaceIdent::new(lake.ns.clone()), HashMap::new())
            .await
            .unwrap();
        lake.create("sales", sales_schema()).await;
        lake
    }

    fn catalog(&self) -> &Arc<dyn Catalog> {
        self.store.catalog()
    }

    fn table(&self, name: &str) -> String {
        format!("{}.{name}", self.ns)
    }

    fn ident(&self, name: &str) -> TableIdent {
        TableIdent::from_strs([self.ns.as_str(), name]).unwrap()
    }

    async fn create(&self, name: &str, schema: Schema) {
        self.catalog()
            .create_table(
                &NamespaceIdent::new(self.ns.clone()),
                TableCreation::builder()
                    .name(name.to_owned())
                    .schema(schema)
                    .build(),
            )
            .await
            .unwrap();
    }

    /// `dat.txn` of every snapshot of `name`, oldest first.
    async fn snapshots(&self, name: &str) -> Vec<Option<String>> {
        let table = self.catalog().load_table(&self.ident(name)).await.unwrap();
        let mut snapshots: Vec<_> = table.metadata().snapshots().collect();
        snapshots.sort_by_key(|s| s.sequence_number());
        snapshots
            .iter()
            .map(|s| s.summary().additional_properties.get("dat.txn").cloned())
            .collect()
    }

    async fn rows(&self, name: &str) -> usize {
        self.store
            .scan(&self.table(name))
            .await
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum()
    }

    async fn exists(&self, name: &str, path: &str) -> bool {
        let table = self.catalog().load_table(&self.ident(name)).await.unwrap();
        table.file_io().exists(path).await.unwrap()
    }

    async fn drop(self) {
        for t in self
            .catalog()
            .list_tables(&NamespaceIdent::new(self.ns.clone()))
            .await
            .unwrap()
        {
            self.catalog().drop_table(&t).await.unwrap();
        }
        self.catalog()
            .drop_namespace(&NamespaceIdent::new(self.ns.clone()))
            .await
            .unwrap();
    }
}

/// Run `test` on the memory catalog, then on REST if it is up.
async fn each(test: impl AsyncFn(&Lake)) {
    let lake = Lake::memory().await;
    test(&lake).await;
    lake.drop().await;
    let _turn = REST.lock().await;
    if let Some(lake) = Lake::rest().await {
        test(&lake).await;
        lake.drop().await;
    }
}

fn record(store: &str, txn: Uuid, payload: Option<Vec<u8>>) -> BranchRecord {
    BranchRecord {
        payload,
        ..BranchRecord::new(store, gid_for(txn, store), false)
    }
}

async fn staged(lake: &Lake, rows: usize) -> (Uuid, IcebergBranch, Vec<u8>) {
    let txn = new_id();
    let mut branch = lake.store.branch();
    branch.append(&lake.table("sales"), sales(rows)).unwrap();
    let payload = branch.stage(txn, TS).await.unwrap().unwrap();
    (txn, branch, payload)
}

fn files(payload: &[u8]) -> Vec<String> {
    decode_staged(Some(payload))
        .unwrap()
        .into_iter()
        .flat_map(|t| t.files)
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn staged_files_are_invisible() {
    each(async |lake| {
        let (_, _, payload) = staged(lake, 3).await;
        assert_eq!(lake.snapshots("sales").await, Vec::<Option<String>>::new());
        let paths = files(&payload);
        assert_eq!(paths.len(), 1, "{paths:?}");
        assert!(lake.exists("sales", &paths[0]).await);
        let tables = decode_staged(Some(&payload)).unwrap();
        assert_eq!(tables[0].table, lake.table("sales"));
        assert_eq!(tables[0].data_files.as_ref().map(Vec::len), Some(1));
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn one_phase_commit_adds_one_tagged_snapshot() {
    each(async |lake| {
        let (txn, mut branch, _) = staged(lake, 3).await;
        branch.commit_one_phase().await.unwrap();
        assert_eq!(lake.snapshots("sales").await, [Some(txn.to_string())]);
        let table = lake
            .catalog()
            .load_table(&lake.ident("sales"))
            .await
            .unwrap();
        let summary = table.metadata().current_snapshot().unwrap().summary();
        assert_eq!(
            summary.additional_properties.get("dat.tx_time"),
            Some(&TS.to_string())
        );
        let batches = lake.store.scan(&lake.table("sales")).await.unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
        for b in &batches {
            let txns = b
                .column_by_name("dat_txn")
                .unwrap()
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            assert!(txns.iter().all(|t| t == Some(txn.as_bytes().as_slice())));
            let times = b
                .column_by_name("dat_tx_time")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert!(times.iter().all(|t| t == Some(TS)));
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn appends_to_one_table_become_one_snapshot() {
    each(async |lake| {
        let txn = new_id();
        let mut branch = lake.store.branch();
        branch.append(&lake.table("sales"), sales(2)).unwrap();
        branch.append(&lake.table("sales"), sales(3)).unwrap();
        branch.stage(txn, TS).await.unwrap();
        branch.commit_one_phase().await.unwrap();
        assert_eq!(lake.snapshots("sales").await.len(), 1);
        assert_eq!(lake.rows("sales").await, 5);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_optional_columns_are_null_and_types_are_cast() {
    each(async |lake| {
        let ids = [*Uuid::new_v4().as_bytes()];
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                Field::new("order_id", DataType::FixedSizeBinary(16), false),
                Field::new("sku", DataType::Utf8, false),
                Field::new("qty", DataType::Int32, true),
            ])),
            vec![
                Arc::new(FixedSizeBinaryArray::try_from_iter(ids.iter()).unwrap()),
                Arc::new(StringArray::from(vec!["SKU-1"])),
                Arc::new(arrow_array::Int32Array::from(vec![7])),
            ],
        )
        .unwrap();
        let mut branch = lake.store.branch();
        branch.append(&lake.table("sales"), batch).unwrap();
        branch.stage(new_id(), TS).await.unwrap();
        branch.commit_one_phase().await.unwrap();
        let batches = lake.store.scan(&lake.table("sales")).await.unwrap();
        let b = &batches[0];
        let qty = b.column_by_name("qty").unwrap();
        assert_eq!(
            qty.as_any().downcast_ref::<Int64Array>().unwrap().value(0),
            7
        );
        assert_eq!(b.column_by_name("price").unwrap().null_count(), 1);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn reserved_columns_are_set_by_datorium() {
    let lake = Lake::memory().await;
    let mut branch = lake.store.branch();
    let column: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let err = branch
        .append(&lake.table("sales"), one_column("dat_tx_time", column))
        .unwrap_err();
    assert!(err.to_string().contains("set by datorium"), "{err}");
    lake.drop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn stage_rejects_what_does_not_fit_the_table() {
    each(async |lake| {
        let unknown: ArrayRef = Arc::new(StringArray::from(vec!["x"]));
        let mut branch = lake.store.branch();
        branch
            .append(&lake.table("sales"), one_column("colour", unknown))
            .unwrap();
        let err = branch.stage(new_id(), TS).await.unwrap_err();
        assert!(err.to_string().contains("no column(s) colour"), "{err}");
        assert!(!branch.is_retryable(err.as_ref()));

        // sku is required.
        let qty: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let mut branch = lake.store.branch();
        branch
            .append(&lake.table("sales"), one_column("qty", qty))
            .unwrap();
        let err = branch.stage(new_id(), TS).await.unwrap_err();
        assert!(err.to_string().contains("do not fit"), "{err}");

        // A list cannot become a long.
        let wrong: ArrayRef = Arc::new(arrow_array::ListArray::from_iter_primitive::<
            arrow_array::types::Int64Type,
            _,
            _,
        >(vec![Some(vec![Some(1)])]));
        let mut branch = lake.store.branch();
        branch
            .append(&lake.table("sales"), one_column("qty", wrong))
            .unwrap();
        let err = branch.stage(new_id(), TS).await.unwrap_err();
        assert!(err.to_string().contains("do not fit"), "{err}");
        assert_eq!(lake.snapshots("sales").await.len(), 0);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn stage_rejects_a_table_without_transaction_columns() {
    each(async |lake| {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "sku", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();
        lake.create("plain", schema).await;
        let sku: ArrayRef = Arc::new(StringArray::from(vec!["x"]));
        let mut branch = lake.store.branch();
        branch
            .append(&lake.table("plain"), one_column("sku", sku))
            .unwrap();
        let err = branch.stage(new_id(), TS).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("no column(s) dat_txn, dat_tx_time"),
            "{err}"
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_string_dat_txn_column_gets_the_text() {
    each(async |lake| {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "sku", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(2, "dat_txn", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(3, "dat_tx_time", Type::Primitive(PrimitiveType::Long))
                    .into(),
            ])
            .build()
            .unwrap();
        lake.create("notes", schema).await;
        let sku: ArrayRef = Arc::new(StringArray::from(vec!["x"]));
        let txn = new_id();
        let mut branch = lake.store.branch();
        branch
            .append(&lake.table("notes"), one_column("sku", sku))
            .unwrap();
        branch.stage(txn, TS).await.unwrap();
        branch.commit_one_phase().await.unwrap();
        let batches = lake.store.scan(&lake.table("notes")).await.unwrap();
        let txns = batches[0].column_by_name("dat_txn").unwrap();
        let txns = txns.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(txns.value(0), txn.to_string());
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn stage_fails_for_a_missing_table() {
    each(async |lake| {
        let mut branch = lake.store.branch();
        branch.append(&lake.table("nope"), sales(1)).unwrap();
        let err = branch.stage(new_id(), TS).await.unwrap_err();
        assert!(!branch.is_retryable(err.as_ref()), "{err}");
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn partitioned_tables_are_refused_at_stage() {
    each(async |lake| {
        let spec = iceberg::spec::UnboundPartitionSpec::builder()
            .with_spec_id(0)
            .add_partition_field(2, "sku", iceberg::spec::Transform::Identity)
            .unwrap()
            .build();
        lake.catalog()
            .create_table(
                &NamespaceIdent::new(lake.ns.clone()),
                TableCreation::builder()
                    .name("by_sku".to_owned())
                    .schema(sales_schema())
                    .partition_spec(spec)
                    .build(),
            )
            .await
            .unwrap();
        let mut branch = lake.store.branch();
        branch.append(&lake.table("by_sku"), sales(1)).unwrap();
        let err = branch.stage(new_id(), TS).await.unwrap_err();
        assert!(err.to_string().contains("partitioned"), "{err}");
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn rollback_deletes_staged_files() {
    each(async |lake| {
        let (_, mut branch, payload) = staged(lake, 2).await;
        branch.rollback().await.unwrap();
        for path in files(&payload) {
            assert!(!lake.exists("sales", &path).await, "{path}");
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn rollback_after_an_applied_commit_keeps_the_files() {
    each(async |lake| {
        let (_, mut branch, payload) = staged(lake, 2).await;
        branch.commit_one_phase().await.unwrap();
        branch.rollback().await.unwrap();
        for path in files(&payload) {
            assert!(lake.exists("sales", &path).await, "{path}");
        }
        assert_eq!(lake.rows("sales").await, 2);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn rollback_keeps_files_when_the_outcome_is_unknown() {
    each(async |lake| {
        let (_, mut branch, payload) = staged(lake, 2).await;
        branch.commit_one_phase().await.unwrap();
        // The table is gone, so whether the commit landed cannot be checked.
        let table = lake
            .catalog()
            .load_table(&lake.ident("sales"))
            .await
            .unwrap();
        lake.catalog()
            .drop_table(&lake.ident("sales"))
            .await
            .unwrap();
        branch.rollback().await.unwrap();
        for path in files(&payload) {
            assert!(table.file_io().exists(&path).await.unwrap(), "{path}");
        }
        lake.create("sales", sales_schema()).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn branch_is_not_two_phase() {
    let lake = Lake::memory().await;
    let mut branch = lake.store.branch();
    assert!(!branch.two_phase());
    assert!(branch.prepare("dat:x:lake").await.is_err());
    assert!(branch.commit_one_phase().await.is_err(), "before stage");
    lake.drop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn finish_commits_from_the_journal_payload() {
    each(async |lake| {
        let (txn, _, payload) = staged(lake, 3).await;
        let participant = lake.store.participant();
        let rec = record("lake", txn, Some(payload));
        assert!(!participant.is_applied(&rec, txn).await.unwrap());
        participant.finish(&rec, txn, TS).await.unwrap();
        assert!(participant.is_applied(&rec, txn).await.unwrap());
        assert_eq!(lake.rows("sales").await, 3);
        assert_eq!(lake.snapshots("sales").await, [Some(txn.to_string())]);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn finish_is_idempotent() {
    each(async |lake| {
        let (txn, _, payload) = staged(lake, 3).await;
        let participant = lake.store.participant();
        let rec = record("lake", txn, Some(payload));
        participant.finish(&rec, txn, TS).await.unwrap();
        participant.finish(&rec, txn, TS).await.unwrap();
        assert_eq!(lake.snapshots("sales").await.len(), 1);
        assert_eq!(lake.rows("sales").await, 3);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn files_already_referenced_count_as_applied_without_the_tag() {
    // The tagged snapshot may have been expired: the files decide.
    each(async |lake| {
        let (txn, _, payload) = staged(lake, 2).await;
        let participant = lake.store.participant();
        participant
            .finish(&record("lake", txn, Some(payload.clone())), txn, TS)
            .await
            .unwrap();
        let other = new_id();
        let rec = record("lake", other, Some(payload));
        assert!(participant.is_applied(&rec, other).await.unwrap());
        participant.finish(&rec, other, TS).await.unwrap();
        assert_eq!(lake.snapshots("sales").await.len(), 1);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn finish_skips_tables_already_committed() {
    each(async |lake| {
        lake.create("refunds", sales_schema()).await;
        let txn = new_id();
        let mut branch = lake.store.branch();
        branch.append(&lake.table("sales"), sales(2)).unwrap();
        branch.append(&lake.table("refunds"), sales(1)).unwrap();
        let payload = branch.stage(txn, TS).await.unwrap().unwrap();
        let participant = lake.store.participant();
        // A crash after the first table's snapshot: only sales is in.
        let mut first = decode_staged(Some(&payload)).unwrap();
        first.retain(|t| t.table == lake.table("sales"));
        let first = datorium_core::staged::encode_staged(&first);
        participant
            .finish(&record("lake", txn, Some(first)), txn, TS)
            .await
            .unwrap();
        let rec = record("lake", txn, Some(payload));
        assert!(!participant.is_applied(&rec, txn).await.unwrap());
        participant.finish(&rec, txn, TS).await.unwrap();
        assert_eq!(lake.snapshots("sales").await.len(), 1);
        assert_eq!(lake.snapshots("refunds").await.len(), 1);
        assert_eq!(
            (lake.rows("sales").await, lake.rows("refunds").await),
            (2, 1)
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_payload_with_paths_only_is_committed_from_the_parquet_footers() {
    // What the Python reference writes: no DataFile objects.
    each(async |lake| {
        let (txn, _, payload) = staged(lake, 4).await;
        let paths = files(&payload);
        let python = format!(
            r#"{{"v":1,"tables":[{{"table":"{}","files":["{}"]}}]}}"#,
            lake.table("sales"),
            paths[0]
        );
        let rec = record("lake", txn, Some(python.into_bytes()));
        let participant = lake.store.participant();
        participant.finish(&rec, txn, TS).await.unwrap();
        assert!(participant.is_applied(&rec, txn).await.unwrap());
        assert_eq!(lake.rows("sales").await, 4);
        assert_eq!(lake.snapshots("sales").await, [Some(txn.to_string())]);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn nothing_is_applied_without_a_payload() {
    let lake = Lake::memory().await;
    let txn = new_id();
    let participant = lake.store.participant();
    assert!(
        !participant
            .is_applied(&record("lake", txn, None), txn)
            .await
            .unwrap()
    );
    assert_eq!(
        participant.prepared_gids().await.unwrap(),
        Vec::<String>::new()
    );
    lake.drop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn abort_deletes_staged_files_but_never_referenced_ones() {
    each(async |lake| {
        let participant = lake.store.participant();
        let (txn, _, payload) = staged(lake, 1).await;
        participant
            .abort(&record("lake", txn, Some(payload.clone())))
            .await
            .unwrap();
        for path in files(&payload) {
            assert!(!lake.exists("sales", &path).await, "{path}");
        }

        let (txn, _, payload) = staged(lake, 1).await;
        let rec = record("lake", txn, Some(payload.clone()));
        participant.finish(&rec, txn, TS).await.unwrap();
        participant.abort(&rec).await.unwrap();
        for path in files(&payload) {
            assert!(lake.exists("sales", &path).await, "{path}");
        }
        assert_eq!(lake.rows("sales").await, 1);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_catalog_that_is_down_is_retryable() {
    // Connecting is lazy: the first request finds the catalog down.
    let store = IcebergStore::connect("iceberg+rest://127.0.0.1:1/", "lake")
        .await
        .unwrap();
    let err = store.scan("erp.sales").await.unwrap_err();
    assert!(is_retryable(err.as_ref()), "{err}");
}

// --- with PostgreSQL: orders in the ERP, their sales in the lake ----------------------

struct Erp {
    _turn: tokio::sync::MutexGuard<'static, ()>,
    lake: Lake,
    pg_dsn: String,
    schema: String,
    admin: tokio_postgres::Client,
    orders: PostgresStore,
    journal: Arc<PostgresJournal>,
}

impl Erp {
    async fn new() -> Option<Self> {
        let pg_dsn = dsn("DATORIUM_TEST_PG_DSN")?;
        let turn = REST.lock().await;
        let lake = Lake::rest().await?;
        let (admin, conn) = tokio_postgres::connect(&pg_dsn, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(conn);
        let schema = lake.ns.clone();
        admin
            .batch_execute(&format!(
                "CREATE SCHEMA {schema}; CREATE SCHEMA {schema}_orders"
            ))
            .await
            .unwrap();
        let orders = PostgresStore::new(
            &pg_dsn,
            &format!("orders_{schema}"),
            Some(&format!("{schema}_orders")),
        )
        .unwrap();
        orders.install().await.unwrap();
        orders
            .connect()
            .await
            .unwrap()
            .batch_execute("CREATE TABLE orders (id uuid PRIMARY KEY)")
            .await
            .unwrap();
        let journal = Arc::new(
            PostgresJournal::open(&pg_dsn, Some(&schema), "dat_journal")
                .await
                .unwrap(),
        );
        Some(Self {
            _turn: turn,
            lake,
            pg_dsn,
            schema,
            admin,
            orders,
            journal,
        })
    }

    fn participants(orders: &PostgresStore, lake: &IcebergStore) -> Participants {
        Participants::new()
            .with(orders.name(), orders.participant() as Arc<dyn Participant>)
            .with(lake.name(), lake.participant() as Arc<dyn Participant>)
    }

    async fn place_order(&self) -> Vec<Box<dyn Branch>> {
        let pg = self.orders.branch().await.unwrap();
        pg.client()
            .unwrap()
            .execute("INSERT INTO orders (id) VALUES ($1)", &[&new_id()])
            .await
            .unwrap();
        let mut lake = self.lake.store.branch();
        lake.append(&self.lake.table("sales"), sales(2)).unwrap();
        vec![Box::new(pg), Box::new(lake)]
    }

    async fn counts(&self) -> (i64, usize) {
        let sql = format!("SELECT count(*) FROM {}_orders.orders", self.schema);
        let orders: i64 = self.admin.query_one(&sql, &[]).await.unwrap().get(0);
        (orders, self.lake.rows("sales").await)
    }

    async fn drop(self) {
        self.admin
            .batch_execute(&format!(
                "DROP SCHEMA {0} CASCADE; DROP SCHEMA {0}_orders CASCADE",
                self.schema
            ))
            .await
            .unwrap();
        self.lake.drop().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_order_and_its_sales_commit_together() {
    let Some(erp) = Erp::new().await else {
        return;
    };
    let coordinator = Coordinator::new(
        Some(erp.journal.clone()),
        Erp::participants(&erp.orders, &erp.lake.store),
        Arc::new(HybridLogicalClock::new()),
        Arc::new(TokioTimer::default()),
    );
    let txn = new_id();
    let outcome = coordinator
        .commit(txn, &mut erp.place_order().await)
        .await
        .unwrap();
    assert!(matches!(outcome, Outcome::Committed { .. }), "{outcome:?}");
    assert_eq!(erp.counts().await, (1, 2));
    assert_eq!(
        erp.journal.get(txn).await.unwrap().unwrap().state,
        State::Done
    );
    erp.drop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn sales_left_uncommitted_by_a_crash_are_committed_by_recovery() {
    let Some(erp) = Erp::new().await else {
        return;
    };
    let txn = new_id();
    let mut branches = erp.place_order().await;
    // The protocol by hand up to the decision, then the process "dies".
    let mut records = Vec::new();
    for b in &mut branches {
        let payload = b.stage(txn, 3).await.unwrap();
        records.push(BranchRecord {
            payload,
            ..BranchRecord::new(b.store(), gid_for(txn, b.store()), b.two_phase())
        });
    }
    erp.journal.begin(txn, &records).await.unwrap();
    let gid = gid_for(txn, branches[0].store());
    branches[0].prepare(&gid).await.unwrap();
    erp.journal
        .transition(txn, State::Active, State::Prepared, Some(3))
        .await
        .unwrap();
    erp.journal
        .transition(txn, State::Prepared, State::Committed, None)
        .await
        .unwrap();
    drop(branches);
    assert_eq!(erp.counts().await, (0, 0));

    // Recovery with its own connections and its own catalog client.
    let orders = PostgresStore::new(
        &erp.pg_dsn,
        erp.orders.name(),
        Some(&format!("{}_orders", erp.schema)),
    )
    .unwrap();
    let lake = IcebergStore::connect(&dsn("DATORIUM_TEST_ICEBERG_REST_DSN").unwrap(), "lake")
        .await
        .unwrap();
    let journal = Arc::new(
        PostgresJournal::open(&erp.pg_dsn, Some(&erp.schema), "dat_journal")
            .await
            .unwrap(),
    );
    let later = datorium_core::identity::system_ms() + 60_000;
    let recovery = Recovery::new(journal, Erp::participants(&orders, &lake), 30_000)
        .unwrap()
        .with_wall(move || later);
    assert_eq!(recovery.run_once(100).await.unwrap().finished, [txn]);
    assert_eq!(erp.counts().await, (1, 2));
    assert_eq!(erp.lake.snapshots("sales").await, [Some(txn.to_string())]);
    assert_eq!(
        recovery.run_once(100).await.unwrap().finished,
        Vec::<Uuid>::new()
    );
    assert_eq!(erp.counts().await, (1, 2));
    erp.drop().await;
}

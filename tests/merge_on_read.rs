use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use datafusion_distributed::test_utils::in_memory_channel_resolver::{
    InMemoryChannelResolver, InMemoryWorkerResolver,
};
use datafusion_distributed::{DistributedExt, SessionStateBuilderExt, WorkerQueryContext};
use datafusion_distributed_iceberg::{
    IcebergCatalogTableProvider, IcebergExt, IcebergIntegrationOptions, iceberg,
};
use futures::TryStreamExt;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, ManifestListWriter, ManifestWriterBuilder,
    NestedField, PrimitiveType, Schema, Struct, Type,
};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation};
use parquet::arrow::ArrowWriter;

#[tokio::test]
async fn position_delete_manifest_is_applied_before_query_filters()
-> Result<(), Box<dyn std::error::Error>> {
    let warehouse = tempfile::tempdir()?;
    let catalog = Arc::new(
        MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "memory",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    warehouse.path().display().to_string(),
                )]),
            )
            .await?,
    );
    let namespace = NamespaceIdent::new("tests".to_string());
    catalog.create_namespace(&namespace, HashMap::new()).await?;
    let schema = Schema::builder()
        .with_fields([
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()?;
    catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name("target".to_string())
                .schema(schema)
                .build(),
        )
        .await?;
    let provider = IcebergCatalogTableProvider::try_new(
        Arc::clone(&catalog) as Arc<dyn Catalog>,
        namespace.clone(),
        "target",
        iceberg::Runtime::current(),
    )
    .await?;
    let ctx = SessionContext::new();
    ctx.register_table("target", Arc::new(provider))?;
    ctx.sql("INSERT INTO target VALUES (1, 'one'), (2, 'two'), (3, 'three')")
        .await?
        .collect()
        .await?;

    let ident = iceberg::TableIdent::new(namespace, "target".to_string());
    let table = catalog.load_table(&ident).await?;
    let task = table
        .scan()
        .select_all()
        .build()?
        .plan_files()
        .await?
        .try_collect::<Vec<_>>()
        .await?
        .pop()
        .expect("insert created one data file");

    let delete_path = format!(
        "{}/data/position-delete.parquet",
        table.metadata().location()
    );
    let delete_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("file_path", DataType::Utf8, false),
        Field::new("pos", DataType::Int64, false),
    ]));
    let delete_batch = RecordBatch::try_new(
        Arc::clone(&delete_schema),
        vec![
            Arc::new(StringArray::from(vec![task.data_file_path.clone()])),
            Arc::new(Int64Array::from(vec![1_i64])),
        ],
    )?;
    let mut parquet = ArrowWriter::try_new(File::create(&delete_path)?, delete_schema, None)?;
    parquet.write(&delete_batch)?;
    parquet.close()?;
    let delete_size = std::fs::metadata(&delete_path)?.len();

    let snapshot = table
        .metadata()
        .current_snapshot()
        .expect("insert snapshot");
    let existing = table.manifest_list_reader(snapshot).load().await?;
    let manifest_path = format!(
        "{}/metadata/position-delete-manifest.avro",
        table.metadata().location()
    );
    let mut manifest_writer = ManifestWriterBuilder::new(
        table.file_io().new_output(&manifest_path)?,
        Some(snapshot.snapshot_id()),
        Arc::clone(table.metadata().current_schema()),
        table.metadata().default_partition_spec().as_ref().clone(),
    )
    .build_v2_deletes();
    manifest_writer.add_file(
        DataFileBuilder::default()
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .content(DataContentType::PositionDeletes)
            .file_path(delete_path)
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(delete_size)
            .record_count(1)
            .partition(Struct::empty())
            .build()?,
        snapshot.sequence_number(),
    )?;
    let delete_manifest = manifest_writer.write_manifest_file().await?;

    let output = table
        .file_io()
        .new_output(snapshot.manifest_list())?
        .writer()
        .await?;
    let mut list_writer = ManifestListWriter::v2(
        output,
        snapshot.snapshot_id(),
        snapshot.parent_snapshot_id(),
        snapshot.sequence_number(),
    );
    list_writer.add_manifests(existing.entries().iter().cloned().chain([delete_manifest]))?;
    list_writer.close().await?;

    let rows = ctx
        .sql("SELECT id, name FROM target WHERE id >= 2 ORDER BY id")
        .await?
        .collect()
        .await?;
    assert_eq!(
        pretty_format_batches(&rows)?.to_string(),
        "+----+-------+\n| id | name  |\n+----+-------+\n| 3  | three |\n+----+-------+"
    );

    let worker_builder = |ctx: WorkerQueryContext| async move {
        Ok(ctx
            .builder
            .with_iceberg_integration(IcebergIntegrationOptions::default())
            .build())
    };
    let channel = InMemoryChannelResolver::from_session_builder(worker_builder);
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_iceberg_integration(IcebergIntegrationOptions::default())
        .with_distributed_planner()
        .with_distributed_worker_resolver(InMemoryWorkerResolver::new(2))
        .with_distributed_channel_resolver(channel)
        .with_distributed_file_scan_config_bytes_per_partition(1)?
        .build();
    let distributed = SessionContext::new_with_state(state);
    let provider = IcebergCatalogTableProvider::try_new(
        catalog as Arc<dyn Catalog>,
        ident.namespace.clone(),
        ident.name.clone(),
        iceberg::Runtime::current(),
    )
    .await?;
    distributed.register_table("target", Arc::new(provider))?;
    let dataframe = distributed
        .sql("SELECT id, name FROM target WHERE id >= 2 ORDER BY id")
        .await?;
    let plan = dataframe.create_physical_plan().await?;
    let plan_text = datafusion::physical_plan::displayable(plan.as_ref())
        .indent(true)
        .to_string();
    assert!(
        plan_text.contains("Network"),
        "expected distributed merge-on-read:\n{plan_text}"
    );
    let rows = dataframe.collect().await?;
    assert_eq!(
        pretty_format_batches(&rows)?.to_string(),
        "+----+-------+\n| id | name  |\n+----+-------+\n| 3  | three |\n+----+-------+"
    );
    Ok(())
}

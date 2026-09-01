use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::union::InterleaveExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::{SessionConfig, SessionContext};
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
    DataContentType, DataFileBuilder, DataFileFormat, Literal, ManifestListWriter,
    ManifestWriterBuilder, NestedField, PrimitiveType, Schema, Transform, Type,
    UnboundPartitionSpec,
};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation};
use parquet::arrow::ArrowWriter;

#[tokio::test]
async fn interleave_preserves_ranges_for_mixed_clean_and_dirty_files()
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
            NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()?;
    let spec = UnboundPartitionSpec::builder()
        .add_partition_field(1, "id".to_string(), Transform::Identity)?
        .build();
    catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name("target".to_string())
                .schema(schema)
                .partition_spec(spec)
                .build(),
        )
        .await?;

    let ident = iceberg::TableIdent::new(namespace.clone(), "target".to_string());
    let provider = IcebergCatalogTableProvider::try_new(
        Arc::clone(&catalog) as Arc<dyn Catalog>,
        namespace.clone(),
        "target",
        iceberg::Runtime::current(),
    )
    .await?;
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
    ctx.register_table("target", Arc::new(provider))?;
    ctx.sql(
        "INSERT INTO target VALUES \
         ('one', 1), ('two-delete', 2), ('two-keep', 2), ('three', 3), ('four', 4)",
    )
    .await?
    .collect()
    .await?;

    attach_position_delete(&catalog, &ident, 2, 0).await?;

    let plan = ctx
        .sql("SELECT name, id FROM target")
        .await?
        .create_physical_plan()
        .await?;
    let interleave = find_interleave(&plan).expect("mixed scan contains InterleaveExec");
    assert_eq!(interleave.inputs().len(), 2, "clean plus dirty branches");
    let Partitioning::Range(range) = interleave.properties().output_partitioning() else {
        panic!(
            "expected range-preserving InterleaveExec, got {}",
            interleave.properties().output_partitioning()
        );
    };
    assert_eq!(range.partition_count(), 4);
    assert_eq!(range.ordering()[0].to_string(), "id@1 ASC NULLS LAST");
    assert_eq!(
        range
            .split_points()
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        ["(2)", "(3)", "(4)"]
    );
    assert!(
        interleave
            .inputs()
            .iter()
            .all(|input| input.output_partitioning()
                == interleave.properties().output_partitioning())
    );
    assert_eq!(
        interleave
            .inputs()
            .iter()
            .filter(|input| contains_node(input, "IcebergDeleteApplyExec"))
            .count(),
        1,
        "one range-aligned dirty branch"
    );

    let expected_by_partition = [
        vec![("one".to_string(), 1)],
        vec![("two-keep".to_string(), 2)],
        vec![("three".to_string(), 3)],
        vec![("four".to_string(), 4)],
    ];
    for (partition, expected) in expected_by_partition.into_iter().enumerate() {
        let batches = interleave
            .execute(partition, ctx.task_ctx())?
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(rows(&batches), expected, "range partition {partition}");
    }

    let local_rows = ctx
        .sql("SELECT name, id FROM target")
        .await?
        .collect()
        .await?;
    assert_eq!(
        sorted_rows(&local_rows),
        [
            ("four".to_string(), 4),
            ("one".to_string(), 1),
            ("three".to_string(), 3),
            ("two-keep".to_string(), 2),
        ]
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
        .with_config(SessionConfig::new().with_target_partitions(4))
        .with_iceberg_integration(IcebergIntegrationOptions::default())
        .with_distributed_planner()
        .with_distributed_worker_resolver(InMemoryWorkerResolver::new(4))
        .with_distributed_channel_resolver(channel)
        .with_distributed_file_scan_config_bytes_per_partition(1)?
        .build();
    let distributed = SessionContext::new_with_state(state);
    let provider = IcebergCatalogTableProvider::try_new(
        catalog as Arc<dyn Catalog>,
        namespace,
        "target",
        iceberg::Runtime::current(),
    )
    .await?;
    distributed.register_table("target", Arc::new(provider))?;
    let dataframe = distributed.sql("SELECT name, id FROM target").await?;
    let distributed_plan = dataframe.create_physical_plan().await?;
    let plan_text = datafusion::physical_plan::displayable(distributed_plan.as_ref())
        .indent(true)
        .to_string();
    assert!(plan_text.contains("Network"), "{plan_text}");
    assert!(plan_text.contains("IcebergRangeStageExec"), "{plan_text}");
    assert!(plan_text.contains("InterleaveExec"), "{plan_text}");
    assert!(plan_text.contains("IcebergDeleteApplyExec"), "{plan_text}");
    assert_eq!(
        sorted_rows(&dataframe.collect().await?),
        [
            ("four".to_string(), 4),
            ("one".to_string(), 1),
            ("three".to_string(), 3),
            ("two-keep".to_string(), 2),
        ]
    );
    Ok(())
}

async fn attach_position_delete(
    catalog: &Arc<iceberg::memory::MemoryCatalog>,
    ident: &iceberg::TableIdent,
    partition_value: i64,
    position: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    let table = catalog.load_table(ident).await?;
    let tasks = table
        .scan()
        .select_all()
        .build()?
        .plan_files()
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    let task = tasks
        .iter()
        .find(|task| {
            task.partition.as_ref().is_some_and(|partition| {
                partition.iter().next() == Some(Some(&Literal::long(partition_value)))
            })
        })
        .expect("data file for dirty partition");

    let delete_path = format!(
        "{}/data/position-delete-{partition_value}.parquet",
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
            Arc::new(Int64Array::from(vec![position])),
        ],
    )?;
    let mut writer = ArrowWriter::try_new(File::create(&delete_path)?, delete_schema, None)?;
    writer.write(&delete_batch)?;
    writer.close()?;

    let snapshot = table
        .metadata()
        .current_snapshot()
        .expect("insert snapshot");
    let existing = table.manifest_list_reader(snapshot).load().await?;
    let manifest_path = format!(
        "{}/metadata/position-delete-{partition_value}.avro",
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
            .file_path(delete_path.clone())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(std::fs::metadata(&delete_path)?.len())
            .record_count(1)
            .partition(task.partition.clone().expect("partitioned data task"))
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
    Ok(())
}

fn find_interleave(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<InterleaveExec>> {
    if let Some(interleave) = plan.downcast_ref::<InterleaveExec>() {
        return Some(Arc::new(interleave.clone()));
    }
    plan.children().into_iter().find_map(find_interleave)
}

fn contains_node(plan: &Arc<dyn ExecutionPlan>, name: &str) -> bool {
    plan.name() == name
        || plan
            .children()
            .into_iter()
            .any(|child| contains_node(child, name))
}

fn rows(batches: &[RecordBatch]) -> Vec<(String, i64)> {
    let mut rows = Vec::new();
    for batch in batches {
        let names = batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for index in 0..batch.num_rows() {
            rows.push((names.value(index).to_string(), ids.value(index)));
        }
    }
    rows
}

fn sorted_rows(batches: &[RecordBatch]) -> Vec<(String, i64)> {
    let mut rows = rows(batches);
    rows.sort();
    rows
}

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::SessionContext;
use datafusion_distributed_iceberg::{IcebergCatalogTableProvider, iceberg};
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Transform, Type, UnboundPartitionSpec};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation};

#[tokio::test]
async fn insert_appends_parquet_and_commits_a_readable_snapshot()
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
        catalog,
        namespace,
        "target",
        iceberg::Runtime::current(),
    )
    .await?;
    let ctx = SessionContext::new();
    ctx.register_table("target", Arc::new(provider))?;
    let inserted = ctx
        .sql("INSERT INTO target VALUES (1, 'one'), (2, 'two'), (3, 'three')")
        .await?
        .collect()
        .await?;
    assert_eq!(
        pretty_format_batches(&inserted)?.to_string(),
        "+-------+\n| count |\n+-------+\n| 3     |\n+-------+"
    );

    let rows = ctx
        .sql("SELECT id, name FROM target ORDER BY id")
        .await?
        .collect()
        .await?;
    assert_eq!(
        pretty_format_batches(&rows)?.to_string(),
        "+----+-------+\n| id | name  |\n+----+-------+\n| 1  | one   |\n| 2  | two   |\n| 3  | three |\n+----+-------+"
    );
    Ok(())
}

#[tokio::test]
async fn insert_fans_out_rows_by_iceberg_partition() -> Result<(), Box<dyn std::error::Error>> {
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
                .name("partitioned".to_string())
                .schema(schema)
                .partition_spec(spec)
                .build(),
        )
        .await?;

    let provider = IcebergCatalogTableProvider::try_new(
        Arc::clone(&catalog) as Arc<dyn Catalog>,
        namespace.clone(),
        "partitioned",
        iceberg::Runtime::current(),
    )
    .await?;
    let ctx = SessionContext::new();
    ctx.register_table("partitioned", Arc::new(provider))?;
    ctx.sql("INSERT INTO partitioned VALUES ('one-a', 1), ('two', 2), ('one-b', 1)")
        .await?
        .collect()
        .await?;

    let rows = ctx
        .sql("SELECT id, name FROM partitioned ORDER BY id, name")
        .await?
        .collect()
        .await?;
    assert_eq!(
        pretty_format_batches(&rows)?.to_string(),
        "+----+-------+\n| id | name  |\n+----+-------+\n| 1  | one-a |\n| 1  | one-b |\n| 2  | two   |\n+----+-------+"
    );

    let data_root = warehouse.path().join("tests/partitioned/data");
    let directories = std::fs::read_dir(data_root)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().is_dir())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(
        directories,
        std::collections::HashSet::from(["id=1".into(), "id=2".into()])
    );
    Ok(())
}

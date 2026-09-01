use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::execution::{SessionStateBuilder, runtime_env::RuntimeEnv};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_distributed::test_utils::in_memory_channel_resolver::{
    InMemoryChannelResolver, InMemoryWorkerResolver,
};
use datafusion_distributed::{DistributedExt, SessionStateBuilderExt, WorkerQueryContext};
use datafusion_distributed_iceberg::test_utils::FixtureStorageFactory;
use datafusion_distributed_iceberg::{IcebergExt, IcebergIntegrationOptions, iceberg};
use object_store::local::LocalFileSystem;
use url::Url;

#[tokio::test]
async fn native_iceberg_parquet_leaves_execute_on_dfd_workers()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("testdata/iceberg");
    let worker_root = fixture_root.clone();
    let worker_builder = move |ctx: WorkerQueryContext| {
        let root = worker_root.clone();
        async move {
            let runtime = RuntimeEnv::default();
            runtime.register_object_store(
                &Url::parse("s3://iceberg-test").unwrap(),
                Arc::new(LocalFileSystem::new_with_prefix(root).unwrap()),
            );
            Ok(ctx.builder.with_runtime_env(Arc::new(runtime)).build())
        }
    };
    let channel = InMemoryChannelResolver::from_session_builder(worker_builder);
    let runtime = RuntimeEnv::default();
    runtime.register_object_store(
        &Url::parse("s3://iceberg-test")?,
        Arc::new(LocalFileSystem::new_with_prefix(&fixture_root)?),
    );
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_config(SessionConfig::new().with_target_partitions(4))
        .with_runtime_env(Arc::new(runtime))
        .with_iceberg_integration(IcebergIntegrationOptions {
            storage_factory: Arc::new(FixtureStorageFactory::default()),
            iceberg_runtime: iceberg::Runtime::current(),
        })
        .with_distributed_planner()
        .with_distributed_worker_resolver(InMemoryWorkerResolver::new(4))
        .with_distributed_channel_resolver(channel)
        .with_distributed_file_scan_config_bytes_per_partition(1)?
        .build();
    let ctx = SessionContext::new_with_state(state);
    ctx.sql(
        "CREATE EXTERNAL TABLE taxi STORED AS ICEBERG \
         LOCATION 's3://iceberg-test/warehouse/taxi/metadata/v1.metadata.json'",
    )
    .await?
    .collect()
    .await?;

    let dataframe = ctx
        .sql("SELECT pickup_date, COUNT(*) AS trips FROM taxi GROUP BY pickup_date ORDER BY pickup_date")
        .await?;
    let plan = dataframe.create_physical_plan().await?;
    let plan_text = datafusion::physical_plan::displayable(plan.as_ref())
        .indent(true)
        .to_string();
    assert!(
        plan_text.contains("Network"),
        "expected a distributed plan:\n{plan_text}"
    );
    assert!(
        plan_text.contains("DataSourceExec"),
        "expected native Parquet leaves:\n{plan_text}"
    );

    let batches = dataframe.collect().await?;
    let output = pretty_format_batches(&batches)?.to_string();
    assert!(output.contains("2024-01-08"));
    assert!(output.contains("2024-01-14"));
    Ok(())
}

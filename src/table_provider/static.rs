use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{Result, plan_datafusion_err};
use datafusion::datasource::TableType;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;

use crate::common::df_err;
use crate::data_source::{NativeScanOptions, plan_native_scan};
use crate::schema::{iceberg_schema_to_arrow, reorder_identity_partitions_last};

/// Static, read-only provider for a table or a specific snapshot.
#[derive(Debug, Clone)]
pub struct IcebergStaticTableProvider {
    table: iceberg::table::Table,
    snapshot_id: Option<i64>,
    schema: SchemaRef,
    _iceberg_runtime: iceberg::Runtime,
}

impl IcebergStaticTableProvider {
    /// Creates a provider that reads the provided table snapshot, or the current snapshot
    /// if none provided.
    pub fn try_new(
        table: iceberg::table::Table,
        snapshot_id: Option<i64>,
        iceberg_runtime: iceberg::Runtime,
    ) -> Result<Self> {
        let table_schema = if let Some(snapshot_id) = snapshot_id {
            let snapshot = table
                .metadata()
                .snapshot_by_id(snapshot_id)
                .ok_or_else(|| {
                    plan_datafusion_err!(
                        "snapshot id {snapshot_id} not found in table {}",
                        table.identifier().name()
                    )
                })?;
            snapshot.schema(table.metadata()).map_err(df_err)?
        } else {
            Arc::clone(table.metadata().current_schema())
        };

        let arrow_schema = iceberg_schema_to_arrow(&table_schema)?;
        let arrow_schema = reorder_identity_partitions_last(
            &arrow_schema,
            &table_schema,
            table.metadata().default_partition_spec(),
        );
        Ok(Self {
            table,
            snapshot_id,
            schema: Arc::new(arrow_schema),
            _iceberg_runtime: iceberg_runtime,
        })
    }
}

#[async_trait]
impl TableProvider for IcebergStaticTableProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan_native_scan(
            &self.table,
            self.schema.clone(),
            NativeScanOptions {
                session: state,
                snapshot_id: self.snapshot_id,
                projection,
                filters,
                limit,
                target_partitions: state.config().target_partitions(),
                parquet_options: datafusion::config::TableParquetOptions {
                    global: state.config().options().execution.parquet.clone(),
                    ..Default::default()
                },
            },
        )
        .await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}

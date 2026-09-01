use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::SchemaExt;
use datafusion::datasource::sink::DataSinkExec;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use iceberg::{Catalog, NamespaceIdent, TableIdent};

use crate::common::df_err;
use crate::data_source::{NativeScanOptions, plan_native_scan};
use crate::schema::{iceberg_schema_to_arrow, reorder_identity_partitions_last};

/// Catalog-backed, read-only table provider with automatic metadata refresh.
///
/// The provider loads fresh table metadata from the catalog on every scan. For
/// a fixed snapshot, use IcebergStaticTableProvider instead.
#[derive(Debug, Clone)]
pub struct IcebergCatalogTableProvider {
    catalog: Arc<dyn Catalog>,
    table_ident: TableIdent,
    schema: SchemaRef,
    _iceberg_runtime: iceberg::Runtime,
}

impl IcebergCatalogTableProvider {
    /// Creates a read-only catalog-backed provider.
    pub async fn try_new(
        catalog: Arc<dyn Catalog>,
        namespace: NamespaceIdent,
        name: impl Into<String>,
        iceberg_runtime: iceberg::Runtime,
    ) -> Result<Self> {
        let table_ident = TableIdent::new(namespace, name.into());
        let table = catalog.load_table(&table_ident).await.map_err(df_err)?;
        let current_schema = table.metadata().current_schema();
        let schema = iceberg_schema_to_arrow(current_schema)?;
        let schema = reorder_identity_partitions_last(
            &schema,
            current_schema,
            table.metadata().default_partition_spec(),
        );

        Ok(Self {
            catalog,
            table_ident,
            schema: Arc::new(schema),
            _iceberg_runtime: iceberg_runtime,
        })
    }
}

#[async_trait]
impl TableProvider for IcebergCatalogTableProvider {
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
        let table = self
            .catalog
            .load_table(&self.table_ident)
            .await
            .map_err(df_err)?;

        plan_native_scan(
            &table,
            self.schema.clone(),
            NativeScanOptions {
                session: state,
                snapshot_id: None,
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

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.schema
            .logically_equivalent_names_and_types(&input.schema())?;
        if insert_op != InsertOp::Append {
            return Err(datafusion::error::DataFusionError::NotImplemented(format!(
                "Iceberg {insert_op} is not implemented; INSERT append is supported"
            )));
        }
        let sink = crate::insert::IcebergInsertSink::new(
            Arc::clone(&self.catalog),
            self.table_ident.clone(),
            Arc::clone(&self.schema),
        );
        Ok(Arc::new(DataSinkExec::new(input, Arc::new(sink), None)))
    }
}

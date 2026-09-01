// Licensed under the Apache License, Version 2.0.
//
// Portions derived from LakeSail:
// https://github.com/lakehq/sail
// Original files: crates/sail-iceberg/src/physical_plan/writer_exec.rs and
//                 crates/sail-iceberg/src/operations/write/
// Source revision: 1a449ec44fabc53ee38f19661b5dbe47e45aa676
//
// Modified to use iceberg-rust's native writers and transaction API.

use std::fmt;
use std::io::Cursor;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::sink::DataSink;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, SendableRecordBatchStream};
use futures::TryStreamExt;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::partitioning::PartitioningWriter;
use iceberg::writer::partitioning::fanout_writer::FanoutWriter;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, TableIdent};
use parquet_58::file::properties::WriterProperties;

/// DataFusion sink that writes Parquet data files and atomically appends them
/// to a freshly loaded Iceberg table.
#[derive(Debug)]
pub(crate) struct IcebergInsertSink {
    catalog: Arc<dyn Catalog>,
    table_ident: TableIdent,
    schema: SchemaRef,
}

impl IcebergInsertSink {
    pub(crate) fn new(
        catalog: Arc<dyn Catalog>,
        table_ident: TableIdent,
        schema: SchemaRef,
    ) -> Self {
        Self {
            catalog,
            table_ident,
            schema,
        }
    }
}

impl DisplayAs for IcebergInsertSink {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IcebergInsertSink: table={}", self.table_ident)
    }
}

#[async_trait]
impl DataSink for IcebergInsertSink {
    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let table = self
            .catalog
            .load_table(&self.table_ident)
            .await
            .map_err(df_err)?;
        let location = DefaultLocationGenerator::new(table.metadata()).map_err(df_err)?;
        let file_name = DefaultFileNameGenerator::new(
            format!("datafusion-{}", uuid::Uuid::now_v7()),
            None,
            iceberg::spec::DataFileFormat::Parquet,
        );
        let parquet = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            Arc::clone(table.metadata().current_schema()),
        );
        let rolling = RollingFileWriterBuilder::new_with_default_file_size(
            parquet,
            table.file_io().clone(),
            location,
            file_name,
        );
        let builder = DataFileWriterBuilder::new(rolling);
        let spec = Arc::clone(table.metadata().default_partition_spec());
        let schema = Arc::clone(table.metadata().current_schema());
        let mut row_count = 0_u64;

        let files = if spec.fields().is_empty() {
            let mut writer = builder.build(None).await.map_err(df_err)?;
            while let Some(batch) = data.try_next().await? {
                row_count = row_count.saturating_add(batch.num_rows() as u64);
                writer
                    .write(to_iceberg_arrow(&batch, &self.schema)?)
                    .await
                    .map_err(df_err)?;
            }
            writer.close().await.map_err(df_err)?
        } else {
            let splitter =
                iceberg::arrow::RecordBatchPartitionSplitter::try_new_with_computed_values(
                    Arc::clone(&schema),
                    Arc::clone(&spec),
                )
                .map_err(df_err)?;
            let mut writer = FanoutWriter::new(builder);
            while let Some(batch) = data.try_next().await? {
                row_count = row_count.saturating_add(batch.num_rows() as u64);
                let batch = to_iceberg_arrow(&batch, &self.schema)?;
                for (key, partition_batch) in splitter.split(&batch).map_err(df_err)? {
                    writer.write(key, partition_batch).await.map_err(df_err)?;
                }
            }
            writer.close().await.map_err(df_err)?
        };

        if files.is_empty() {
            return Ok(0);
        }
        let transaction = Transaction::new(&table);
        let action = transaction.fast_append().add_data_files(files);
        let transaction = action.apply(transaction).map_err(df_err)?;
        transaction
            .commit(self.catalog.as_ref())
            .await
            .map_err(df_err)?;
        Ok(row_count)
    }
}

/// iceberg-rust 0.10 uses Arrow 58 while DataFusion 55 uses Arrow 59. IPC is
/// the stable, schema-preserving compatibility boundary until those releases
/// converge on one Arrow version.
fn to_iceberg_arrow(
    batch: &datafusion::arrow::record_batch::RecordBatch,
    iceberg_schema: &SchemaRef,
) -> Result<arrow_array_58::RecordBatch> {
    let batch = datafusion::arrow::record_batch::RecordBatch::try_new(
        Arc::clone(iceberg_schema),
        batch.columns().to_vec(),
    )
    .map_err(external_error)?;
    let mut bytes = Vec::new();
    {
        let mut writer = arrow_ipc_59::writer::StreamWriter::try_new(&mut bytes, &batch.schema())
            .map_err(external_error)?;
        writer.write(&batch).map_err(external_error)?;
        writer.finish().map_err(external_error)?;
    }
    let mut reader = arrow_ipc_58::reader::StreamReader::try_new(Cursor::new(bytes), None)
        .map_err(external_error)?;
    reader
        .next()
        .transpose()
        .map_err(external_error)?
        .ok_or_else(|| DataFusionError::Internal("Arrow IPC conversion produced no batch".into()))
}

fn df_err(error: iceberg::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

fn external_error(error: impl std::error::Error + Send + Sync + 'static) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

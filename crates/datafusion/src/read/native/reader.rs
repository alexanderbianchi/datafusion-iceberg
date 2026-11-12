// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::ops::Range;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{Result, exec_err};
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{
    FileScanConfigBuilder, FileSource, ParquetFileMetrics, ParquetFileReaderFactory,
    ParquetSource,
};
use datafusion::datasource::source::DataSource;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_datasource::PartitionedFile;
use futures::TryStreamExt;
use futures::future::BoxFuture;
use iceberg::arrow::delete_file_loader::DeleteFileLoader;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::io::{FileIO, FileRead};
use iceberg::metadata_columns::{
    RESERVED_FIELD_ID_DELETE_FILE_PATH, RESERVED_FIELD_ID_DELETE_FILE_POS,
};
use iceberg::scan::{ArrowRecordBatchStream, FileScanTask, FileScanTaskDeleteFile};
use iceberg::spec::{
    DataContentType, DataFileFormat, NestedField, PrimitiveType, Schema,
    SchemaRef as IcebergSchemaRef, Type,
};
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use tokio::sync::OnceCell;

use super::expr::IcebergExprAdapterFactory;
use crate::{from_datafusion_error, to_datafusion_error};

#[derive(Debug)]
pub(super) struct IcebergReaderFactory {
    pub file_io: FileIO,
}

impl ParquetFileReaderFactory for IcebergReaderFactory {
    fn create_reader(
        &self,
        partition: usize,
        file: PartitionedFile,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn AsyncFileReader + Send>> {
        let task = file.extensions.get::<Arc<FileScanTask>>().ok_or_else(|| {
            datafusion::common::exec_datafusion_err!("Missing Iceberg file task")
        })?;
        Ok(Box::new(IcebergFileReader {
            file_io: self.file_io.clone(),
            path: task.data_file_path().to_owned(),
            size: task.file_size_in_bytes(),
            metadata_size_hint,
            reader: OnceCell::new(),
            metrics: ParquetFileMetrics::new(partition, task.data_file_path(), metrics),
        }))
    }
}

struct IcebergFileReader {
    file_io: FileIO,
    path: String,
    size: u64,
    metadata_size_hint: Option<usize>,
    reader: OnceCell<Box<dyn FileRead>>,
    metrics: ParquetFileMetrics,
}

impl AsyncFileReader for IcebergFileReader {
    fn get_bytes(
        &mut self,
        range: Range<u64>,
    ) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        Box::pin(async move {
            let reader = self
                .reader
                .get_or_try_init(|| async {
                    self.file_io.new_input(&self.path)?.reader().await
                })
                .await
                .map_err(|e| parquet::errors::ParquetError::External(Box::new(e)))?;
            let bytes = reader
                .read(range)
                .await
                .map_err(|e| parquet::errors::ParquetError::External(Box::new(e)))?;
            self.metrics.bytes_scanned.add(bytes.len());
            Ok(bytes)
        })
    }
    fn get_metadata(
        &mut self,
        options: Option<&ArrowReaderOptions>,
    ) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        let reader = ParquetMetaDataReader::new()
            .with_prefetch_hint(self.metadata_size_hint)
            .with_metadata_options(options.map(|o| o.metadata_options().clone()))
            .with_decryption_properties(
                options
                    .and_then(|o| o.file_decryption_properties())
                    .cloned(),
            );
        let size = self.size;
        Box::pin(async move { Ok(Arc::new(reader.load_and_finish(self, size).await?)) })
    }
}

pub(super) fn partitioned_file(task: Arc<FileScanTask>) -> Result<PartitionedFile> {
    if task.predicate().is_some() {
        return exec_err!(
            "Native task residuals must be enforced by the caller before using without_predicate"
        );
    }
    if task.data_file_format() != DataFileFormat::Parquet {
        return exec_err!("Native Iceberg scan requires Parquet data files");
    }
    // Iceberg byte ranges use row-group midpoints and remain in the task.
    // PartitionedFile ranges are reserved for DataFusion's own repartitioning.
    Ok(
        PartitionedFile::new(task.data_file_path(), task.file_size_in_bytes())
            .with_extension(task),
    )
}

pub(super) fn with_task_decryption(
    source: ParquetSource,
    task: &FileScanTask,
) -> Result<ParquetSource> {
    let Some(bytes) = task.key_metadata() else {
        return Ok(source);
    };
    let metadata = iceberg::encryption::StandardKeyMetadata::decode(bytes)
        .map_err(to_datafusion_error)?;
    let mut properties = parquet::encryption::decrypt::FileDecryptionProperties::builder(
        metadata.encryption_key().as_bytes().to_vec(),
    );
    if let Some(aad) = metadata.aad_prefix() {
        properties = properties.with_aad_prefix(aad.to_vec());
    }
    let properties = properties.build()?;
    let mut options = source.table_parquet_options().clone();
    options.crypto.file_decryption = Some((&properties).try_into()?);
    Ok(source.with_table_parquet_options(options))
}

pub(super) async fn preload_metadata(
    source: ParquetSource,
    file: &PartitionedFile,
    partition: usize,
) -> Result<(ParquetSource, Arc<ParquetMetaData>)> {
    let factory = source.parquet_file_reader_factory().ok_or_else(|| {
        datafusion::common::exec_datafusion_err!("Missing native reader factory")
    })?;
    let mut reader = factory.create_reader(
        partition,
        file.clone(),
        file.metadata_size_hint,
        source.metrics(),
    )?;
    let mut options = ArrowReaderOptions::new();
    if let Some(properties) = source
        .table_parquet_options()
        .crypto
        .file_decryption
        .clone()
    {
        options =
            options.with_file_decryption_properties(Arc::new(properties.try_into()?));
    }
    let metadata = reader.get_metadata(Some(&options)).await?;
    let factory = PreloadedReaderFactory(Mutex::new(Some(Box::new(PreloadedReader {
        inner: reader,
        metadata: metadata.clone(),
    }))));
    Ok((
        source.with_parquet_file_reader_factory(Arc::new(factory)),
        metadata,
    ))
}

struct PreloadedReaderFactory(Mutex<Option<Box<dyn AsyncFileReader + Send>>>);
impl std::fmt::Debug for PreloadedReaderFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreloadedReaderFactory")
            .finish_non_exhaustive()
    }
}
impl ParquetFileReaderFactory for PreloadedReaderFactory {
    fn create_reader(
        &self,
        _: usize,
        _: PartitionedFile,
        _: Option<usize>,
        _: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn AsyncFileReader + Send>> {
        self.0
            .lock()
            .map_err(|_| {
                datafusion::common::exec_datafusion_err!("Preloaded reader lock poisoned")
            })?
            .take()
            .ok_or_else(|| {
                datafusion::common::exec_datafusion_err!(
                    "Preloaded reader has already been consumed"
                )
            })
    }
}
struct PreloadedReader {
    inner: Box<dyn AsyncFileReader + Send>,
    metadata: Arc<ParquetMetaData>,
}
impl AsyncFileReader for PreloadedReader {
    fn get_bytes(
        &mut self,
        range: Range<u64>,
    ) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        self.inner.get_bytes(range)
    }
    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        self.inner.get_byte_ranges(ranges)
    }
    fn get_metadata(
        &mut self,
        _: Option<&ArrowReaderOptions>,
    ) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        let metadata = self.metadata.clone();
        Box::pin(async move { Ok(metadata) })
    }
}

#[derive(Debug)]
pub(super) struct NativeDeleteFileLoader {
    pub context: Arc<TaskContext>,
    pub source: ParquetSource,
}

#[async_trait]
impl DeleteFileLoader for NativeDeleteFileLoader {
    async fn read_delete_file(
        &self,
        delete: &FileScanTaskDeleteFile,
        schema: IcebergSchemaRef,
    ) -> iceberg::Result<ArrowRecordBatchStream> {
        self.read(delete, schema).map_err(from_datafusion_error)
    }
}

impl NativeDeleteFileLoader {
    fn read(
        &self,
        delete: &FileScanTaskDeleteFile,
        schema: IcebergSchemaRef,
    ) -> Result<ArrowRecordBatchStream> {
        let (schema, ids) = match delete.file_type() {
            DataContentType::EqualityDeletes => {
                let ids = delete.equality_ids().ok_or_else(|| {
                    datafusion::common::exec_datafusion_err!("Missing equality IDs")
                })?;
                (schema, ids.to_vec())
            }
            DataContentType::PositionDeletes => {
                let fields = vec![
                    Arc::new(NestedField::required(
                        RESERVED_FIELD_ID_DELETE_FILE_PATH,
                        "file_path",
                        Type::Primitive(PrimitiveType::String),
                    )),
                    Arc::new(NestedField::required(
                        RESERVED_FIELD_ID_DELETE_FILE_POS,
                        "pos",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                ];
                (
                    Arc::new(
                        Schema::builder()
                            .with_fields(fields)
                            .build()
                            .map_err(to_datafusion_error)?,
                    ),
                    vec![
                        RESERVED_FIELD_ID_DELETE_FILE_PATH,
                        RESERVED_FIELD_ID_DELETE_FILE_POS,
                    ],
                )
            }
            DataContentType::Data => return exec_err!("Expected an Iceberg delete file"),
        };
        let task = Arc::new(
            FileScanTask::builder()
                .with_file_size_in_bytes(delete.file_size_in_bytes())
                .with_start(0)
                .with_length(delete.file_size_in_bytes())
                .with_data_file_path(delete.file_path().to_owned())
                .with_data_file_format(delete.file_format())
                .with_schema(schema.clone())
                .with_project_field_ids(ids.clone())
                .with_case_sensitive(true)
                .with_key_metadata(delete.key_metadata().map(Box::from))
                .build()
                .map_err(to_datafusion_error)?,
        );
        let full_schema = schema_to_arrow_schema(&schema).map_err(to_datafusion_error)?;
        let indices = ids
            .iter()
            .map(|id| {
                full_schema
                    .fields()
                    .iter()
                    .position(|field| {
                        field
                            .metadata()
                            .get(parquet::arrow::PARQUET_FIELD_ID_META_KEY)
                            .and_then(|v| v.parse::<i32>().ok())
                            == Some(*id)
                    })
                    .ok_or_else(|| {
                        datafusion::common::exec_datafusion_err!(
                            "Nested equality key {id} is not supported"
                        )
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        let arrow_schema: SchemaRef = Arc::new(full_schema.project(&indices)?);
        let mut source = ParquetSource::new(arrow_schema)
            .with_table_parquet_options(self.source.table_parquet_options().clone());
        if let Some(factory) = self.source.parquet_file_reader_factory() {
            source = source.with_parquet_file_reader_factory(factory.clone());
        }
        let source = with_task_decryption(source, &task)?;
        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            Arc::new(source),
        )
        .with_file(partitioned_file(task.clone())?)
        .with_expr_adapter(Some(Arc::new(IcebergExprAdapterFactory { task })))
        .build();
        Ok(Box::pin(
            config
                .open(0, self.context.clone())?
                .map_err(from_datafusion_error),
        ))
    }
}

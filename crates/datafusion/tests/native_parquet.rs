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

use std::collections::HashMap;
use std::fs::File;
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::{
    ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::common::Result;
use datafusion::datasource::physical_plan::{
    FileScanConfigBuilder, ParquetFileReaderFactory, ParquetSource,
};
use datafusion::datasource::source::{DataSource, DataSourceExec};
use datafusion::logical_expr::Operator;
use datafusion::object_store::local::LocalFileSystem;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{
    BinaryExpr, Column, DynamicFilterPhysicalExpr, lit,
};
use datafusion::physical_plan::collect;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_datasource::PartitionedFile;
use datafusion_datasource_parquet::DefaultParquetFileReaderFactory;
use futures::{StreamExt, TryStreamExt};
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::io::FileIO;
use iceberg::metadata_columns::{
    RESERVED_FIELD_ID_DELETE_FILE_PATH, RESERVED_FIELD_ID_DELETE_FILE_POS,
};
use iceberg::scan::{FileScanTask, FileScanTaskDeleteFile};
use iceberg::spec::{
    DataContentType, DataFileFormat, Literal, NestedField, PrimitiveType, Schema,
    SchemaRef, Type,
};
use iceberg_datafusion::read::IcebergFileSource;
use iceberg_datafusion::test_utils::IcebergTestHarness;
use iceberg_datafusion::{
    IcebergPlanningConfig, IcebergStaticTableProvider, to_datafusion_error,
};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use parquet::file::properties::WriterProperties;
use tempfile::TempDir;

mod common;

fn config(pushdown: bool) -> SessionConfig {
    SessionConfig::new()
        .with_target_partitions(2)
        .with_batch_size(2)
        .with_option_extension(IcebergPlanningConfig::default())
        .set_bool("iceberg.native_parquet_reader", true)
        .set_bool("datafusion.execution.parquet.pushdown_filters", pushdown)
}

#[tokio::test]
async fn provider_native_queries_match_existing_reader() -> Result<()> {
    let native =
        IcebergTestHarness::with_config(config(true).with_batch_size(2048)).await?;
    let reference = IcebergTestHarness::new().await?;
    for sql in [
        "SELECT COUNT(*) AS n FROM taxi",
        "SELECT vendor_id, trip_distance FROM taxi WHERE pickup_date = DATE '2024-01-08' AND trip_distance > 50 ORDER BY vendor_id, trip_distance",
        "SELECT vendor_id, pickup_at FROM taxi ORDER BY pickup_at, vendor_id, pickup_location_id LIMIT 3",
        "SELECT COUNT(*) AS n FROM taxi WHERE pickup_date = DATE '2025-01-01'",
    ] {
        let (plan, actual) = native.query(sql).await?;
        let (_, expected) = reference.query(sql).await?;
        assert!(plan.contains("iceberg-parquet"), "{plan}");
        assert_eq!(actual, expected, "{sql}");
    }
    Ok(())
}

#[tokio::test]
async fn provider_applies_position_deletes_before_limit_without_query_pushdown()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let fixture = common::position_deletes::table_with_position_delete().await?;
    for pushdown in [false, true] {
        let context = SessionContext::new_with_config(config(pushdown));
        context.register_table(
            "t",
            Arc::new(
                IcebergStaticTableProvider::try_new_from_table(fixture.table.clone())
                    .await?,
            ),
        )?;
        let batches = context
            .sql("SELECT id FROM t ORDER BY id LIMIT 2")
            .await?
            .collect()
            .await?;
        assert_eq!(values(&batches), vec![1, 2]);
    }
    Ok(())
}

fn field(name: &str, data_type: DataType, id: i32) -> Field {
    Field::new(name, data_type, true).with_metadata(HashMap::from([(
        PARQUET_FIELD_ID_META_KEY.to_owned(),
        id.to_string(),
    )]))
}

fn schema() -> SchemaRef {
    Arc::new(
        Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::optional(
                    1,
                    "key",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "value",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(
                    NestedField::optional(
                        3,
                        "added",
                        Type::Primitive(PrimitiveType::Int),
                    )
                    .with_initial_default(Literal::int(7)),
                ),
            ])
            .build()
            .unwrap(),
    )
}

fn write_file(
    directory: &TempDir,
    name: &str,
    fields: Vec<Field>,
    columns: Vec<ArrayRef>,
) -> (String, u64) {
    write_file_with_properties(
        directory,
        name,
        fields,
        columns,
        WriterProperties::builder()
            .set_max_row_group_row_count(Some(2))
            .build(),
    )
}

fn write_file_with_properties(
    directory: &TempDir,
    name: &str,
    fields: Vec<Field>,
    columns: Vec<ArrayRef>,
    props: WriterProperties,
) -> (String, u64) {
    let path = directory.path().join(name);
    let schema = Arc::new(ArrowSchema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    let mut writer =
        ArrowWriter::try_new(File::create(&path).unwrap(), schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    (
        path.to_str().unwrap().to_owned(),
        std::fs::metadata(&path).unwrap().len(),
    )
}

fn task(
    path: String,
    size: u64,
    schema: SchemaRef,
    rows: Option<u64>,
    deletes: Vec<FileScanTaskDeleteFile>,
) -> FileScanTask {
    FileScanTask::builder()
        .with_data_file_path(path)
        .with_data_file_format(DataFileFormat::Parquet)
        .with_file_size_in_bytes(size)
        .with_start(0)
        .with_length(size)
        .with_record_count(rows)
        .with_schema(schema)
        .with_project_field_ids(vec![1, 2, 3])
        .with_case_sensitive(true)
        .with_deletes(deletes)
        .build()
        .unwrap()
}

fn delete_file(path: String, size: u64, equality: bool) -> FileScanTaskDeleteFile {
    FileScanTaskDeleteFile::builder()
        .with_file_path(path)
        .with_file_size_in_bytes(size)
        .with_file_type(if equality {
            DataContentType::EqualityDeletes
        } else {
            DataContentType::PositionDeletes
        })
        .with_file_format(DataFileFormat::Parquet)
        .with_partition_spec_id(0)
        .with_equality_ids(equality.then_some(vec![1]))
        .build()
        .unwrap()
}

#[derive(Debug)]
struct CountingFactory {
    inner: DefaultParquetFileReaderFactory,
    opened: Arc<Mutex<Vec<String>>>,
}
impl ParquetFileReaderFactory for CountingFactory {
    fn create_reader(
        &self,
        partition: usize,
        file: PartitionedFile,
        hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn AsyncFileReader + Send>> {
        self.opened.lock().unwrap().push(
            file.extensions
                .get::<Arc<FileScanTask>>()
                .unwrap()
                .data_file_path()
                .to_owned(),
        );
        self.inner.create_reader(partition, file, hint, metrics)
    }
}

fn values(batches: &[RecordBatch]) -> Vec<i64> {
    let mut values: Vec<_> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect();
    values.sort_unstable();
    values
}

#[tokio::test]
async fn native_reader_handles_per_file_deletes_hidden_keys_defaults_and_limits()
-> Result<()> {
    let directory = TempDir::new()?;
    let schema = schema();
    let (a, a_size) = write_file(
        &directory,
        "a.parquet",
        vec![
            field("old_key", DataType::Int32, 1),
            field("value", DataType::Int64, 2),
        ],
        vec![
            Arc::new(Int32Array::from(vec![Some(1), None, Some(3), Some(4)])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
        ],
    );
    let (b, b_size) = write_file(
        &directory,
        "b.parquet",
        vec![
            field("value", DataType::Int64, 2),
            field("key", DataType::Int64, 1),
        ],
        vec![
            Arc::new(Int64Array::from(vec![100, 200])),
            Arc::new(Int64Array::from(vec![1, 2])),
        ],
    );
    let (eq, eq_size) = write_file(
        &directory,
        "eq.parquet",
        vec![field("key", DataType::Int64, 1)],
        vec![Arc::new(Int64Array::from(vec![Some(1), None]))],
    );
    let (pos, pos_size) = write_file(
        &directory,
        "pos.parquet",
        vec![
            field(
                "file_path",
                DataType::Utf8,
                RESERVED_FIELD_ID_DELETE_FILE_PATH,
            ),
            field("pos", DataType::Int64, RESERVED_FIELD_ID_DELETE_FILE_POS),
        ],
        vec![
            Arc::new(StringArray::from(vec![a.clone()])),
            Arc::new(Int64Array::from(vec![2])),
        ],
    );
    let (empty_eq, empty_eq_size) = write_file(
        &directory,
        "empty-eq.parquet",
        vec![field("key", DataType::Int64, 1)],
        vec![Arc::new(Int64Array::from(Vec::<i64>::new()))],
    );
    let (value_eq, value_eq_size) = write_file(
        &directory,
        "value-eq.parquet",
        vec![field("value", DataType::Int64, 2)],
        vec![Arc::new(Int64Array::from(vec![100]))],
    );
    let value_delete = FileScanTaskDeleteFile::builder()
        .with_file_path(value_eq.clone())
        .with_file_size_in_bytes(value_eq_size)
        .with_file_type(DataContentType::EqualityDeletes)
        .with_file_format(DataFileFormat::Parquet)
        .with_partition_spec_id(0)
        .with_equality_ids(Some(vec![2]))
        .build()
        .unwrap();
    let tasks = vec![
        task(
            a.clone(),
            a_size,
            schema.clone(),
            Some(4),
            vec![
                delete_file(eq.clone(), eq_size, true),
                delete_file(pos.clone(), pos_size, false),
            ],
        ),
        task(
            b.clone(),
            b_size,
            schema.clone(),
            Some(2),
            vec![
                delete_file(pos.clone(), pos_size, false),
                delete_file(empty_eq.clone(), empty_eq_size, true),
                value_delete,
            ],
        ),
    ];
    for pushdown in [false, true] {
        let context = SessionContext::new_with_config(config(pushdown));
        let opened = Arc::new(Mutex::new(Vec::new()));
        let factory = Arc::new(CountingFactory {
            inner: DefaultParquetFileReaderFactory::new(Arc::new(LocalFileSystem::new())),
            opened: opened.clone(),
        });
        let arrow_schema =
            Arc::new(schema_to_arrow_schema(&schema).map_err(to_datafusion_error)?);
        let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("added", 2)),
            Operator::Eq,
            lit(7i32),
        ));
        let parquet = ParquetSource::new(arrow_schema)
            .with_pushdown_filters(pushdown)
            .with_predicate(predicate)
            .with_parquet_file_reader_factory(factory);
        let source = IcebergFileSource::new(
            parquet,
            schema.clone(),
            FileIO::new_with_fs(),
            context.task_ctx(),
        )?;
        let config = source.file_scan_config(vec![tasks.clone()])?;
        let config = FileScanConfigBuilder::from(config)
            .with_projection_indices(Some(vec![1]))?
            .with_limit(Some(2))
            .build();
        let batches =
            collect(DataSourceExec::from_data_source(config), context.task_ctx()).await?;
        assert_eq!(values(&batches), vec![40, 200]);
        let mut actual = opened.lock().unwrap().clone();
        actual.sort();
        let mut expected = vec![
            a.clone(),
            b.clone(),
            eq.clone(),
            pos.clone(),
            empty_eq.clone(),
            value_eq.clone(),
        ];
        expected.sort();
        assert_eq!(
            actual, expected,
            "both data and delete files use the configured native backend, and shared deletes load once"
        );
        let no_deletes = task(b.clone(), b_size, schema.clone(), Some(2), vec![]);
        let config =
            FileScanConfigBuilder::from(source.file_scan_config(vec![vec![no_deletes]])?)
                .with_projection_indices(Some(vec![1]))?
                .build();
        let batches =
            collect(DataSourceExec::from_data_source(config), context.task_ctx()).await?;
        assert_eq!(values(&batches), vec![100, 200]);
    }
    Ok(())
}

#[tokio::test]
async fn dynamic_filter_updates_after_opening_with_position_deletes() -> Result<()> {
    let directory = TempDir::new()?;
    let schema = schema();
    let (path, size) = write_file(
        &directory,
        "dynamic.parquet",
        vec![
            field("old_key", DataType::Int32, 1),
            field("value", DataType::Int64, 2),
        ],
        vec![
            Arc::new(Int32Array::from_iter_values(0..100)),
            Arc::new(Int64Array::from_iter_values(0..100)),
        ],
    );
    let (pos, pos_size) = write_file(
        &directory,
        "pos.parquet",
        vec![
            field(
                "file_path",
                DataType::Utf8,
                RESERVED_FIELD_ID_DELETE_FILE_PATH,
            ),
            field("pos", DataType::Int64, RESERVED_FIELD_ID_DELETE_FILE_POS),
        ],
        vec![
            Arc::new(StringArray::from(vec![path.clone()])),
            Arc::new(Int64Array::from(vec![99])),
        ],
    );
    let context = SessionContext::new_with_config(config(true));
    let dynamic = Arc::new(DynamicFilterPhysicalExpr::new(
        vec![Arc::new(Column::new("key", 0))],
        lit(true),
    ));
    let arrow_schema =
        Arc::new(schema_to_arrow_schema(&schema).map_err(to_datafusion_error)?);
    let parquet = ParquetSource::new(arrow_schema)
        .with_pushdown_filters(true)
        .with_predicate(dynamic.clone());
    let source = IcebergFileSource::new(
        parquet,
        schema.clone(),
        FileIO::new_with_fs(),
        context.task_ctx(),
    )?;
    let config = source.file_scan_config(vec![vec![task(
        path,
        size,
        schema,
        None,
        vec![delete_file(pos, pos_size, false)],
    )]])?;
    let config = FileScanConfigBuilder::from(config)
        .with_projection_indices(Some(vec![1]))?
        .build();
    let mut stream = config.open(0, context.task_ctx())?;
    let first = stream.next().await.unwrap()?;
    assert_eq!(values(&[first]), vec![0, 1]);
    dynamic.update(Arc::new(BinaryExpr::new(
        Arc::new(Column::new("key", 0)),
        Operator::GtEq,
        lit(90i64),
    )))?;
    let mut remaining = Vec::new();
    while let Some(batch) = stream.next().await {
        remaining.push(batch?);
    }
    assert_eq!(values(&remaining), (90..99).collect::<Vec<_>>());
    Ok(())
}

#[tokio::test]
async fn encrypted_data_and_equality_deletes_use_native_parquet() -> Result<()> {
    use iceberg::encryption::StandardKeyMetadata;
    use parquet::encryption::encrypt::FileEncryptionProperties;

    let directory = TempDir::new()?;
    let schema = schema();
    let key = b"0123456789012345";
    let aad = b"test-iceberg-native";
    let properties = || {
        WriterProperties::builder()
            .with_file_encryption_properties(
                FileEncryptionProperties::builder(key.to_vec())
                    .with_aad_prefix(aad.to_vec())
                    .build()
                    .unwrap(),
            )
            .build()
    };
    let (path, size) = write_file_with_properties(
        &directory,
        "encrypted.parquet",
        vec![
            field("key", DataType::Int64, 1),
            field("value", DataType::Int64, 2),
        ],
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![10, 20])),
        ],
        properties(),
    );
    let (eq, eq_size) = write_file_with_properties(
        &directory,
        "encrypted-eq.parquet",
        vec![field("key", DataType::Int64, 1)],
        vec![Arc::new(Int64Array::from(vec![2]))],
        properties(),
    );
    let metadata = StandardKeyMetadata::try_new(key)
        .map_err(to_datafusion_error)?
        .with_aad_prefix(aad)
        .encode()
        .map_err(to_datafusion_error)?;
    let delete = FileScanTaskDeleteFile::builder()
        .with_file_path(eq)
        .with_file_size_in_bytes(eq_size)
        .with_file_type(DataContentType::EqualityDeletes)
        .with_file_format(DataFileFormat::Parquet)
        .with_partition_spec_id(0)
        .with_equality_ids(Some(vec![1]))
        .with_key_metadata(Some(metadata.clone()))
        .build()
        .map_err(to_datafusion_error)?;
    let task = FileScanTask::builder()
        .with_data_file_path(path)
        .with_data_file_format(DataFileFormat::Parquet)
        .with_file_size_in_bytes(size)
        .with_start(0)
        .with_length(size)
        .with_record_count(Some(2))
        .with_schema(schema.clone())
        .with_project_field_ids(vec![2])
        .with_case_sensitive(true)
        .with_deletes(vec![delete])
        .with_key_metadata(Some(metadata))
        .build()
        .map_err(to_datafusion_error)?;
    let context = SessionContext::new();
    let source = IcebergFileSource::new(
        ParquetSource::new(Arc::new(
            schema_to_arrow_schema(&schema).map_err(to_datafusion_error)?,
        )),
        schema,
        FileIO::new_with_fs(),
        context.task_ctx(),
    )?;
    let config = FileScanConfigBuilder::from(source.file_scan_config(vec![vec![task]])?)
        .with_projection_indices(Some(vec![1]))?
        .build();
    assert_eq!(
        values(
            &collect(DataSourceExec::from_data_source(config), context.task_ctx())
                .await?
        ),
        vec![10]
    );
    Ok(())
}

#[tokio::test]
async fn byte_splits_keep_whole_file_position_coordinates() -> Result<()> {
    let directory = TempDir::new()?;
    let schema = schema();
    let (path, size) = write_file(
        &directory,
        "split.parquet",
        vec![
            field("key", DataType::Int64, 1),
            field("value", DataType::Int64, 2),
        ],
        vec![
            Arc::new(Int64Array::from_iter_values(0..8)),
            Arc::new(Int64Array::from_iter_values(0..8)),
        ],
    );
    let (pos, pos_size) = write_file(
        &directory,
        "split-pos.parquet",
        vec![
            field(
                "file_path",
                DataType::Utf8,
                RESERVED_FIELD_ID_DELETE_FILE_PATH,
            ),
            field("pos", DataType::Int64, RESERVED_FIELD_ID_DELETE_FILE_POS),
        ],
        vec![
            Arc::new(StringArray::from(vec![path.clone(); 3])),
            Arc::new(Int64Array::from(vec![1, 4, 7])),
        ],
    );
    let tasks: Vec<_> = (0..size)
        .step_by(80)
        .map(|start| {
            FileScanTask::builder()
                .with_data_file_path(path.clone())
                .with_data_file_format(DataFileFormat::Parquet)
                .with_file_size_in_bytes(size)
                .with_start(start)
                .with_length((size - start).min(80))
                .with_record_count(Some(8))
                .with_schema(schema.clone())
                .with_project_field_ids(vec![2])
                .with_case_sensitive(true)
                .with_deletes(vec![delete_file(pos.clone(), pos_size, false)])
                .build()
                .unwrap()
        })
        .collect();
    let context = SessionContext::new_with_config(config(true));
    let source = IcebergFileSource::new(
        ParquetSource::new(Arc::new(
            schema_to_arrow_schema(&schema).map_err(to_datafusion_error)?,
        ))
        .with_pushdown_filters(true),
        schema,
        FileIO::new_with_fs(),
        context.task_ctx(),
    )?;
    let mut actual = Vec::new();
    for task in tasks {
        let expected = iceberg::arrow::ArrowReaderBuilder::new(
            FileIO::new_with_fs(),
            iceberg::Runtime::try_current().map_err(to_datafusion_error)?,
        )
        .build()
        .read(Box::pin(futures::stream::iter([Ok(task.clone())])))
        .map_err(to_datafusion_error)?
        .stream()
        .try_collect::<Vec<_>>()
        .await
        .map_err(to_datafusion_error)?;
        let config =
            FileScanConfigBuilder::from(source.file_scan_config(vec![vec![task]])?)
                .with_projection_indices(Some(vec![1]))?
                .build();
        let config = config
            .file_source
            .repartitioned(3, 1, None, &config)?
            .unwrap();
        assert_eq!(config.file_groups.len(), 3);
        let batches =
            collect(DataSourceExec::from_data_source(config), context.task_ctx()).await?;
        assert_eq!(values(&batches), values(&expected));
        actual.extend(batches);
    }
    assert_eq!(values(&actual), vec![0, 2, 3, 5, 6]);
    Ok(())
}

#[tokio::test]
async fn assigned_native_tasks_respect_the_memory_pool() -> Result<()> {
    use datafusion::execution::memory_pool::GreedyMemoryPool;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    let runtime = Arc::new(
        RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(GreedyMemoryPool::new(1)))
            .build()?,
    );
    let context =
        SessionContext::new_with_config_rt(SessionConfig::new(), runtime.clone());
    let schema = schema();
    let source = IcebergFileSource::new(
        ParquetSource::new(Arc::new(
            schema_to_arrow_schema(&schema).map_err(to_datafusion_error)?,
        )),
        schema.clone(),
        FileIO::new_with_fs(),
        context.task_ctx(),
    )?;
    let error = source
        .file_scan_config(vec![vec![task(
            "data.parquet".to_owned(),
            1,
            schema,
            Some(1),
            vec![],
        )]])
        .unwrap_err();
    assert!(error.to_string().contains("Resources exhausted"), "{error}");
    assert_eq!(runtime.memory_pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn missing_required_field_fails_in_native_binding() -> Result<()> {
    let directory = TempDir::new()?;
    let schema = Arc::new(
        Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "required",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap(),
    );
    let (path, size) = write_file(
        &directory,
        "missing.parquet",
        vec![field("different", DataType::Int64, 2)],
        vec![Arc::new(Int64Array::from(vec![1]))],
    );
    let context = SessionContext::new();
    let source = IcebergFileSource::new(
        ParquetSource::new(Arc::new(
            schema_to_arrow_schema(&schema).map_err(to_datafusion_error)?,
        )),
        schema.clone(),
        FileIO::new_with_fs(),
        context.task_ctx(),
    )?;
    let config =
        source.file_scan_config(vec![vec![task(path, size, schema, Some(1), vec![])]])?;
    let error = collect(DataSourceExec::from_data_source(config), context.task_ctx())
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("Missing required field"),
        "{error}"
    );
    Ok(())
}

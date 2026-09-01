// Licensed under the Apache License, Version 2.0.
//
// Portions derived from LakeSail:
// https://github.com/lakehq/sail
// Original file: crates/sail-iceberg/src/datasource/provider.rs
// Source revision: 1a449ec44fabc53ee38f19661b5dbe47e45aa676
//
// Modified for standalone DataFusion and DataFusion Distributed integration.

use std::cmp::Ordering;
use std::sync::Arc;

use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{DataType, FieldRef, SchemaRef, TimeUnit};
use datafusion::catalog::Session;
use datafusion::common::stats::Precision;
use datafusion::common::{
    ColumnStatistics, DFSchema, DataFusionError, Result, ScalarValue, Statistics,
};
use datafusion::config::TableParquetOptions;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{
    LexOrdering, Partitioning, PhysicalExpr, PhysicalSortExpr, RangePartitioning, SplitPoint,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::prelude::Expr;
use futures::TryStreamExt;
use iceberg::scan::FileScanTask;
use iceberg::spec::{
    DataContentType, DataFileFormat, Literal, PrimitiveLiteral, PrimitiveType, Transform, Type,
};
use url::Url;

use crate::common::{convert_filters_to_predicate, df_err};

pub(crate) struct NativeScanOptions<'a> {
    pub(crate) session: &'a dyn Session,
    pub(crate) snapshot_id: Option<i64>,
    pub(crate) projection: Option<&'a Vec<usize>>,
    pub(crate) filters: &'a [Expr],
    pub(crate) limit: Option<usize>,
    pub(crate) target_partitions: usize,
    pub(crate) parquet_options: TableParquetOptions,
}

pub(crate) async fn plan_native_scan(
    table: &iceberg::table::Table,
    table_schema: SchemaRef,
    options: NativeScanOptions<'_>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut scan = match options.snapshot_id {
        Some(snapshot_id) => table.scan().snapshot_id(snapshot_id),
        None => table.scan(),
    }
    .select_all();
    if let Some(predicate) = convert_filters_to_predicate(options.filters) {
        scan = scan.with_filter(predicate);
    }

    let tasks = scan
        .build()
        .map_err(df_err)?
        .plan_files()
        .await
        .map_err(df_err)?
        .try_collect::<Vec<_>>()
        .await
        .map_err(df_err)?;

    if let Some(task) = tasks
        .iter()
        .find(|task| task.data_file_format != DataFileFormat::Parquet)
    {
        return Err(DataFusionError::NotImplemented(format!(
            "Iceberg data format {} is not supported; native scans require Parquet",
            task.data_file_format
        )));
    }

    if table.metadata().partition_specs_iter().len() > 1 {
        return Err(DataFusionError::NotImplemented(
            "Iceberg partition-spec evolution requires scan tasks to expose partition_spec_id; refusing to declare a potentially incorrect range"
                .to_string(),
        ));
    }
    let default_spec = table.metadata().default_partition_spec();
    let identity_fields =
        common_identity_fields(&tasks, table.metadata().current_schema(), default_spec)?;
    let partition_fields = identity_fields
        .iter()
        .map(|field| {
            table_schema
                .fields()
                .iter()
                .find(|candidate| candidate.name() == field.name())
                .cloned()
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "Iceberg identity partition field {} is missing from the table schema",
                        field.name()
                    ))
                })
        })
        .collect::<Result<Vec<_>>>()?;
    // Iceberg partitioning is hidden partitioning: data files retain source
    // columns. Manifest partition values are planning metadata, not virtual
    // directory columns to append to Parquet batches.
    let file_schema = Arc::clone(&table_schema);
    let physical_table_schema = TableSchema::from(Arc::clone(&file_schema));

    if tasks.iter().any(|task| !task.deletes.is_empty()) {
        return plan_merge_on_read(
            table,
            table_schema,
            tasks,
            &identity_fields,
            &partition_fields,
            default_spec,
            options,
        );
    }

    let (object_store_url, files) = build_partitioned_files(
        tasks,
        &identity_fields,
        table.metadata().current_schema(),
        default_spec,
        Arc::clone(&file_schema),
    )?;
    let (file_groups, output_partitioning) = create_file_groups(
        files,
        &partition_fields,
        partition_fields.first().and_then(|field| {
            table_schema
                .fields()
                .iter()
                .position(|candidate| candidate.name() == field.name())
        }),
        options.target_partitions.max(1),
    )?;

    let source = Arc::new(
        ParquetSource::new(physical_table_schema)
            .with_table_parquet_options(options.parquet_options),
    );
    let statistics = aggregate_statistics(table, &table_schema);
    let range_partitioned = output_partitioning.is_some();
    let config = FileScanConfigBuilder::new(object_store_url, source)
        .with_file_groups(file_groups)
        .with_output_partitioning(output_partitioning)
        // A declared range is a correctness contract. Prevent file work
        // stealing or scan rebalancing from moving a file across its bucket.
        .with_preserve_order(range_partitioned)
        .with_statistics(statistics)
        .with_projection_indices(options.projection.cloned())?
        .with_limit(options.limit)
        .build();
    Ok(datafusion::catalog::memory::DataSourceExec::from_data_source(config))
}

fn plan_merge_on_read(
    table: &iceberg::table::Table,
    table_schema: SchemaRef,
    tasks: Vec<FileScanTask>,
    identity_fields: &[FieldRef],
    partition_fields: &[FieldRef],
    default_spec: &iceberg::spec::PartitionSpec,
    options: NativeScanOptions<'_>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut clean = Vec::new();
    let mut dirty = Vec::new();
    for task in tasks {
        if task.deletes.is_empty() {
            clean.push(task);
        } else {
            if task.start != 0 || task.length < task.file_size_in_bytes {
                return Err(DataFusionError::Plan(format!(
                    "merge-on-read requires a complete scan of data file {}, got byte range {}..{}",
                    task.data_file_path,
                    task.start,
                    task.start.saturating_add(task.length)
                )));
            }
            dirty.push(task);
        }
    }

    let mut branches = Vec::<Arc<dyn ExecutionPlan>>::new();
    if !clean.is_empty() {
        branches.push(build_native_exec(
            table,
            Arc::clone(&table_schema),
            clean,
            identity_fields,
            partition_fields,
            default_spec,
            options.target_partitions,
            options.parquet_options.clone(),
            None,
            None,
        )?);
    }

    for task in dirty {
        let data_file_path = task.data_file_path.clone();
        let mut positional = Vec::new();
        let mut equality = Vec::new();
        for delete in task.deletes.clone() {
            match delete.file_type {
                DataContentType::PositionDeletes => positional.push(delete),
                DataContentType::EqualityDeletes => equality.push(delete),
                DataContentType::Data => {
                    return Err(DataFusionError::Plan(format!(
                        "scan task for {data_file_path} attached a data file as a delete file"
                    )));
                }
            }
        }
        let scan = build_native_exec(
            table,
            Arc::clone(&table_schema),
            vec![task],
            identity_fields,
            partition_fields,
            default_spec,
            1,
            options.parquet_options.clone(),
            None,
            None,
        )?;
        branches.push(Arc::new(crate::delete_apply::IcebergDeleteApplyExec::new(
            scan,
            data_file_path,
            positional,
            equality,
            table.metadata().location().to_string(),
            Arc::clone(table.metadata().current_schema()),
        )));
    }

    let unioned = match branches.len() {
        0 => {
            return Err(DataFusionError::Internal(
                "merge-on-read scan produced no branches".into(),
            ));
        }
        1 => branches.pop().unwrap(),
        _ => UnionExec::try_new(branches)?,
    };
    let filtered: Arc<dyn ExecutionPlan> = if let Some(predicate) =
        datafusion::logical_expr::utils::conjunction(options.filters.to_vec())
    {
        let df_schema = DFSchema::try_from(table_schema.as_ref().clone())?;
        Arc::new(FilterExec::try_new(
            options
                .session
                .create_physical_expr(predicate, &df_schema)?,
            unioned,
        )?)
    } else {
        unioned
    };
    let projected: Arc<dyn ExecutionPlan> = if let Some(projection) = options.projection {
        let expressions: Vec<(Arc<dyn PhysicalExpr>, String)> = projection
            .iter()
            .map(|index| {
                let field = table_schema.field(*index);
                (
                    Arc::new(Column::new(field.name(), *index)) as Arc<dyn PhysicalExpr>,
                    field.name().to_string(),
                )
            })
            .collect();
        Arc::new(ProjectionExec::try_new(expressions, filtered)?)
    } else {
        filtered
    };
    Ok(match options.limit {
        Some(limit) => Arc::new(GlobalLimitExec::new(projected, 0, Some(limit))),
        None => projected,
    })
}

#[allow(clippy::too_many_arguments)]
fn build_native_exec(
    table: &iceberg::table::Table,
    table_schema: SchemaRef,
    tasks: Vec<FileScanTask>,
    identity_fields: &[FieldRef],
    partition_fields: &[FieldRef],
    default_spec: &iceberg::spec::PartitionSpec,
    target_partitions: usize,
    parquet_options: TableParquetOptions,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let (object_store_url, files) = build_partitioned_files(
        tasks,
        identity_fields,
        table.metadata().current_schema(),
        default_spec,
        Arc::clone(&table_schema),
    )?;
    let (file_groups, output_partitioning) = create_file_groups(
        files,
        partition_fields,
        partition_fields.first().and_then(|field| {
            table_schema
                .fields()
                .iter()
                .position(|candidate| candidate.name() == field.name())
        }),
        target_partitions.max(1),
    )?;
    let range_partitioned = output_partitioning.is_some();
    let source = Arc::new(
        ParquetSource::new(TableSchema::from(Arc::clone(&table_schema)))
            .with_table_parquet_options(parquet_options),
    );
    let config = FileScanConfigBuilder::new(object_store_url, source)
        .with_file_groups(file_groups)
        .with_output_partitioning(output_partitioning)
        .with_preserve_order(range_partitioned)
        .with_statistics(aggregate_statistics(table, &table_schema))
        .with_projection_indices(projection)?
        .with_limit(limit)
        .build();
    Ok(datafusion::catalog::memory::DataSourceExec::from_data_source(config))
}

fn common_identity_fields(
    tasks: &[FileScanTask],
    schema: &iceberg::spec::Schema,
    default_spec: &iceberg::spec::PartitionSpec,
) -> Result<Vec<FieldRef>> {
    let mut expected: Option<Vec<i32>> = None;
    for task in tasks {
        let ids = task
            .partition_spec
            .as_ref()
            .map(|spec| {
                spec.fields()
                    .iter()
                    .filter(|field| field.transform == Transform::Identity)
                    .map(|field| field.source_id)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_else(|| {
                default_spec
                    .fields()
                    .iter()
                    .filter(|field| field.transform == Transform::Identity)
                    .map(|field| field.source_id)
                    .collect()
            });
        if expected.as_ref().is_some_and(|expected| expected != &ids) {
            return Err(DataFusionError::NotImplemented(
                "Iceberg partition-spec evolution with different identity fields requires an explicit repartition"
                    .to_string(),
            ));
        }
        expected = Some(ids);
    }

    expected
        .unwrap_or_else(|| {
            tasks
                .first()
                .and_then(|task| task.partition_spec.as_ref())
                .map(|spec| {
                    spec.fields()
                        .iter()
                        .filter(|field| field.transform == Transform::Identity)
                        .map(|field| field.source_id)
                        .collect()
                })
                .unwrap_or_else(|| {
                    default_spec
                        .fields()
                        .iter()
                        .filter(|field| field.transform == Transform::Identity)
                        .map(|field| field.source_id)
                        .collect()
                })
        })
        .into_iter()
        .map(|id| {
            let field = schema.field_by_id(id).ok_or_else(|| {
                DataFusionError::Plan(format!("Iceberg partition source field id {id} not found"))
            })?;
            crate::schema::iceberg_schema_to_arrow(
                &iceberg::spec::Schema::builder()
                    .with_fields([field.clone()])
                    .build()
                    .map_err(df_err)?,
            )
            .map(|schema| schema.field(0).clone().into())
        })
        .collect()
}

fn build_partitioned_files(
    tasks: Vec<FileScanTask>,
    identity_fields: &[FieldRef],
    schema: &iceberg::spec::Schema,
    default_spec: &iceberg::spec::PartitionSpec,
    _file_schema: SchemaRef,
) -> Result<(ObjectStoreUrl, Vec<PartitionedFile>)> {
    let mut store_url = None;
    let mut files = Vec::with_capacity(tasks.len());

    for task in tasks {
        let url = Url::parse(&task.data_file_path)
            .or_else(|error| {
                if std::path::Path::new(&task.data_file_path).is_absolute() {
                    Url::from_file_path(&task.data_file_path).map_err(|_| error)
                } else {
                    Err(error)
                }
            })
            .map_err(external_error)?;
        let current_store =
            ObjectStoreUrl::parse(&url[..url::Position::BeforePath]).map_err(external_error)?;
        if store_url
            .as_ref()
            .is_some_and(|expected: &ObjectStoreUrl| expected != &current_store)
        {
            return Err(DataFusionError::NotImplemented(
                "a single Iceberg scan cannot currently span multiple object stores".to_string(),
            ));
        }
        store_url = Some(current_store);

        let path = if url.scheme() == "file" {
            url.path().to_string()
        } else {
            url.path().trim_start_matches('/').to_string()
        };
        let end = task.start.saturating_add(task.length);
        let mut file = if task.start == 0 && end >= task.file_size_in_bytes {
            PartitionedFile::new(path, task.file_size_in_bytes)
        } else {
            PartitionedFile::new_with_range(
                path,
                task.file_size_in_bytes,
                i64::try_from(task.start).map_err(external_error)?,
                i64::try_from(end).map_err(external_error)?,
            )
        };

        if !identity_fields.is_empty() {
            let spec = task.partition_spec.as_deref().unwrap_or(default_spec);
            let partition = task.partition.as_ref().ok_or_else(|| {
                DataFusionError::Plan("partitioned scan task is missing partition values".into())
            })?;
            let values = spec
                .fields()
                .iter()
                .zip(partition.iter())
                .filter(|(field, _)| field.transform == Transform::Identity)
                .map(|(field, literal)| {
                    let source_type = schema
                        .field_by_id(field.source_id)
                        .ok_or_else(|| {
                            DataFusionError::Plan(format!(
                                "Iceberg partition source field id {} not found",
                                field.source_id
                            ))
                        })?
                        .field_type
                        .as_ref();
                    literal_to_scalar(literal, source_type)
                })
                .collect::<Result<Vec<_>>>()?;
            file = file.with_partition_values(values);
        }
        files.push(file);
    }

    let store_url = store_url.unwrap_or_else(ObjectStoreUrl::local_filesystem);
    Ok((store_url, files))
}

fn create_file_groups(
    mut files: Vec<PartitionedFile>,
    partition_fields: &[FieldRef],
    partition_column_index: Option<usize>,
    target_partitions: usize,
) -> Result<(Vec<FileGroup>, Option<Partitioning>)> {
    if files.is_empty() {
        return Ok((vec![FileGroup::new(vec![])], None));
    }

    if partition_fields.len() != 1
        || files
            .iter()
            .any(|file| file.partition_values.len() != partition_fields.len())
    {
        let count = target_partitions.min(files.len()).max(1);
        let mut groups = (0..count).map(|_| Vec::new()).collect::<Vec<_>>();
        for (index, file) in files.into_iter().enumerate() {
            groups[index % count].push(file.with_partition_values(vec![]));
        }
        return Ok((groups.into_iter().map(FileGroup::new).collect(), None));
    }

    files.sort_by(|left, right| scalar_cmp(&left.partition_values[0], &right.partition_values[0]));
    let mut key_groups: Vec<(ScalarValue, Vec<PartitionedFile>)> = Vec::new();
    for file in files {
        let key = file.partition_values[0].clone();
        if let Some((previous, group)) = key_groups.last_mut()
            && *previous == key
        {
            group.push(file);
        } else {
            key_groups.push((key, vec![file]));
        }
    }

    let bucket_count = target_partitions.min(key_groups.len()).max(1);
    let keys_per_bucket = key_groups.len().div_ceil(bucket_count);
    let mut groups = Vec::with_capacity(bucket_count);
    let mut split_points = Vec::with_capacity(bucket_count.saturating_sub(1));
    for (bucket_index, chunk) in key_groups.chunks(keys_per_bucket).enumerate() {
        if bucket_index > 0 {
            split_points.push(SplitPoint::new(vec![chunk[0].0.clone()]));
        }
        groups.push(FileGroup::new(
            chunk
                .iter()
                .flat_map(|(_, files)| files.iter().cloned())
                .map(|file| file.with_partition_values(vec![]))
                .collect(),
        ));
    }

    let partition_column_index = partition_column_index.ok_or_else(|| {
        DataFusionError::Internal("range partition column is missing from the file schema".into())
    })?;
    let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new(
        partition_fields[0].name(),
        partition_column_index,
    ));
    let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
        column,
        SortOptions {
            descending: false,
            nulls_first: false,
        },
    )])
    .ok_or_else(|| DataFusionError::Internal("range ordering cannot be empty".into()))?;
    let partitioning = Partitioning::Range(RangePartitioning::try_new(ordering, split_points)?);
    Ok((groups, Some(partitioning)))
}

fn scalar_cmp(left: &ScalarValue, right: &ScalarValue) -> Ordering {
    match (left.is_null(), right.is_null()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => left.partial_cmp(right).unwrap_or(Ordering::Equal),
    }
}

fn literal_to_scalar(literal: Option<&Literal>, ty: &Type) -> Result<ScalarValue> {
    let Some(Literal::Primitive(literal)) = literal else {
        return ScalarValue::try_from(iceberg_type_to_arrow(ty)).map_err(external_error);
    };
    let Type::Primitive(primitive_type) = ty else {
        return Err(DataFusionError::NotImplemented(
            "nested Iceberg partition values are not supported".to_string(),
        ));
    };
    let value = match (primitive_type, literal) {
        (PrimitiveType::Boolean, PrimitiveLiteral::Boolean(value)) => {
            ScalarValue::Boolean(Some(*value))
        }
        (PrimitiveType::Int, PrimitiveLiteral::Int(value)) => ScalarValue::Int32(Some(*value)),
        (PrimitiveType::Long, PrimitiveLiteral::Long(value)) => ScalarValue::Int64(Some(*value)),
        (PrimitiveType::Float, PrimitiveLiteral::Float(value)) => {
            ScalarValue::Float32(Some(value.into_inner()))
        }
        (PrimitiveType::Double, PrimitiveLiteral::Double(value)) => {
            ScalarValue::Float64(Some(value.into_inner()))
        }
        (PrimitiveType::Date, PrimitiveLiteral::Int(value)) => ScalarValue::Date32(Some(*value)),
        (PrimitiveType::Time, PrimitiveLiteral::Long(value)) => {
            ScalarValue::Time64Microsecond(Some(*value))
        }
        (PrimitiveType::Timestamp, PrimitiveLiteral::Long(value)) => {
            ScalarValue::TimestampMicrosecond(Some(*value), None)
        }
        (PrimitiveType::Timestamptz, PrimitiveLiteral::Long(value)) => {
            ScalarValue::TimestampMicrosecond(Some(*value), Some("+00:00".into()))
        }
        (PrimitiveType::TimestampNs, PrimitiveLiteral::Long(value)) => {
            ScalarValue::TimestampNanosecond(Some(*value), None)
        }
        (PrimitiveType::TimestamptzNs, PrimitiveLiteral::Long(value)) => {
            ScalarValue::TimestampNanosecond(Some(*value), Some("+00:00".into()))
        }
        (PrimitiveType::String, PrimitiveLiteral::String(value)) => {
            ScalarValue::Utf8(Some(value.clone()))
        }
        (PrimitiveType::Binary, PrimitiveLiteral::Binary(value)) => {
            ScalarValue::LargeBinary(Some(value.clone()))
        }
        (PrimitiveType::Fixed(_), PrimitiveLiteral::Binary(value)) => ScalarValue::FixedSizeBinary(
            i32::try_from(value.len()).map_err(external_error)?,
            Some(value.clone()),
        ),
        (PrimitiveType::Uuid, PrimitiveLiteral::UInt128(value)) => {
            ScalarValue::FixedSizeBinary(16, Some(value.to_be_bytes().to_vec()))
        }
        (PrimitiveType::Decimal { precision, scale }, PrimitiveLiteral::Int128(value)) => {
            ScalarValue::Decimal128(
                Some(*value),
                u8::try_from(*precision).map_err(external_error)?,
                i8::try_from(*scale).map_err(external_error)?,
            )
        }
        _ => {
            return Err(DataFusionError::Plan(format!(
                "Iceberg partition literal {literal:?} does not match {primitive_type}"
            )));
        }
    };
    Ok(value)
}

fn iceberg_type_to_arrow(ty: &Type) -> DataType {
    match ty {
        Type::Primitive(PrimitiveType::Boolean) => DataType::Boolean,
        Type::Primitive(PrimitiveType::Int) => DataType::Int32,
        Type::Primitive(PrimitiveType::Long) => DataType::Int64,
        Type::Primitive(PrimitiveType::Float) => DataType::Float32,
        Type::Primitive(PrimitiveType::Double) => DataType::Float64,
        Type::Primitive(PrimitiveType::Date) => DataType::Date32,
        Type::Primitive(PrimitiveType::Time) => DataType::Time64(TimeUnit::Microsecond),
        Type::Primitive(PrimitiveType::Timestamp) => {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        Type::Primitive(PrimitiveType::Timestamptz) => {
            DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()))
        }
        Type::Primitive(PrimitiveType::TimestampNs) => {
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        }
        Type::Primitive(PrimitiveType::TimestamptzNs) => {
            DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into()))
        }
        Type::Primitive(PrimitiveType::String) => DataType::Utf8,
        Type::Primitive(PrimitiveType::Uuid) => DataType::FixedSizeBinary(16),
        Type::Primitive(PrimitiveType::Fixed(length)) => i32::try_from(*length)
            .ok()
            .map(DataType::FixedSizeBinary)
            .unwrap_or(DataType::LargeBinary),
        Type::Primitive(PrimitiveType::Binary) => DataType::LargeBinary,
        Type::Primitive(PrimitiveType::Decimal { precision, scale }) => {
            DataType::Decimal128(*precision as u8, *scale as i8)
        }
        _ => DataType::Null,
    }
}

fn aggregate_statistics(table: &iceberg::table::Table, schema: &SchemaRef) -> Statistics {
    const TOTAL_RECORDS: &str = "total-records";
    const TOTAL_FILE_SIZE: &str = "total-files-size";
    let Some(snapshot) = table.metadata().current_snapshot() else {
        return Statistics {
            num_rows: Precision::Exact(0),
            total_byte_size: Precision::Exact(0),
            column_statistics: vec![ColumnStatistics::new_unknown(); schema.fields().len()],
        };
    };
    let properties = &snapshot.summary().additional_properties;
    Statistics {
        num_rows: properties
            .get(TOTAL_RECORDS)
            .and_then(|value| value.parse().ok())
            .map(Precision::Exact)
            .unwrap_or(Precision::Absent),
        total_byte_size: properties
            .get(TOTAL_FILE_SIZE)
            .and_then(|value| value.parse().ok())
            .map(Precision::Exact)
            .unwrap_or(Precision::Absent),
        column_statistics: vec![ColumnStatistics::new_unknown(); schema.fields().len()],
    }
}

fn external_error(error: impl std::error::Error + Send + Sync + 'static) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

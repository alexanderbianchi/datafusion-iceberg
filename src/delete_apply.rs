// Licensed under the Apache License, Version 2.0.
//
// Portions derived from LakeSail:
// https://github.com/lakehq/sail
// Original file: crates/sail-iceberg/src/physical_plan/delete_apply_exec.rs
// Source revision: 1a449ec44fabc53ee38f19661b5dbe47e45aa676
//
// Modified for standalone DataFusion and DataFusion Distributed integration.

use std::collections::HashSet;
use std::fmt;
use std::ops::Range;
use std::sync::Arc;

use async_stream::try_stream;
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray,
};
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::row::{OwnedRow, RowConverter, SortField};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties, SendableRecordBatchStream,
};
use futures::future::BoxFuture;
use futures::stream::TryStreamExt;
use iceberg::scan::FileScanTaskDeleteFile;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::{AsyncFileReader, ParquetRecordBatchStreamBuilder};
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use parquet::schema::types::SchemaDescriptor;
use url::Url;

const FILE_PATH_COLUMN: &str = "file_path";
const POSITION_COLUMN: &str = "pos";

#[derive(Clone)]
struct ObjectStoreParquetReader {
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
    size: u64,
}

impl AsyncFileReader for ObjectStoreParquetReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        Box::pin(async move {
            self.store
                .get_range(&self.path, range)
                .await
                .map_err(parquet_store_error)
        })
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, ParquetResult<Vec<Bytes>>> {
        Box::pin(async move {
            self.store
                .get_ranges(&self.path, &ranges)
                .await
                .map_err(parquet_store_error)
        })
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        let size = self.size;
        Box::pin(async move {
            Ok(Arc::new(
                ParquetMetaDataReader::new()
                    .with_arrow_reader_options(options)
                    .load_and_finish(self, size)
                    .await?,
            ))
        })
    }
}

fn parquet_store_error(error: object_store::Error) -> ParquetError {
    ParquetError::External(Box::new(error))
}

/// Applies the delete files that Iceberg associated with one complete data file.
///
/// The child is deliberately a one-file, one-partition, unfiltered Parquet scan.
/// That keeps the stream offset equal to the Iceberg row position. Filters and
/// the user projection are installed above this node by the scan planner.
#[derive(Debug, Clone)]
pub(crate) struct IcebergDeleteApplyExec {
    input: Arc<dyn ExecutionPlan>,
    data_file_path: String,
    positional_deletes: Vec<FileScanTaskDeleteFile>,
    equality_deletes: Vec<FileScanTaskDeleteFile>,
    table_url: String,
    iceberg_schema: iceberg::spec::SchemaRef,
    cache: Arc<PlanProperties>,
}

impl IcebergDeleteApplyExec {
    pub(crate) fn new(
        input: Arc<dyn ExecutionPlan>,
        data_file_path: String,
        positional_deletes: Vec<FileScanTaskDeleteFile>,
        equality_deletes: Vec<FileScanTaskDeleteFile>,
        table_url: String,
        iceberg_schema: iceberg::spec::SchemaRef,
    ) -> Self {
        let cache = Arc::new(PlanProperties::new(
            input.equivalence_properties().clone(),
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        ));
        Self {
            input,
            data_file_path,
            positional_deletes,
            equality_deletes,
            table_url,
            iceberg_schema,
            cache,
        }
    }

    pub(crate) fn data_file_path(&self) -> &str {
        &self.data_file_path
    }

    pub(crate) fn positional_deletes(&self) -> &[FileScanTaskDeleteFile] {
        &self.positional_deletes
    }

    pub(crate) fn equality_deletes(&self) -> &[FileScanTaskDeleteFile] {
        &self.equality_deletes
    }

    pub(crate) fn table_url(&self) -> &str {
        &self.table_url
    }

    pub(crate) fn iceberg_schema(&self) -> &iceberg::spec::SchemaRef {
        &self.iceberg_schema
    }
}

impl DisplayAs for IcebergDeleteApplyExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "IcebergDeleteApplyExec: data_file={}, position_deletes={}, equality_deletes={}",
            self.data_file_path,
            self.positional_deletes.len(),
            self.equality_deletes.len()
        )
    }
}

#[async_trait]
impl ExecutionPlan for IcebergDeleteApplyExec {
    fn name(&self) -> &str {
        "IcebergDeleteApplyExec"
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![true]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "IcebergDeleteApplyExec requires exactly one child".into(),
            ));
        }
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            self.data_file_path.clone(),
            self.positional_deletes.clone(),
            self.equality_deletes.clone(),
            self.table_url.clone(),
            Arc::clone(&self.iceberg_schema),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = self.schema();
        let mut child = self.input.execute(partition, Arc::clone(&context))?;
        let data_file_path = self.data_file_path.clone();
        let positional_deletes = self.positional_deletes.clone();
        let equality_deletes = self.equality_deletes.clone();
        let table_url = self.table_url.clone();
        let iceberg_schema = Arc::clone(&self.iceberg_schema);

        let stream = try_stream! {
            // Range-aligned dirty branches deliberately contain empty file groups
            // outside the data file's bucket. Avoid opening delete files for those
            // partitions, while keeping a one-to-one partition mapping to the child.
            let Some(first_batch) = child.try_next().await? else {
                return;
            };
            let positions = load_positions(
                &context,
                &table_url,
                &positional_deletes,
                &data_file_path,
            ).await?;
            let equality = load_equality(
                &context,
                &table_url,
                &equality_deletes,
                &iceberg_schema,
            ).await?;

            let mut row_offset = 0_u64;
            let mut next_batch = Some(first_batch);
            while let Some(batch) = next_batch {
                let mask = delete_mask(&batch, row_offset, &positions, &equality)?;
                row_offset += batch.num_rows() as u64;
                let filtered = filter_record_batch(&batch, &mask)
                    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;
                if filtered.num_rows() != 0 {
                    yield filtered;
                }
                next_batch = child.try_next().await?;
            }
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

async fn load_positions(
    context: &TaskContext,
    table_url: &str,
    files: &[FileScanTaskDeleteFile],
    data_file_path: &str,
) -> Result<Vec<u64>> {
    let mut positions = Vec::new();
    for file in files {
        for batch in read_parquet(context, table_url, file).await? {
            let paths = batch
                .column_by_name(FILE_PATH_COLUMN)
                .and_then(|array| array.as_any().downcast_ref::<StringArray>())
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "position-delete file {} is missing Utf8 column '{FILE_PATH_COLUMN}'",
                        file.file_path
                    ))
                })?;
            let offsets = batch
                .column_by_name(POSITION_COLUMN)
                .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "position-delete file {} is missing Int64 column '{POSITION_COLUMN}'",
                        file.file_path
                    ))
                })?;
            for index in 0..batch.num_rows() {
                if !paths.is_null(index)
                    && !offsets.is_null(index)
                    && paths.value(index) == data_file_path
                    && offsets.value(index) >= 0
                {
                    positions.push(offsets.value(index) as u64);
                }
            }
        }
    }
    positions.sort_unstable();
    positions.dedup();
    Ok(positions)
}

struct EqualityField {
    id: i32,
    name: String,
    data_type: DataType,
}

struct LoadedEqualityDelete {
    fields: Vec<EqualityField>,
    converter: RowConverter,
    rows: HashSet<OwnedRow>,
}

async fn load_equality(
    context: &TaskContext,
    table_url: &str,
    files: &[FileScanTaskDeleteFile],
    schema: &iceberg::spec::Schema,
) -> Result<Vec<LoadedEqualityDelete>> {
    let mut loaded = Vec::with_capacity(files.len());
    for file in files {
        let ids = file
            .equality_ids
            .as_deref()
            .filter(|ids| !ids.is_empty())
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "equality-delete file {} has no equality field IDs",
                    file.file_path
                ))
            })?;
        let fields = equality_fields(schema, ids)?;
        let converter = RowConverter::new(
            fields
                .iter()
                .map(|field| SortField::new(field.data_type.clone()))
                .collect(),
        )
        .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;

        let (store, path) = resolve_store(context, table_url, &file.file_path)?;
        let reader = ObjectStoreParquetReader {
            store,
            path,
            size: file.file_size_in_bytes,
        };
        let builder = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .map_err(external_error)?;
        let projection = EqualityProjection::try_new(builder.parquet_schema(), &fields)?;
        let mut stream = builder
            .with_projection(projection.mask.clone())
            .build()
            .map_err(external_error)?;
        let mut rows = HashSet::new();
        while let Some(batch) = stream.try_next().await.map_err(external_error)? {
            let columns = projection.columns(&batch, &fields)?;
            let encoded = converter
                .convert_columns(&columns)
                .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;
            for index in 0..encoded.num_rows() {
                rows.insert(encoded.row(index).owned());
            }
        }
        loaded.push(LoadedEqualityDelete {
            fields,
            converter,
            rows,
        });
    }
    Ok(loaded)
}

fn equality_fields(schema: &iceberg::spec::Schema, ids: &[i32]) -> Result<Vec<EqualityField>> {
    ids.iter()
        .map(|id| {
            let field = schema.field_by_id(*id).ok_or_else(|| {
                DataFusionError::Plan(format!("equality delete references unknown field id {id}"))
            })?;
            let one = iceberg::spec::Schema::builder()
                .with_fields([field.clone()])
                .build()
                .map_err(external_error)?;
            let arrow = crate::schema::iceberg_schema_to_arrow(&one)?;
            Ok(EqualityField {
                id: *id,
                name: field.name.clone(),
                data_type: arrow.field(0).data_type().clone(),
            })
        })
        .collect()
}

struct EqualityProjection {
    mask: ProjectionMask,
    indices: Vec<usize>,
}

impl EqualityProjection {
    fn try_new(schema: &SchemaDescriptor, fields: &[EqualityField]) -> Result<Self> {
        let roots = schema.root_schema().get_fields();
        let mut root_indices = Vec::with_capacity(fields.len());
        for field in fields {
            let matches = roots
                .iter()
                .enumerate()
                .filter_map(|(index, ty)| {
                    let info = ty.get_basic_info();
                    (info.has_id() && info.id() == field.id).then_some(index)
                })
                .collect::<Vec<_>>();
            match matches.as_slice() {
                [index] => root_indices.push(*index),
                [] => {
                    return Err(DataFusionError::Execution(format!(
                        "equality-delete Parquet file is missing Iceberg field id {}",
                        field.id
                    )));
                }
                _ => {
                    return Err(DataFusionError::Execution(format!(
                        "equality-delete Parquet file repeats Iceberg field id {}",
                        field.id
                    )));
                }
            }
        }
        let mut projected = root_indices.clone();
        projected.sort_unstable();
        projected.dedup();
        let indices = root_indices
            .iter()
            .map(|index| {
                projected.binary_search(index).map_err(|_| {
                    DataFusionError::Internal("failed to map an equality-delete projection".into())
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            mask: ProjectionMask::roots(schema, projected),
            indices,
        })
    }

    fn columns(&self, batch: &RecordBatch, fields: &[EqualityField]) -> Result<Vec<ArrayRef>> {
        self.indices
            .iter()
            .zip(fields)
            .map(|(index, field)| {
                let column = batch.column(*index);
                if column.data_type() != &field.data_type {
                    return Err(DataFusionError::Execution(format!(
                        "equality-delete field id {} has type {:?}, expected {:?}",
                        field.id,
                        column.data_type(),
                        field.data_type
                    )));
                }
                Ok(Arc::clone(column))
            })
            .collect()
    }
}

async fn read_parquet(
    context: &TaskContext,
    table_url: &str,
    file: &FileScanTaskDeleteFile,
) -> Result<Vec<RecordBatch>> {
    let (store, path) = resolve_store(context, table_url, &file.file_path)?;
    let reader = ObjectStoreParquetReader {
        store,
        path,
        size: file.file_size_in_bytes,
    };
    ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(external_error)?
        .build()
        .map_err(external_error)?
        .try_collect()
        .await
        .map_err(external_error)
}

fn resolve_store(
    context: &TaskContext,
    table_url: &str,
    raw_path: &str,
) -> Result<(Arc<dyn ObjectStore>, ObjectPath)> {
    let url = match parse_location(raw_path) {
        Ok(url) => url,
        Err(_) => {
            let base = format!("{}/", table_url.trim_end_matches('/'));
            parse_location(&base)
                .map_err(external_error)?
                .join(raw_path)
                .map_err(external_error)?
        }
    };
    let store = context
        .runtime_env()
        .object_store_registry
        .get_store(&url)?;
    let path = ObjectPath::from_url_path(url.path()).map_err(external_error)?;
    Ok((store, path))
}

fn parse_location(location: &str) -> std::result::Result<Url, url::ParseError> {
    Url::parse(location).or_else(|error| {
        if std::path::Path::new(location).is_absolute() {
            Url::from_file_path(location).map_err(|_| error)
        } else {
            Err(error)
        }
    })
}

fn delete_mask(
    batch: &RecordBatch,
    row_offset: u64,
    positions: &[u64],
    equality: &[LoadedEqualityDelete],
) -> Result<BooleanArray> {
    let mut keep = vec![true; batch.num_rows()];
    let end = row_offset + batch.num_rows() as u64;
    let first = positions.partition_point(|position| *position < row_offset);
    let last = positions.partition_point(|position| *position < end);
    for position in &positions[first..last] {
        keep[(*position - row_offset) as usize] = false;
    }

    for delete in equality {
        let columns = delete
            .fields
            .iter()
            .map(|field| {
                let column = batch.column_by_name(&field.name).ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "data scan is missing equality-delete column '{}'",
                        field.name
                    ))
                })?;
                if column.data_type() != &field.data_type {
                    return Err(DataFusionError::Execution(format!(
                        "data column '{}' has type {:?}, expected {:?}",
                        field.name,
                        column.data_type(),
                        field.data_type
                    )));
                }
                Ok(Arc::clone(column))
            })
            .collect::<Result<Vec<_>>>()?;
        let encoded = delete
            .converter
            .convert_columns(&columns)
            .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;
        for (index, keep) in keep.iter_mut().enumerate() {
            if *keep && delete.rows.contains(&encoded.row(index).owned()) {
                *keep = false;
            }
        }
    }
    Ok(BooleanArray::from(keep))
}

fn external_error(error: impl std::error::Error + Send + Sync + 'static) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{Field, Schema};

    #[test]
    fn positional_mask_uses_absolute_offsets() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![10, 11, 12, 13]))],
        )
        .unwrap();
        let mask = delete_mask(&batch, 7, &[6, 8, 10, 20], &[]).unwrap();
        assert_eq!(
            (0..mask.len())
                .map(|index| mask.value(index))
                .collect::<Vec<_>>(),
            vec![true, false, true, false]
        );
    }

    #[test]
    fn equality_mask_matches_projected_field_values() {
        let ids: ArrayRef = Arc::new(Int32Array::from(vec![10, 11, 10, 12]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::clone(&ids)],
        )
        .unwrap();
        let converter = RowConverter::new(vec![SortField::new(DataType::Int32)]).unwrap();
        let deleted = converter.convert_columns(&[ids]).unwrap();
        let equality = LoadedEqualityDelete {
            fields: vec![EqualityField {
                id: 1,
                name: "id".into(),
                data_type: DataType::Int32,
            }],
            converter,
            rows: HashSet::from([deleted.row(1).owned()]),
        };

        let mask = delete_mask(&batch, 0, &[], &[equality]).unwrap();
        assert_eq!(
            (0..mask.len())
                .map(|index| mask.value(index))
                .collect::<Vec<_>>(),
            vec![true, false, true, true]
        );
    }
}

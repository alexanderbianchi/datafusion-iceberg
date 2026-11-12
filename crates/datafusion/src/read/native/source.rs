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

use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::{BooleanArray, RecordBatch};
use datafusion::arrow::compute::filter_record_batch;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Result, exec_err};
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{
    FileScanConfig, FileScanConfigBuilder, FileSource, ParquetSource,
};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::execution::TaskContext;
use datafusion::object_store::ObjectStore;
use datafusion::physical_expr::projection::{ProjectionExpr, ProjectionExprs};
use datafusion::physical_expr::utils::conjunction;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::filter_pushdown::FilterPushdownPropagation;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::sort_pushdown::SortOrderPushdownResult;
use datafusion_datasource::PartitionedFile;
use datafusion_datasource::file_stream::FileOpener;
use datafusion_datasource::morsel::{Morsel, MorselPlan, MorselPlanner, Morselizer};
use datafusion_datasource_parquet::ParquetRowSelection;
use futures::TryStreamExt;
use futures::stream::BoxStream;
use iceberg::arrow::DeleteReadContext;
use iceberg::io::FileIO;
use iceberg::scan::FileScanTask;
use iceberg::spec::SchemaRef;

use super::expr::{EqualityDeleteExpr, IcebergExprAdapterFactory};
use super::reader::{
    IcebergReaderFactory, NativeDeleteFileLoader, partitioned_file, preload_metadata,
    with_task_decryption,
};
use crate::read::file_planning::PlannedFileScan;
use crate::to_datafusion_error;

/// Applies Iceberg semantics around a configured native DataFusion Parquet source.
#[derive(Debug, Clone)]
pub struct IcebergFileSource {
    inner: ParquetSource,
    projection: ProjectionExprs,
    deletes: Arc<DeleteReadContext>,
    context: Arc<TaskContext>,
    reservation: Option<Arc<PlannedFileScan>>,
}

impl IcebergFileSource {
    /// Creates a native reader for a read schema; the supplied source must be unprojected.
    ///
    /// Reader options and any supplied file reader factory are retained. Without a
    /// factory, byte access is bridged to `file_io`. Output projection is applied
    /// through `FileScanConfig`, leaving delete keys available to the decoder.
    pub fn new(
        mut parquet: ParquetSource,
        schema: SchemaRef,
        file_io: FileIO,
        context: Arc<TaskContext>,
    ) -> Result<Self> {
        let table_schema = parquet.table_schema().table_schema();
        let identity = ProjectionExprs::from_indices(
            &(0..table_schema.fields().len()).collect::<Vec<_>>(),
            table_schema,
        );
        if parquet.projection() != Some(&identity) {
            return exec_err!(
                "Configure the Iceberg projection on FileScanConfig, not on the supplied ParquetSource"
            );
        }
        if parquet.parquet_file_reader_factory().is_none() {
            parquet = parquet.with_parquet_file_reader_factory(Arc::new(
                IcebergReaderFactory {
                    file_io: file_io.clone(),
                },
            ));
        }
        let loader = Arc::new(NativeDeleteFileLoader {
            context: context.clone(),
            source: parquet.clone(),
        });
        Ok(Self {
            inner: parquet,
            projection: identity,
            deletes: Arc::new(DeleteReadContext::new(loader, file_io, schema)),
            context,
            reservation: None,
        })
    }

    /// Builds a native scan from assigned tasks without catalog access or manifest discovery.
    ///
    /// Descriptors are charged to the construction context's memory pool. Tasks
    /// must not carry row predicates; the caller enforces query residuals.
    pub fn file_scan_config(
        &self,
        groups: Vec<Vec<FileScanTask>>,
    ) -> Result<FileScanConfig> {
        if groups.is_empty() {
            return exec_err!("Iceberg scan requires at least one file group");
        }
        let mut source = self.clone();
        let tasks = groups.iter().flatten().cloned().collect();
        source.reservation =
            Some(Arc::new(PlannedFileScan::from_tasks(tasks, &self.context)?));
        let groups = groups
            .into_iter()
            .map(|group| {
                group
                    .into_iter()
                    .map(|task| partitioned_file(Arc::new(task)))
                    .collect::<Result<Vec<_>>>()
                    .map(Into::into)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            Arc::new(source),
        )
        .with_file_groups(groups)
        .build())
    }

    fn rewrap(&self, source: Arc<dyn FileSource>) -> Arc<dyn FileSource> {
        let mut wrapper = self.clone();
        wrapper.inner = source
            .downcast_ref::<ParquetSource>()
            .expect("ParquetSource returns ParquetSource")
            .clone();
        Arc::new(wrapper)
    }
}

impl FileSource for IcebergFileSource {
    fn create_file_opener(
        &self,
        _: Arc<dyn ObjectStore>,
        _: &FileScanConfig,
        _: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        exec_err!("IcebergFileSource uses native Parquet morsels")
    }
    fn create_morselizer(
        &self,
        store: Arc<dyn ObjectStore>,
        config: &FileScanConfig,
        partition: usize,
    ) -> Result<Box<dyn Morselizer>> {
        if config.expr_adapter_factory.is_some() {
            return exec_err!(
                "IcebergFileSource requires its per-file Iceberg expression adapter"
            );
        }
        Ok(Box::new(IcebergMorselizer {
            source: self.clone(),
            store,
            config: config.clone(),
            partition,
        }))
    }
    fn table_schema(&self) -> &TableSchema {
        self.inner.table_schema()
    }
    fn projection(&self) -> Option<&ProjectionExprs> {
        Some(&self.projection)
    }
    fn filter(&self) -> Option<Arc<dyn PhysicalExpr>> {
        self.inner.filter()
    }
    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        self.inner.metrics()
    }
    fn file_type(&self) -> &str {
        "iceberg-parquet"
    }
    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        self.rewrap(self.inner.with_batch_size(batch_size))
    }
    fn fmt_extra(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        self.inner.fmt_extra(t, f)
    }
    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn FileSource>>> {
        let mut result = self.inner.try_pushdown_filters(filters, config)?;
        result.updated_node = result.updated_node.map(|source| self.rewrap(source));
        Ok(result)
    }
    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        let mut source = self.clone();
        source.projection = self.projection.try_merge(projection)?;
        Ok(Some(Arc::new(source)))
    }
    fn try_pushdown_sort(
        &self,
        order: &[PhysicalSortExpr],
        properties: &EquivalenceProperties,
    ) -> Result<SortOrderPushdownResult<Arc<dyn FileSource>>> {
        Ok(self
            .inner
            .try_pushdown_sort(order, properties)?
            .map(|source| self.rewrap(source)))
    }
    fn reorder_files(&self, files: Vec<PartitionedFile>) -> Vec<PartitionedFile> {
        self.inner.reorder_files(files)
    }
    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        if let Some(predicate) = self.filter()
            && f(&predicate)? == TreeNodeRecursion::Stop
        {
            return Ok(TreeNodeRecursion::Stop);
        }
        for expr in self.projection.iter() {
            if f(&expr.expr)? == TreeNodeRecursion::Stop {
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    }
}

#[derive(Debug, Clone)]
struct IcebergMorselizer {
    source: IcebergFileSource,
    store: Arc<dyn ObjectStore>,
    config: FileScanConfig,
    partition: usize,
}

impl Morselizer for IcebergMorselizer {
    fn plan_file(&self, file: PartitionedFile) -> Result<Box<dyn MorselPlanner>> {
        Ok(Box::new(PrepareFile {
            setup: self.clone(),
            file,
        }))
    }
}

#[derive(Debug)]
struct PrepareFile {
    setup: IcebergMorselizer,
    file: PartitionedFile,
}

impl MorselPlanner for PrepareFile {
    fn plan(self: Box<Self>) -> Result<Option<MorselPlan>> {
        Ok(Some(MorselPlan::new().with_pending_planner(async move {
            let Self { setup, mut file } = *self;
            let task = file
                .extensions
                .get::<Arc<FileScanTask>>()
                .cloned()
                .ok_or_else(|| {
                    datafusion::common::exec_datafusion_err!("Missing Iceberg file task")
                })?;
            let prepared = setup
                .source
                .deletes
                .prepare(&task)
                .await
                .map_err(to_datafusion_error)?;
            let mut source = with_task_decryption(setup.source.inner, &task)?;
            let split = task.start() != 0
                || (task.length() != 0 && task.length() != task.file_size_in_bytes());
            if split || (prepared.has_position_deletes() && task.record_count().is_none())
            {
                let (preloaded, metadata) =
                    preload_metadata(source, &file, setup.partition).await?;
                source = preloaded;
                if let Some(selection) = prepared
                    .row_selection(&task, &metadata)
                    .map_err(to_datafusion_error)?
                {
                    file.extensions.insert(ParquetRowSelection::new(selection));
                }
            } else if prepared.has_position_deletes() {
                let rows = task.record_count().ok_or_else(|| {
                    datafusion::common::exec_datafusion_err!(
                        "Position deletes require the whole-file record count"
                    )
                })?;
                if let Some(selection) = prepared
                    .position_selection(rows)
                    .map_err(to_datafusion_error)?
                {
                    file.extensions.insert(ParquetRowSelection::new(selection));
                }
            }
            let mut projection =
                setup.source.projection.iter().cloned().collect::<Vec<_>>();
            let mut config = setup.config;
            config.expr_adapter_factory =
                Some(Arc::new(IcebergExprAdapterFactory { task }));
            let equality = prepared.equality_filter().map_err(to_datafusion_error)?;
            let has_equality = equality.is_some();
            if let Some(equality) = equality {
                let matcher: Arc<dyn PhysicalExpr> = Arc::new(EqualityDeleteExpr::new(
                    equality,
                    source.table_schema().table_schema(),
                )?);
                let predicate = conjunction(
                    source
                        .filter()
                        .into_iter()
                        .chain(std::iter::once(matcher.clone())),
                );
                source = source.with_predicate(predicate);
                // Native row-filter construction is best effort. A projected keep mask
                // is mandatory and remains evaluated even if pushdown is disabled.
                projection.push(ProjectionExpr::new(matcher, "__iceberg_delete_keep"));
                config.limit = None;
            }
            let source = source
                .try_pushdown_projection(&ProjectionExprs::new(projection))?
                .ok_or_else(|| {
                    datafusion::common::exec_datafusion_err!(
                        "Native Parquet projection was rejected"
                    )
                })?;
            let planner = source
                .create_morselizer(setup.store, &config, setup.partition)?
                .plan_file(file)?;
            Ok(if has_equality {
                Box::new(DeleteGuardPlanner { inner: planner }) as Box<dyn MorselPlanner>
            } else {
                planner
            })
        })))
    }
}

#[derive(Debug)]
struct DeleteGuardPlanner {
    inner: Box<dyn MorselPlanner>,
}
impl MorselPlanner for DeleteGuardPlanner {
    fn plan(self: Box<Self>) -> Result<Option<MorselPlan>> {
        let Some(mut plan) = self.inner.plan()? else {
            return Ok(None);
        };
        let morsels = plan
            .take_morsels()
            .into_iter()
            .map(|inner| Box::new(DeleteGuardMorsel { inner }) as Box<dyn Morsel>)
            .collect();
        let planners = plan
            .take_ready_planners()
            .into_iter()
            .map(|inner| Box::new(Self { inner }) as Box<dyn MorselPlanner>)
            .collect();
        let pending = plan.take_pending_planner();
        let mut result = MorselPlan::new()
            .with_morsels(morsels)
            .with_planners(planners);
        if let Some(pending) = pending {
            result.set_pending_planner(async move {
                Ok(Box::new(Self {
                    inner: pending.await?,
                }) as Box<dyn MorselPlanner>)
            });
        }
        Ok(Some(result))
    }
}

#[derive(Debug)]
struct DeleteGuardMorsel {
    inner: Box<dyn Morsel>,
}
impl Morsel for DeleteGuardMorsel {
    fn into_stream(self: Box<Self>) -> BoxStream<'static, Result<RecordBatch>> {
        Box::pin(self.inner.into_stream().and_then(|batch| async move {
            let index = batch.num_columns().checked_sub(1).ok_or_else(|| {
                datafusion::common::exec_datafusion_err!("Missing mandatory delete mask")
            })?;
            let mask = batch
                .column(index)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    datafusion::common::exec_datafusion_err!(
                        "Invalid mandatory delete mask"
                    )
                })?;
            let filtered = filter_record_batch(&batch, mask)?;
            Ok(filtered.project(&(0..index).collect::<Vec<_>>())?)
        }))
    }
}

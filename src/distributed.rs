// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfig};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{Partitioning, PhysicalExpr, RangePartitioning};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties,
};
use datafusion_distributed::{
    DesiredTaskCountEvent, DesiredTaskCountEventResponse, DistributedLeafExec,
    ScaleUpLeafNodeEvent, ScaleUpLeafNodeEventResponse,
};

/// Preserves a range-partitioned child one-to-one while conservatively hiding
/// that contract from DFD's network boundary, which cannot scale ranges yet.
#[derive(Debug, Clone)]
pub(crate) struct IcebergRangeStageExec {
    input: Arc<dyn ExecutionPlan>,
    cache: Arc<PlanProperties>,
}

impl IcebergRangeStageExec {
    pub(crate) fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let cache = Arc::new(PlanProperties::new(
            input.equivalence_properties().clone(),
            Partitioning::UnknownPartitioning(input.output_partitioning().partition_count()),
            input.pipeline_behavior(),
            input.boundedness(),
        ));
        Self { input, cache }
    }
}

impl DisplayAs for IcebergRangeStageExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IcebergRangeStageExec")
    }
}

impl ExecutionPlan for IcebergRangeStageExec {
    fn name(&self) -> &str {
        "IcebergRangeStageExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
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

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = children.as_slice() else {
            return Err(DataFusionError::Internal(
                "IcebergRangeStageExec requires exactly one child".into(),
            ));
        };
        Ok(Arc::new(Self::new(Arc::clone(input))))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }
}

/// Assign one desired task per declared range bucket. DFD caps this at the
/// available worker count before invoking the scale-up handler below.
pub(crate) fn range_leaf_desired_tasks(
    event: DesiredTaskCountEvent<'_>,
) -> Option<Result<DesiredTaskCountEventResponse>> {
    range_file_scan(event.plan).map(|scan| {
        Ok(DesiredTaskCountEventResponse::desired(
            scan.file_groups.len().max(1),
        ))
    })
}

/// Stops DFD's built-in file-scan scaler from round-robining files across
/// declared Iceberg ranges. Each task receives a contiguous slice of range
/// buckets. Each worker variant retains the range ordering with one local
/// bucket, which allows `InterleaveExec` branches to remain compatible while
/// DFD maps the global buckets to task-specialized plans.
pub(crate) fn preserve_range_leaf(
    event: ScaleUpLeafNodeEvent<'_>,
) -> Option<Result<ScaleUpLeafNodeEventResponse>> {
    let Partitioning::Range(range) = event.plan.output_partitioning() else {
        return None;
    };
    let scan = range_file_scan(event.plan)?;
    let local_range = match RangePartitioning::try_new(range.ordering().clone(), vec![]) {
        Ok(range) => Partitioning::Range(range),
        Err(error) => return Some(Err(error)),
    };
    let task_count = event.task_count.max(1);
    let group_count = scan.file_groups.len();
    let mut variants = Vec::with_capacity(task_count);
    for task_index in 0..task_count {
        let start = task_index * group_count / task_count;
        let end = (task_index + 1) * group_count / task_count;
        let files = scan.file_groups[start..end]
            .iter()
            .flat_map(|group| group.iter().cloned())
            .collect();
        let mut task_scan = scan.clone();
        task_scan.file_groups = vec![FileGroup::new(files)];
        task_scan.output_partitioning = Some(local_range.clone());
        task_scan.preserve_order = true;
        variants.push(DataSourceExec::from_data_source(task_scan) as Arc<_>);
    }
    Some(
        DistributedLeafExec::try_new(Arc::clone(event.plan), variants)
            .map(|leaf| ScaleUpLeafNodeEventResponse::new(Arc::new(leaf))),
    )
}

fn range_file_scan(
    plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
) -> Option<&FileScanConfig> {
    if !matches!(plan.output_partitioning(), Partitioning::Range(_)) {
        return None;
    }
    plan.downcast_ref::<DataSourceExec>()?
        .data_source()
        .downcast_ref()
}

// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::Result;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfig};
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion_distributed::{
    DesiredTaskCountEvent, DesiredTaskCountEventResponse, DistributedLeafExec,
    ScaleUpLeafNodeEvent, ScaleUpLeafNodeEventResponse,
};

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
/// buckets and exposes a single ordinary partition to DFD's network layer.
/// The coordinator's native scan retains the exact range contract; only the
/// per-task transport variants erase it because DFD issue #628 does not yet
/// support carrying a global range through network nodes.
pub(crate) fn preserve_range_leaf(
    event: ScaleUpLeafNodeEvent<'_>,
) -> Option<Result<ScaleUpLeafNodeEventResponse>> {
    let scan = range_file_scan(event.plan)?;
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
        task_scan.output_partitioning = None;
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

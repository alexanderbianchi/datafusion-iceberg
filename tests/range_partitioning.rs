#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::common::Result;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::physical_expr::Partitioning;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion_distributed_iceberg::test_utils::IcebergTestHarness;

    #[tokio::test]
    async fn identity_partitioned_scan_declares_valid_ranges() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let plan = harness
            .physical_plan("SELECT vendor_id, pickup_date FROM taxi")
            .await?;
        let scan = find_scan(&plan).expect("native Parquet scan");
        let config = scan
            .data_source()
            .downcast_ref::<datafusion::datasource::physical_plan::FileScanConfig>()
            .expect("file scan config");

        let Partitioning::Range(range) = scan.properties().output_partitioning() else {
            panic!(
                "expected range-partitioned Iceberg leaf, got {}",
                scan.properties().output_partitioning()
            );
        };
        assert_eq!(range.partition_count(), 4);
        assert_eq!(
            range.ordering()[0].to_string(),
            "pickup_date@1 ASC NULLS LAST"
        );
        assert_eq!(
            range
                .split_points()
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            ["(2024-01-10)", "(2024-01-12)", "(2024-01-14)"]
        );
        let dates_by_group = config
            .file_groups
            .iter()
            .map(|group| {
                group
                    .files()
                    .iter()
                    .map(|file| {
                        file.path()
                            .as_ref()
                            .split("pickup_date=")
                            .nth(1)
                            .and_then(|suffix| suffix.split('/').next())
                            .expect("partition date in fixture path")
                            .to_string()
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            dates_by_group,
            [
                vec!["2024-01-08", "2024-01-09"],
                vec!["2024-01-10", "2024-01-11"],
                vec!["2024-01-12", "2024-01-13"],
                vec!["2024-01-14"],
            ]
        );
        Ok(())
    }

    fn find_scan(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<DataSourceExec>> {
        if let Some(scan) = plan.downcast_ref::<DataSourceExec>() {
            return Some(Arc::new(scan.clone()));
        }
        plan.children().into_iter().find_map(find_scan)
    }
}

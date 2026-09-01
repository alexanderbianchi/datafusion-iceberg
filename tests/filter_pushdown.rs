#[cfg(test)]
mod tests {
    use datafusion::error::Result;
    use datafusion_distributed_iceberg::test_utils::IcebergTestHarness;

    fn assert_native_parquet(plan: &str) {
        assert!(plan.contains("DataSourceExec:"), "{plan}");
        assert!(plan.contains("file_type=parquet"), "{plan}");
        assert!(!plan.contains("format=iceberg"), "{plan}");
    }

    #[tokio::test]
    async fn pushes_down_compound_predicates() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let (plan, batches) = harness
            .query(
                r"SELECT COUNT(*) AS trips
               FROM taxi
               WHERE pickup_date = DATE '2024-01-10'
                 AND payment_type IN (1, 2)
                 AND trip_distance >= 2.0",
            )
            .await?;

        assert_native_parquet(&plan);
        assert!(plan.contains("predicate=pickup_date"), "{plan}");
        assert!(plan.contains("payment_type"), "{plan}");
        assert!(plan.contains("trip_distance"), "{plan}");
        insta::assert_snapshot!(batches, @r"
    +-------+
    | trips |
    +-------+
    | 9891  |
    +-------+
    ");

        Ok(())
    }

    #[tokio::test]
    async fn pushes_down_disjunctions() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let (plan, batches) = harness
            .query(
                r"SELECT pickup_date, COUNT(*) AS trips
                   FROM taxi
                   WHERE pickup_date = DATE '2024-01-10'
                      OR pickup_date = DATE '2024-01-11'
                   GROUP BY pickup_date
                   ORDER BY pickup_date",
            )
            .await?;

        assert_native_parquet(&plan);
        assert!(plan.contains("predicate=pickup_date"), "{plan}");
        assert!(plan.contains("output_partitioning=Range"), "{plan}");
        insta::assert_snapshot!(batches, @r"
    +-------------+-------+
    | pickup_date | trips |
    +-------------+-------+
    | 2024-01-10  | 25000 |
    | 2024-01-11  | 25000 |
    +-------------+-------+
    ");

        Ok(())
    }

    #[tokio::test]
    async fn pushes_down_null_predicates() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let (plan, batches) = harness
            .query("SELECT COUNT(*) AS trips FROM taxi WHERE passenger_count IS NULL")
            .await?;

        assert_native_parquet(&plan);
        assert!(plan.contains("predicate=passenger_count"), "{plan}");
        insta::assert_snapshot!(batches, @r"
    +-------+
    | trips |
    +-------+
    | 8829  |
    +-------+
    ");

        Ok(())
    }

    #[tokio::test]
    async fn retains_unsupported_filter_after_safe_pushdown() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let (plan, batches) = harness
            .query(
                r"SELECT COUNT(*) AS trips
                   FROM taxi
                   WHERE pickup_date = DATE '2024-01-10'
                     AND trip_distance + 1.0 > 3.0",
            )
            .await?;

        assert_native_parquet(&plan);
        assert!(plan.contains("predicate=pickup_date"), "{plan}");
        assert!(plan.contains("trip_distance"), "{plan}");
        insta::assert_snapshot!(batches, @r"
    +-------+
    | trips |
    +-------+
    | 10516 |
    +-------+
    ");

        Ok(())
    }

    #[tokio::test]
    async fn executes_wholly_unsupported_filters_without_pushdown() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let (plan, batches) = harness
            .query("SELECT COUNT(*) AS trips FROM taxi WHERE trip_distance + 1.0 > 20.0")
            .await?;

        assert_native_parquet(&plan);
        assert!(plan.contains("predicate=trip_distance"), "{plan}");
        insta::assert_snapshot!(batches, @r"
    +-------+
    | trips |
    +-------+
    | 2757  |
    +-------+
    ");

        Ok(())
    }
}

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
    async fn applies_sql_limit_to_the_iceberg_scan_output() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let (plan, batches) = harness
            .query(
                r"SELECT vendor_id, pickup_at, passenger_count, trip_distance, pickup_date
               FROM taxi
               WHERE pickup_date = DATE '2024-01-10'
               ORDER BY pickup_at
               LIMIT 3",
            )
            .await?;

        assert_native_parquet(&plan);
        assert!(plan.contains("fetch=3"), "{plan}");
        assert!(
            plan.contains(
                "projection=[vendor_id, pickup_at, passenger_count, trip_distance, pickup_date]"
            ),
            "{plan}"
        );
        insta::assert_snapshot!(batches, @r"
    +-----------+---------------------+-----------------+---------------+-------------+
    | vendor_id | pickup_at           | passenger_count | trip_distance | pickup_date |
    +-----------+---------------------+-----------------+---------------+-------------+
    | 2         | 2024-01-10T00:00:09 |                 | 0.78          | 2024-01-10  |
    | 2         | 2024-01-10T00:00:10 | 1               | 0.88          | 2024-01-10  |
    | 1         | 2024-01-10T00:00:10 | 1               | 3.4           | 2024-01-10  |
    +-----------+---------------------+-----------------+---------------+-------------+
    ");

        Ok(())
    }

    #[tokio::test]
    async fn keeps_unordered_limits_above_the_iceberg_source() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let (plan, batches) = harness
            .query("SELECT pickup_date FROM taxi WHERE pickup_date = DATE '2024-01-10' LIMIT 3")
            .await?;

        assert_native_parquet(&plan);
        assert!(plan.contains("CoalescePartitionsExec: fetch=3"), "{plan}");
        assert!(
            plan.contains("FilterExec: pickup_date@0 = 2024-01-10, fetch=3"),
            "{plan}"
        );
        insta::assert_snapshot!(batches, @r"
    +-------------+
    | pickup_date |
    +-------------+
    | 2024-01-10  |
    | 2024-01-10  |
    | 2024-01-10  |
    +-------------+
    ");

        Ok(())
    }

    #[tokio::test]
    async fn passes_the_scan_limit_to_the_iceberg_source() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let (plan, batches) = harness
            .query("SELECT pickup_date FROM taxi LIMIT 3")
            .await?;

        assert_native_parquet(&plan);
        assert!(plan.contains("CoalescePartitionsExec: fetch=3"), "{plan}");
        assert!(plan.contains("projection=[pickup_date], limit=3"), "{plan}");
        insta::assert_snapshot!(batches, @r"
    +-------------+
    | pickup_date |
    +-------------+
    | 2024-01-08  |
    | 2024-01-08  |
    | 2024-01-08  |
    +-------------+
    ");

        Ok(())
    }

    #[tokio::test]
    async fn avoids_scanning_for_limit_zero() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let (plan, batches) = harness
            .query("SELECT vendor_id FROM taxi WHERE pickup_date = DATE '2024-01-10' LIMIT 0")
            .await?;

        insta::assert_snapshot!(plan, @r"
    EmptyExec
    ");
        insta::assert_snapshot!(batches, @r"
    ++
    ++
    ");

        Ok(())
    }

    #[tokio::test]
    async fn applies_offset_before_limit() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let (plan, batches) = harness
            .query(
                r"SELECT vendor_id, pickup_at
                   FROM taxi
                   WHERE pickup_date = DATE '2024-01-10'
                   ORDER BY pickup_at, vendor_id, pickup_location_id
                   LIMIT 2 OFFSET 3",
            )
            .await?;

        assert_native_parquet(&plan);
        assert!(plan.contains("GlobalLimitExec: skip=3, fetch=2"), "{plan}");
        assert!(plan.contains("fetch=5"), "{plan}");
        insta::assert_snapshot!(batches, @r"
    +-----------+---------------------+
    | vendor_id | pickup_at           |
    +-----------+---------------------+
    | 2         | 2024-01-10T00:00:12 |
    | 2         | 2024-01-10T00:00:14 |
    +-----------+---------------------+
    ");

        Ok(())
    }
}

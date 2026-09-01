#[cfg(test)]
mod tests {
    use datafusion::error::Result;
    use datafusion_distributed_iceberg::test_utils::IcebergTestHarness;

    fn assert_projection(plan: &str, projection: &str) {
        assert!(plan.contains("DataSourceExec:"), "{plan}");
        assert!(plan.contains("file_type=parquet"), "{plan}");
        assert!(plan.contains(projection), "{plan}");
        assert!(!plan.contains("format=iceberg"), "{plan}");
    }

    #[tokio::test]
    async fn projects_only_columns_required_by_the_query() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let (plan, batches) = harness
            .query(
                r"SELECT pickup_date, COUNT(*) AS trips
               FROM taxi
               WHERE pickup_date >= DATE '2024-01-10'
               GROUP BY pickup_date
               ORDER BY pickup_date",
            )
            .await?;

        assert_projection(&plan, "projection=[pickup_date]");
        assert!(plan.contains("predicate=pickup_date"), "{plan}");
        insta::assert_snapshot!(batches, @r"
    +-------------+-------+
    | pickup_date | trips |
    +-------------+-------+
    | 2024-01-10  | 25000 |
    | 2024-01-11  | 25000 |
    | 2024-01-12  | 25000 |
    | 2024-01-13  | 25000 |
    | 2024-01-14  | 25000 |
    +-------------+-------+
    ");

        Ok(())
    }

    #[tokio::test]
    async fn includes_filter_columns_that_are_not_in_the_output() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let (plan, batches) = harness
            .query(
                r"SELECT vendor_id, pickup_location_id
                   FROM taxi
                   WHERE pickup_date = DATE '2024-01-10'
                   ORDER BY pickup_at, vendor_id
                   LIMIT 3",
            )
            .await?;

        assert_projection(
            &plan,
            "projection=[vendor_id, pickup_at, pickup_location_id, pickup_date]",
        );
        insta::assert_snapshot!(batches, @r"
    +-----------+--------------------+
    | vendor_id | pickup_location_id |
    +-----------+--------------------+
    | 2         | 75                 |
    | 1         | 161                |
    | 2         | 162                |
    +-----------+--------------------+
    ");

        Ok(())
    }

    #[tokio::test]
    async fn projects_source_columns_for_computed_expressions() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let (plan, batches) = harness
            .query(
                r"SELECT MAX(trip_distance * fare_amount) AS max_weighted_fare
                   FROM taxi
                   WHERE pickup_date = DATE '2024-01-10'",
            )
            .await?;

        assert_projection(
            &plan,
            "projection=[trip_distance, fare_amount, pickup_date]",
        );
        insta::assert_snapshot!(batches, @r"
    +-------------------+
    | max_weighted_fare |
    +-------------------+
    | 207508.9584       |
    +-------------------+
    ");

        Ok(())
    }

    #[tokio::test]
    async fn reads_all_columns_when_selecting_star() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let (plan, batches) = harness
            .query(
                r"SELECT *
                   FROM taxi
                   WHERE pickup_date = DATE '2024-01-10'
                   ORDER BY pickup_at, vendor_id, pickup_location_id
                   LIMIT 1",
            )
            .await?;

        assert_projection(
            &plan,
            "projection=[vendor_id, pickup_at, dropoff_at, passenger_count, trip_distance, pickup_location_id, dropoff_location_id, payment_type, fare_amount, tip_amount, tolls_amount, total_amount, pickup_date]",
        );
        insta::assert_snapshot!(batches, @r"
    +-----------+---------------------+---------------------+-----------------+---------------+--------------------+---------------------+--------------+-------------+------------+--------------+--------------+-------------+
    | vendor_id | pickup_at           | dropoff_at          | passenger_count | trip_distance | pickup_location_id | dropoff_location_id | payment_type | fare_amount | tip_amount | tolls_amount | total_amount | pickup_date |
    +-----------+---------------------+---------------------+-----------------+---------------+--------------------+---------------------+--------------+-------------+------------+--------------+--------------+-------------+
    | 2         | 2024-01-10T00:00:09 | 2024-01-10T00:03:30 |                 | 0.78          | 75                 | 236                 | 0            | 1.74        | 3.15       | 0.0          | 8.89         | 2024-01-10  |
    +-----------+---------------------+---------------------+-----------------+---------------+--------------------+---------------------+--------------+-------------+------------+--------------+--------------+-------------+
    ");

        Ok(())
    }
}

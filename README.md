# DataFusion Iceberg

Native Apache Iceberg tables for DataFusion and DataFusion Distributed 4.x,
built against DataFusion 55.

This project was extracted from DataFusion Distributed's isolated Iceberg
crate and implements a Sail-inspired physical plan:

```text
Iceberg metadata and manifests
  -> native DataFusion Parquet DataSourceExec leaves
  -> merge-on-read delete application
  -> partition-aware range planning
  -> DataFusion Distributed execution
```

Parquet decoding stays visible to DataFusion and DataFusion Distributed rather
than being hidden behind `iceberg-rust`'s `ArrowReader`.

## Capabilities

- Native DataFusion `DataSourceExec` Parquet leaves with projection, predicate,
  limit, pruning, and statistics pushdown.
- Merge-on-read for Iceberg position deletes and equality deletes. Delete files
  are applied before user filters, preserving Iceberg row-position semantics.
- Exact DataFusion range partitioning for tables with one identity partition
  field, with deterministic contiguous file groups and range boundaries. Mixed
  clean and merge-on-read data files share one global layout and are combined
  with DataFusion's `InterleaveExec` without losing the range contract.
- DataFusion Distributed 4.x execution. A custom codec transports merge-on-read
  plans to workers, and range buckets are assigned contiguously rather than
  round-robin.
- `INSERT INTO` append writes using Iceberg's native Parquet writers and atomic
  transaction commit, including fan-out into partition directories.
- Local files, S3, and GCS through the default storage factory, with an extension
  point for custom Iceberg storage.

The integration currently targets DataFusion 55, DataFusion Distributed 4.x,
and iceberg-rust 0.10.

```rust
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use datafusion_distributed_iceberg::{IcebergExt, IcebergIntegrationOptions};

async fn example() -> datafusion::error::Result<()> {
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_iceberg_integration(IcebergIntegrationOptions::default())
        .build();
    let ctx = SessionContext::new_with_state(state);

    ctx.sql(
        "CREATE EXTERNAL TABLE taxi STORED AS ICEBERG \
     LOCATION 's3://warehouse/taxi/metadata/v1.metadata.json'",
    )
        .await?
        .collect()
        .await?;
    Ok(())
}
```

The default storage factory resolves `file://`, S3 (`s3://`, `s3a://`,
`s3n://`), and GCS (`gs://`, `gcs://`) URIs. Use
`IcebergIntegrationOptions` to supply custom storage or an Iceberg runtime.

## Current limits

- Range partitioning currently requires exactly one identity partition field.
  Spec evolution is rejected when it would make a scan's range contract
  ambiguous.
- DataFusion Distributed does not yet preserve a global range contract on its
  network nodes ([DFD #628](https://github.com/datafusion-contrib/datafusion-distributed/issues/628)).
  This crate preserves the exact range contract through `InterleaveExec` inside
  the scan stage and gives each worker a contiguous set of range buckets. A
  pass-through stage wrapper conservatively forgets the range only at DFD's
  network boundary; it does not repartition or shuffle rows.
- `UPDATE`, `DELETE`, and `MERGE INTO` mutation planning are not implemented.
  Merge-on-read consumes delete files already present in a table, while writes
  currently support append inserts.

```bash
cargo test
```

## License and provenance

Licensed under Apache-2.0. The initial integration was extracted from
[`datafusion-contrib/datafusion-distributed`](https://github.com/datafusion-contrib/datafusion-distributed).
Algorithms and implementation portions derived from
[`lakehq/sail`](https://github.com/lakehq/sail) carry file-level provenance
headers naming the original source path and revision. See `NOTICE`.

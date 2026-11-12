<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# Native Parquet read experiment

This branch builds on the retained-task planning API in PR #67. It adds an
opt-in reader; the existing lazy and eager ArrowReader paths remain the default.
The base is `1d5fe37514cc`; Cargo pins the companion Iceberg branch at
`48064cb87d71c0d8c39f1a62df198d3e3aab8fc1`, so no sibling checkout is required.

```rust
use datafusion::prelude::{SessionConfig, SessionContext};
use iceberg_datafusion::IcebergPlanningConfig;

let config = SessionConfig::new()
    .with_option_extension(IcebergPlanningConfig::default())
    .set_bool("iceberg.native_parquet_reader", true)
    .set_bool("datafusion.execution.parquet.pushdown_filters", true);
let context = SessionContext::new_with_config(config);
// Register an Iceberg catalog or static table provider with this context.
```

`native_parquet_reader` implies eager task discovery. Both static and catalog
providers use it. It does not change how manifest pruning or delete applicability
is planned. Physical plans identify the reader as `format=iceberg-parquet`.

## Execution contract

DataFusion's `ParquetSource` decodes **both data and Parquet delete files**.
The default `ParquetFileReaderFactory` bridges byte access to the table's
`FileIO`; supplying another factory replaces that byte access for both kinds of
file. Puffin deletion vectors are still parsed by Iceberg.

Iceberg's `FileReadBinding` resolves field IDs, name mappings, promotions,
initial defaults and partition constants against the native decoder's schema.
The connector uses those bindings in DataFusion expressions before row filtering
and output projection. It does not repair defaults after filtering.

Late DataFusion filters remain `PhysicalExpr` trees, including shared
`DynamicFilterPhysicalExpr` state. Their column children are rebound for each
file, rather than taking a one-time snapshot of the predicate. Static query
filters are also converted for Iceberg manifest pruning; the original
DataFusion predicates and their residual filters are retained. With a row
selection installed, DataFusion 55.1 disables runtime row-group pruning; live
decoder row filtering still works and is tested here.

A scan-shared `DeleteReadContext` loads applicable deletes through the supplied
backend and reuses successful loads across files and partitions. Failed or
cancelled loads can be retried. Position selections use whole-file row numbers;
byte splits retain Iceberg's row-group midpoint ownership. For splits, metadata
is loaded through the native reader factory and handed to the same reader for
execution, avoiding a second footer read.

Equality-delete expressions participate in native row filtering when supported.
Because DataFusion's row-filter construction is best effort, a mandatory keep
mask is also evaluated by the native projection, filtered at the morsel boundary,
and removed before output. Hidden equality keys remain available until this
check. Inner decoder limits are disabled for these files; the outer limit sees
only surviving rows. Disabling query predicate pushdown does not disable deletes.

## Assigned tasks and reader configuration

`IcebergFileSource::new(parquet_source, read_schema, file_io, task_context)`
accepts an unprojected, configured `ParquetSource`. Its
`file_scan_config(Vec<Vec<FileScanTask>>)` method reconstructs a scan without a
catalog or manifest discovery. Apply the output projection and limit with
`FileScanConfigBuilder`. Each `PartitionedFile` carries its `Arc<FileScanTask>` in
its extensions, including the original URI and key metadata. The source owns its
per-file expression adapter; do not replace it with a generic schema adapter.

Tasks with Iceberg row predicates are rejected at this boundary. When a caller
independently enforces the original DataFusion predicates, it can explicitly
remove the pruning copy with `FileScanTask::without_predicate()`. This does not
remove deletes. The provider integration performs this step while retaining the
DataFusion filters.

Parquet modular encryption uses each task's key metadata for both data and
delete files. Non-Parquet data files, nested equality keys unsupported by the
existing Arrow evaluator, and row-lineage bindings are not silently downgraded
to the old decoder. They return errors. This experiment does not make a broader
nested-schema evolution guarantee than the reused Arrow transformation rules.

## Reproduce

```bash
cargo +1.99.1 test --locked -p iceberg-datafusion --test native_parquet
```

The tests compare provider queries with the existing reader and exercise native
reader-factory routing for data and deletes, shared preparation, hidden keys,
per-file applicability, empty delete files, null equality keys, rename/reorder/promotion/defaults,
position deletes with limits, disabled query pushdown, encrypted Parquet, and
byte splits against the existing reader, including further native repartitioning.
The schema-evolution integration test also runs the native backend against a
provider whose schema predates a newly added column. The live-filter test reads a batch,
updates the native dynamic predicate, and checks the remaining rows from that
already-open file, with positional deletes present.

The companion Iceberg branch has binding and cache retry/cancellation tests;
its existing ArrowReader remains covered by the original reader tests.

Local validation used Rust 1.99.1: the connector workspace tests and strict
workspace Clippy passed; Iceberg's library tests passed (1,889 passed, one
ignored), and its public API files were regenerated. Strict Iceberg Clippy still
reports existing base-code lints under this toolchain, including `async_trait`
`double_must_use` diagnostics and test-style lints. Those unrelated files were
not changed for this experiment.

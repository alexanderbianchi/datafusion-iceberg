#![allow(clippy::disallowed_types)]

//! Native Apache Iceberg integration for DataFusion and DataFusion Distributed.
//!
//! Iceberg metadata planning is eager on the coordinator and produces native
//! DataFusion Parquet scan leaves. This keeps Parquet execution visible to
//! DataFusion's optimizer and to DataFusion Distributed's standard codecs.

mod codec;
mod common;
mod config;
mod data_source;
mod delete_apply;
mod distributed;
mod iceberg_ext;
mod insert;
mod schema;
mod table_provider;
#[doc(hidden)]
pub mod test_utils;

pub use config::IcebergConfig;
pub use iceberg_ext::IcebergExt;
pub use iceberg_ext::IcebergIntegrationOptions;
pub use table_provider::IcebergCatalogTableProvider;
pub use table_provider::IcebergStaticTableProvider;
pub use table_provider::IcebergTableProviderFactory;

// re-export of iceberg-rust.
pub use iceberg;

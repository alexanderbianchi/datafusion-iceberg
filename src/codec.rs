// Licensed under the Apache License, Version 2.0.
//
// Portions derived from LakeSail:
// https://github.com/lakehq/sail
// Original file: crates/sail-iceberg/src/physical_plan/delete_apply_exec.rs
// Source revision: 1a449ec44fabc53ee38f19661b5dbe47e45aa676
//
// Serialization glue added for DataFusion Distributed workers.

use std::sync::Arc;

use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::{PhysicalExtensionCodec, PhysicalProtoConverterExtension};
use datafusion_proto::protobuf::proto_error;
use iceberg::scan::FileScanTaskDeleteFile;
use iceberg::spec::DataContentType;
use prost::Message;

const DELETE_APPLY_KIND: &str = "datafusion-iceberg/delete-apply/v1";
const RANGE_STAGE_KIND: &str = "datafusion-iceberg/range-stage/v1";

#[derive(Debug)]
pub(crate) struct IcebergCodec;

#[derive(Clone, PartialEq, Message)]
struct DeleteApplyProto {
    #[prost(string, tag = "1")]
    kind: String,
    #[prost(string, tag = "2")]
    data_file_path: String,
    #[prost(message, repeated, tag = "3")]
    positional_deletes: Vec<DeleteFileProto>,
    #[prost(message, repeated, tag = "4")]
    equality_deletes: Vec<DeleteFileProto>,
    #[prost(string, tag = "5")]
    table_url: String,
    #[prost(string, tag = "6")]
    iceberg_schema_json: String,
}

#[derive(Clone, PartialEq, Message)]
struct NodeKindProto {
    #[prost(string, tag = "1")]
    kind: String,
}

#[derive(Clone, PartialEq, Message)]
struct DeleteFileProto {
    #[prost(string, tag = "1")]
    file_path: String,
    #[prost(uint64, tag = "2")]
    file_size_in_bytes: u64,
    #[prost(int32, tag = "3")]
    file_type: i32,
    #[prost(int32, tag = "4")]
    partition_spec_id: i32,
    #[prost(int32, repeated, tag = "5")]
    equality_ids: Vec<i32>,
    #[prost(bool, tag = "6")]
    has_equality_ids: bool,
}

impl From<&FileScanTaskDeleteFile> for DeleteFileProto {
    fn from(file: &FileScanTaskDeleteFile) -> Self {
        Self {
            file_path: file.file_path.clone(),
            file_size_in_bytes: file.file_size_in_bytes,
            file_type: file.file_type as i32,
            partition_spec_id: file.partition_spec_id,
            equality_ids: file.equality_ids.clone().unwrap_or_default(),
            has_equality_ids: file.equality_ids.is_some(),
        }
    }
}

impl TryFrom<DeleteFileProto> for FileScanTaskDeleteFile {
    type Error = datafusion::common::DataFusionError;

    fn try_from(file: DeleteFileProto) -> Result<Self> {
        Ok(FileScanTaskDeleteFile::builder()
            .with_file_path(file.file_path)
            .with_file_size_in_bytes(file.file_size_in_bytes)
            .with_file_type(DataContentType::try_from(file.file_type).map_err(external_error)?)
            .with_partition_spec_id(file.partition_spec_id)
            .with_equality_ids(file.has_equality_ids.then_some(file.equality_ids))
            .build())
    }
}

impl PhysicalExtensionCodec for IcebergCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let node_kind =
            NodeKindProto::decode(buf).map_err(|error| proto_error(error.to_string()))?;
        let [input] = inputs else {
            return Err(proto_error(format!(
                "{} expects one child, got {}",
                node_kind.kind,
                inputs.len()
            )));
        };
        match node_kind.kind.as_str() {
            DELETE_APPLY_KIND => {
                let proto = DeleteApplyProto::decode(buf)
                    .map_err(|error| proto_error(error.to_string()))?;
                let schema: iceberg::spec::Schema =
                    serde_json::from_str(&proto.iceberg_schema_json)
                        .map_err(|error| proto_error(error.to_string()))?;
                Ok(Arc::new(crate::delete_apply::IcebergDeleteApplyExec::new(
                    Arc::clone(input),
                    proto.data_file_path,
                    proto
                        .positional_deletes
                        .into_iter()
                        .map(TryInto::try_into)
                        .collect::<Result<_>>()?,
                    proto
                        .equality_deletes
                        .into_iter()
                        .map(TryInto::try_into)
                        .collect::<Result<_>>()?,
                    proto.table_url,
                    Arc::new(schema),
                )))
            }
            RANGE_STAGE_KIND => Ok(Arc::new(crate::distributed::IcebergRangeStageExec::new(
                Arc::clone(input),
            ))),
            _ => Err(proto_error("not a datafusion-iceberg extension node")),
        }
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        if let Some(node) = node.downcast_ref::<crate::delete_apply::IcebergDeleteApplyExec>() {
            return DeleteApplyProto {
                kind: DELETE_APPLY_KIND.to_string(),
                data_file_path: node.data_file_path().to_string(),
                positional_deletes: node.positional_deletes().iter().map(Into::into).collect(),
                equality_deletes: node.equality_deletes().iter().map(Into::into).collect(),
                table_url: node.table_url().to_string(),
                iceberg_schema_json: serde_json::to_string(node.iceberg_schema().as_ref())
                    .map_err(|error| proto_error(error.to_string()))?,
            }
            .encode(buf)
            .map_err(|error| proto_error(error.to_string()));
        }
        if node.is::<crate::distributed::IcebergRangeStageExec>() {
            return NodeKindProto {
                kind: RANGE_STAGE_KIND.to_string(),
            }
            .encode(buf)
            .map_err(|error| proto_error(error.to_string()));
        }
        Err(proto_error(format!(
            "not a datafusion-iceberg extension node: {}",
            node.name()
        )))
    }
}

fn external_error(
    error: impl std::error::Error + Send + Sync + 'static,
) -> datafusion::common::DataFusionError {
    datafusion::common::DataFusionError::External(Box::new(error))
}

// Licensed under the Apache License, Version 2.0.
//
// Portions derived from Apache Iceberg Rust:
// https://github.com/apache/iceberg-rust
// Original file: crates/iceberg/src/arrow/schema.rs
// Source version: 0.10.1
//
// Modified to produce the Arrow 59 types used by DataFusion 55 without
// crossing iceberg-rust's Arrow 58 ABI boundary.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema, TimeUnit};
use datafusion::common::{DataFusionError, Result};
use iceberg::spec::{MapType, NestedField, PrimitiveType, StructType, Type};

const FIELD_ID_KEY: &str = "PARQUET:field_id";
const DOC_KEY: &str = "doc";
const UTC: &str = "+00:00";

pub(crate) fn iceberg_schema_to_arrow(schema: &iceberg::spec::Schema) -> Result<Schema> {
    let fields = schema
        .as_struct()
        .fields()
        .iter()
        .map(iceberg_field_to_arrow)
        .collect::<Result<Vec<_>>>()?;
    Ok(Schema::new(fields))
}

fn iceberg_field_to_arrow(field: &Arc<NestedField>) -> Result<FieldRef> {
    let mut metadata = HashMap::from([(FIELD_ID_KEY.to_string(), field.id.to_string())]);
    if let Some(doc) = &field.doc {
        metadata.insert(DOC_KEY.to_string(), doc.clone());
    }

    Ok(Arc::new(
        Field::new(
            field.name.clone(),
            iceberg_type_to_arrow(field.field_type.as_ref())?,
            !field.required,
        )
        .with_metadata(metadata),
    ))
}

fn iceberg_type_to_arrow(ty: &Type) -> Result<DataType> {
    match ty {
        Type::Primitive(primitive) => primitive_to_arrow(primitive),
        Type::Struct(StructType { .. }) => {
            let Type::Struct(struct_type) = ty else {
                unreachable!()
            };
            let fields = struct_type
                .fields()
                .iter()
                .map(iceberg_field_to_arrow)
                .collect::<Result<Vec<_>>>()?;
            Ok(DataType::Struct(fields.into()))
        }
        Type::List(list) => Ok(DataType::List(iceberg_field_to_arrow(&list.element_field)?)),
        Type::Map(MapType {
            key_field,
            value_field,
        }) => {
            let entries = Field::new(
                "key_value",
                DataType::Struct(
                    vec![
                        iceberg_field_to_arrow(key_field)?,
                        iceberg_field_to_arrow(value_field)?,
                    ]
                    .into(),
                ),
                false,
            );
            Ok(DataType::Map(Arc::new(entries), false))
        }
    }
}

fn primitive_to_arrow(primitive: &PrimitiveType) -> Result<DataType> {
    let ty = match primitive {
        PrimitiveType::Boolean => DataType::Boolean,
        PrimitiveType::Int => DataType::Int32,
        PrimitiveType::Long => DataType::Int64,
        PrimitiveType::Float => DataType::Float32,
        PrimitiveType::Double => DataType::Float64,
        PrimitiveType::Decimal { precision, scale } => DataType::Decimal128(
            u8::try_from(*precision).map_err(external_error)?,
            i8::try_from(*scale).map_err(external_error)?,
        ),
        PrimitiveType::Date => DataType::Date32,
        PrimitiveType::Time => DataType::Time64(TimeUnit::Microsecond),
        PrimitiveType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
        PrimitiveType::Timestamptz => DataType::Timestamp(TimeUnit::Microsecond, Some(UTC.into())),
        PrimitiveType::TimestampNs => DataType::Timestamp(TimeUnit::Nanosecond, None),
        PrimitiveType::TimestamptzNs => DataType::Timestamp(TimeUnit::Nanosecond, Some(UTC.into())),
        PrimitiveType::String => DataType::Utf8,
        PrimitiveType::Uuid => DataType::FixedSizeBinary(16),
        PrimitiveType::Fixed(length) => i32::try_from(*length)
            .ok()
            .map(DataType::FixedSizeBinary)
            .unwrap_or(DataType::LargeBinary),
        PrimitiveType::Binary => DataType::LargeBinary,
    };
    Ok(ty)
}

fn external_error(error: impl std::error::Error + Send + Sync + 'static) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

pub(crate) fn reorder_identity_partitions_last(
    schema: &Schema,
    iceberg_schema: &iceberg::spec::Schema,
    partition_spec: &iceberg::spec::PartitionSpec,
) -> Schema {
    let identity_names = partition_spec
        .fields()
        .iter()
        .filter(|field| field.transform == iceberg::spec::Transform::Identity)
        .filter_map(|field| iceberg_schema.field_by_id(field.source_id))
        .map(|field| field.name.as_str())
        .collect::<Vec<_>>();

    let mut fields = schema
        .fields()
        .iter()
        .filter(|field| !identity_names.contains(&field.name().as_str()))
        .cloned()
        .collect::<Vec<_>>();
    fields.extend(identity_names.into_iter().filter_map(|name| {
        schema
            .fields()
            .iter()
            .find(|field| field.name() == name)
            .cloned()
    }));
    Schema::new_with_metadata(fields, schema.metadata().clone())
}

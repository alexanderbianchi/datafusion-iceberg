// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Result, ScalarValue, exec_err};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{CastExpr, Column, Literal};
use datafusion::physical_expr_adapter::{
    PhysicalExprAdapter, PhysicalExprAdapterFactory,
};
use iceberg::arrow::{EqualityDeleteFilter, FileReadBinding};
use iceberg::scan::FileScanTask;

use crate::to_datafusion_error;

#[derive(Debug)]
pub(super) struct IcebergExprAdapterFactory {
    pub task: Arc<FileScanTask>,
}

impl PhysicalExprAdapterFactory for IcebergExprAdapterFactory {
    fn create(
        &self,
        logical: SchemaRef,
        physical: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExprAdapter>> {
        Ok(Arc::new(IcebergExprAdapter {
            logical,
            physical: physical.clone(),
            binding: FileReadBinding::new(self.task.clone(), physical)
                .map_err(to_datafusion_error)?,
        }))
    }
}

#[derive(Debug)]
struct IcebergExprAdapter {
    logical: SchemaRef,
    physical: SchemaRef,
    binding: FileReadBinding,
}

impl PhysicalExprAdapter for IcebergExprAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        expr.transform_up(|expr| {
            let Some(column) = expr.downcast_ref::<Column>() else {
                return Ok(Transformed::no(expr));
            };
            let field = self.logical.fields().get(column.index()).ok_or_else(|| {
                datafusion::common::exec_datafusion_err!(
                    "Invalid Iceberg column index {}",
                    column.index()
                )
            })?;
            let id = field
                .metadata()
                .get(parquet::arrow::PARQUET_FIELD_ID_META_KEY)
                .and_then(|id| id.parse().ok())
                .ok_or_else(|| {
                    datafusion::common::exec_datafusion_err!(
                        "Missing Iceberg ID for {}",
                        field.name()
                    )
                })?;
            let bound = self.binding.field(id).map_err(to_datafusion_error)?;
            let mut rewritten: Arc<dyn PhysicalExpr> =
                if let Some(index) = bound.source_index() {
                    Arc::new(Column::new(self.physical.field(index).name(), index))
                } else if let Some(constant) = bound.constant() {
                    let constant =
                        datafusion::arrow::compute::cast(constant, field.data_type())?;
                    Arc::new(Literal::new(ScalarValue::try_from_array(&constant, 0)?))
                } else {
                    return exec_err!(
                        "Iceberg field {id} has no physical source or constant"
                    );
                };
            if rewritten.data_type(&self.physical)? != *field.data_type() {
                rewritten =
                    Arc::new(CastExpr::new(rewritten, field.data_type().clone(), None));
            }
            Ok(Transformed::yes(rewritten))
        })
        .map(|result| result.data)
    }
}

#[derive(Debug)]
pub(super) struct EqualityDeleteExpr {
    filter: Arc<EqualityDeleteFilter>,
    children: Vec<Arc<dyn PhysicalExpr>>,
}

impl EqualityDeleteExpr {
    pub fn new(filter: EqualityDeleteFilter, schema: &Schema) -> Result<Self> {
        let children = filter
            .field_ids()
            .iter()
            .map(|id| {
                let index = schema
                    .fields()
                    .iter()
                    .position(|field| {
                        field
                            .metadata()
                            .get(parquet::arrow::PARQUET_FIELD_ID_META_KEY)
                            .and_then(|v| v.parse::<i32>().ok())
                            == Some(*id)
                    })
                    .ok_or_else(|| {
                        datafusion::common::exec_datafusion_err!(
                            "Equality key {id} is absent from the read schema"
                        )
                    })?;
                Ok(Arc::new(Column::new(schema.field(index).name(), index))
                    as Arc<dyn PhysicalExpr>)
            })
            .collect::<Result<_>>()?;
        Ok(Self {
            filter: Arc::new(filter),
            children,
        })
    }
}

impl PartialEq for EqualityDeleteExpr {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.filter, &other.filter) && self.children == other.children
    }
}
impl Eq for EqualityDeleteExpr {}
impl Hash for EqualityDeleteExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.filter).hash(state);
        self.children.hash(state);
    }
}
impl fmt::Display for EqualityDeleteExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "iceberg_not_deleted({:?})", self.filter.field_ids())
    }
}
impl PhysicalExpr for EqualityDeleteExpr {
    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
    fn data_type(&self, _: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }
    fn nullable(&self, _: &Schema) -> Result<bool> {
        Ok(false)
    }
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.children.iter().collect()
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if children.len() != self.children.len() {
            return exec_err!("Invalid equality-delete child count");
        }
        Ok(Arc::new(Self {
            filter: self.filter.clone(),
            children,
        }))
    }
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let arrays = self
            .children
            .iter()
            .zip(self.filter.schema().fields())
            .map(|(expr, field)| {
                let array = expr.evaluate(batch)?.into_array(batch.num_rows())?;
                Ok(datafusion::arrow::compute::cast(&array, field.data_type())?)
            })
            .collect::<Result<Vec<ArrayRef>>>()?;
        let options = datafusion::arrow::array::RecordBatchOptions::new()
            .with_row_count(Some(batch.num_rows()));
        let keys = RecordBatch::try_new_with_options(
            self.filter.schema().clone(),
            arrays,
            &options,
        )?;
        let mask = self.filter.evaluate(keys).map_err(to_datafusion_error)?;
        Ok(ColumnarValue::Array(Arc::new(mask)))
    }
}

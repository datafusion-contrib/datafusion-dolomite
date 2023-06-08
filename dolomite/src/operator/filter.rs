use crate::error::DolomiteResult;
use crate::operator::{
    DerivePropContext, DerivePropResult, DisplayFields, OperatorTrait,
    PhysicalOperatorTrait,
};
use crate::optimizer::{OptExpr, OptGroup, Optimizer};
use crate::properties::LogicalProperty;
use datafusion::prelude::{Column, Expr};
use datafusion_common::DFSchema;
use itertools::Itertools;
use std::collections::HashMap;
use std::fmt::Formatter;

#[derive(Clone, Debug, Hash, PartialEq)]
pub struct Filter {
    filter: Expr,
    projected_columns: Vec<Column>,
}

impl Filter {
    pub fn new(filter: Expr, projected_columns: Vec<Column>) -> Self {
        Self {
            filter,
            projected_columns,
        }
    }
}

impl PhysicalOperatorTrait for Filter {
    fn derive_properties<O: Optimizer>(
        &self,
        _context: DerivePropContext<O>,
    ) -> DolomiteResult<Vec<DerivePropResult>> {
        todo!()
    }
}

impl OperatorTrait for Filter {
    fn derive_logical_prop<O: Optimizer>(
        &self,
        handle: O::ExprHandle,
        optimizer: &O,
    ) -> DolomiteResult<LogicalProperty> {
        let input_group_handle = optimizer.expr_at(handle).input_at(0, optimizer);
        let input_group = optimizer.group_at(input_group_handle);
        let input_schema = input_group.logical_prop().schema();
        let fields = self
            .projected_columns
            .iter()
            .map(|c| input_schema.index_of_column(c))
            .map(|idx_result| idx_result.map(|idx| input_schema.field(idx).clone()))
            .try_collect()?;

        let schema = DFSchema::new_with_metadata(fields, HashMap::new())?;
        Ok(LogicalProperty::new(schema))
    }
}

impl DisplayFields for Filter {
    fn display(&self, fmt: &mut Formatter) -> std::fmt::Result {
        fmt.debug_struct("")
            .field("filter", &self.filter)
            .field("projected_columns", &self.projected_columns)
            .finish()
    }
}

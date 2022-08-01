use datafusion::common::DFField;
use datafusion::logical_expr::ExprSchemable;
use datafusion::logical_plan::{DFSchema, Expr};

use crate::error::{DFResult, OptResult};
use crate::operator::{
    DerivePropContext, DerivePropResult, OperatorTrait, PhysicalOperatorTrait,
};
use crate::optimizer::{OptExpr, OptGroup, Optimizer};
use crate::properties::LogicalProperty;

#[derive(Clone, Debug, Hash, PartialEq)]
pub struct Projection {
    expr: Vec<Expr>,
}

impl Projection {
    pub fn new<I: IntoIterator<Item = Expr>>(exprs: I) -> Self {
        Self {
            expr: exprs.into_iter().collect(),
        }
    }

    pub fn expr(&self) -> &[Expr] {
        &self.expr
    }
}

impl PhysicalOperatorTrait for Projection {
    fn derive_properties<O: Optimizer>(
        &self,
        _context: DerivePropContext<O>,
    ) -> OptResult<Vec<DerivePropResult>> {
        todo!()
    }
}

impl OperatorTrait for Projection {
    fn derive_logical_prop<O: Optimizer>(
        &self,
        handle: O::ExprHandle,
        optimizer: &O,
    ) -> OptResult<LogicalProperty> {
        let input_logical_prop = optimizer
            .group_at(optimizer.expr_at(handle).input_at(0, optimizer))
            .logical_prop();
        let input_schema = input_logical_prop.schema();
        let schema = DFSchema::new_with_metadata(
            self.expr
                .iter()
                .map(|e| e.to_field(input_schema))
                .collect::<DFResult<Vec<DFField>>>()?,
            input_logical_prop.schema().metadata().clone(),
        )?;

        Ok(LogicalProperty::new(schema))
    }
}

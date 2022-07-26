use datafusion::logical_plan::{DFSchema, Expr, exprlist_to_fields};

use crate::cost::Cost;
use crate::error::OptResult;
use crate::operator::{DerivePropContext, DerivePropResult, OperatorTrait, PhysicalOperatorTrait};
use crate::optimizer::{OptExpr, OptGroup, Optimizer};
use crate::properties::LogicalProperty;

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct Projection {
  expr: Vec<Expr>,
}

impl Projection {
  pub fn new<I: IntoIterator<Item=Expr>>(exprs: I) -> Self {
    Self { expr: exprs.into_iter().collect() }
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

  fn cost<O: Optimizer>(&self, _expr_handle: O::ExprHandle, _optimizer: &O) -> OptResult<Cost> {
    todo!()
  }
}

impl OperatorTrait for Projection {
  fn derive_logical_prop<O: Optimizer>(&self, handle: O::ExprHandle,
                                       optimizer: &O) -> OptResult<LogicalProperty> {
    let input_logical_prop = optimizer.group_at(optimizer.expr_at(handle.clone()).input_at(0,
                                                                                           optimizer)).logical_prop();
    let schema = DFSchema::new_with_metadata(exprlist_to_fields(&self.expr, input_logical_prop
        .schema())?, input_logical_prop.schema().metadata().clone())?;

    Ok(LogicalProperty::new(schema))
  }
}

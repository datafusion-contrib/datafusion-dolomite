use crate::error::OptResult;
use crate::operator::OperatorTrait;
use crate::optimizer::{OptExpr, OptGroup, Optimizer};
use crate::properties::LogicalProperty;

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct Limit {
  limit: usize,
}

impl Limit {
  pub fn new(limit: usize) -> Self {
    Self { limit }
  }

  pub fn limit(&self) -> usize {
    self.limit
  }
}

impl OperatorTrait for Limit {
  fn derive_logical_prop<O: Optimizer>(&self, handle: O::ExprHandle,
                                       optimizer: &O) -> OptResult<LogicalProperty> {
    let input_group_handle = optimizer.expr_at(handle).input_at(0, optimizer);
    let input_group = optimizer.group_at(input_group_handle);
    Ok(input_group.logical_prop().clone())
  }
}

use std::fmt::Debug;


use enum_dispatch::enum_dispatch;

use crate::cost::Cost;
use crate::error::OptResult;
use crate::operator::{Join, Projection, TableScan};
use crate::optimizer::Optimizer;
use crate::properties::PhysicalPropertySet;

/// Physical relational operator.
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
#[enum_dispatch]
pub enum PhysicalOperator {
  PhysicalProjection(Projection),
  PhysicalHashJoin(Join),
  PhysicalTableScan(TableScan),
}

pub struct DerivePropContext<'a, O: Optimizer> {
  pub required_prop: &'a PhysicalPropertySet,
  pub expr_handle: O::ExprHandle,
  pub optimizer: &'a O,
}

#[derive(Debug)]
pub struct DerivePropResult {
  pub output_prop: PhysicalPropertySet,
  pub input_required_props: Vec<PhysicalPropertySet>,
}

#[enum_dispatch(PhysicalOperator)]
pub trait PhysicalOperatorTrait: Debug + PartialEq {
  /// Derive children's required properties with required properties of current node.
  fn derive_properties<O: Optimizer>(
    &self,
    context: DerivePropContext<O>,
  ) -> OptResult<Vec<DerivePropResult>>;

  /// Cost of current operator without accumulating children's cost.
  fn cost<O: Optimizer>(&self, expr_handle: O::ExprHandle, optimizer: &O) -> OptResult<Cost>;
}

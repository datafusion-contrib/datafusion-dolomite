use std::fmt::Debug;

use enum_dispatch::enum_dispatch;

use crate::error::DolomiteResult;
use crate::operator::{Join, Projection, TableScan};
use crate::optimizer::Optimizer;
use crate::properties::PhysicalPropertySet;

/// Physical relational operator.
#[derive(Clone, Debug, Hash, PartialEq)]
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
    ) -> DolomiteResult<Vec<DerivePropResult>>;
}

use crate::operator::PhysicalOperator;
use crate::properties::{DistributionSpec, OrderSpec};

/// All physical properties.
#[derive(Hash, Debug, Clone, Eq, PartialEq, Default)]
pub struct PhysicalPropertySet {
    dist: DistributionSpec,
    orders: OrderSpec,
}

pub(crate) struct Enforcer {
    pub(crate) operator: PhysicalOperator,
    pub(crate) output_prop: PhysicalPropertySet,
}

impl PhysicalPropertySet {
    /// When `input_prop` doesn't meet requirements of `required_prop`, try to append enforcer
    /// physical operators to ensure requirement.s
    pub(crate) fn append_enforcers(
        _required_prop: &PhysicalPropertySet,
        _input_prop: &PhysicalPropertySet,
    ) -> Vec<Enforcer> {
        vec![]
    }
}

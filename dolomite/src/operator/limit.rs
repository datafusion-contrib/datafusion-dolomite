use crate::error::DolomiteResult;
use crate::operator::{DisplayFields, OperatorTrait};
use crate::optimizer::{OptExpr, OptGroup, Optimizer};
use crate::properties::LogicalProperty;
use std::fmt::Formatter;

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
    fn derive_logical_prop<O: Optimizer>(
        &self,
        handle: O::ExprHandle,
        optimizer: &O,
    ) -> DolomiteResult<LogicalProperty> {
        let input_group_handle = optimizer.expr_at(handle).input_at(0, optimizer);
        let input_group = optimizer.group_at(input_group_handle);
        Ok(input_group.logical_prop().clone())
    }
}

impl DisplayFields for Limit {
    fn display(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // The struct should leave to Operator name
        f.debug_struct("").field("limit", &self.limit).finish()
    }
}

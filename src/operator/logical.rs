
use enum_as_inner::EnumAsInner;

use crate::operator::{Join, Limit, Projection, TableScan};
use enum_dispatch::enum_dispatch;

/// Logical relational operator.
#[derive(Clone, Debug, Hash, Eq, PartialEq, EnumAsInner)]
#[enum_dispatch]
pub enum LogicalOperator {
    LogicalLimit(Limit),
    LogicalProjection(Projection),
    LogicalJoin(Join),
    LogicalScan(TableScan),
}

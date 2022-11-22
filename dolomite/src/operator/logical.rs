use enum_as_inner::EnumAsInner;
use std::fmt::{Display, Formatter};

use crate::operator::{DisplayFields, Filter};
use crate::operator::{Join, Limit, Projection, TableScan};
use enum_dispatch::enum_dispatch;
use strum_macros::AsRefStr;

/// Logical relational operator.
#[derive(Clone, Debug, Hash, PartialEq, EnumAsInner, AsRefStr)]
#[enum_dispatch]
pub enum LogicalOperator {
    LogicalLimit(Limit),
    LogicalFilter(Filter),
    LogicalProjection(Projection),
    LogicalJoin(Join),
    LogicalScan(TableScan),
}

impl Display for LogicalOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())?;
        self.display(f)
    }
}

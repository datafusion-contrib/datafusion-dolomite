//! Contains relational operators such as join, projection, limit, etc.
//!
//! Currently they are classified into two categories: logical and physical. We separate logical
//! and physical operators in two enums since they need to implement different traits. For
//! example, physical operators should implement trait for deriving cost and statistics.
mod logical;

pub use logical::*;
use std::fmt::{Display, Formatter};
mod physical;
pub use physical::*;
mod limit;
pub use limit::*;
mod projection;
pub use projection::*;
mod table_scan;
pub use table_scan::*;
mod join;
use enum_as_inner::EnumAsInner;
pub use join::*;
pub use physical::*;

use crate::error::DolomiteResult;
use crate::operator::Operator::{Logical, Physical};
use crate::optimizer::Optimizer;
use crate::properties::LogicalProperty;
use enum_dispatch::enum_dispatch;

#[derive(Clone, Debug, Hash, PartialEq, EnumAsInner)]
pub enum Operator {
    Logical(LogicalOperator),
    Physical(PhysicalOperator),
}

#[enum_dispatch(LogicalOperator, PhysicalOperator)]
pub trait OperatorTrait {
    fn derive_logical_prop<O: Optimizer>(
        &self,
        _handle: O::ExprHandle,
        optimizer: &O,
    ) -> DolomiteResult<LogicalProperty>;
}

impl OperatorTrait for Operator {
    fn derive_logical_prop<O: Optimizer>(
        &self,
        _handle: O::ExprHandle,
        _optimizer: &O,
    ) -> DolomiteResult<LogicalProperty> {
        match self {
            Logical(op) => op.derive_logical_prop(_handle, _optimizer),
            Physical(op) => op.derive_logical_prop(_handle, _optimizer),
        }
    }
}

#[enum_dispatch(LogicalOperator, PhysicalOperator)]
pub trait DisplayFields {
    fn display(&self, fmt: &mut Formatter) -> std::fmt::Result;
}

impl Display for Operator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Logical(op) => write!(f, "{}", op),
            Physical(op) => write!(f, "{}", op),
        }
    }
}

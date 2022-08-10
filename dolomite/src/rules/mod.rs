//! Optimization rules.
//!
//! A rule defines equivalent transformation of query plan. There are three kinds of rules:
//!
//! 1. Rewrite rule. It produces a transformation which is assumed to be better than original
//! plan. For example, [`RemoveLimitRule`] which removes unnecessary limit.
//! 2. Exploration rule. It produces equivalent alternative logical plan, which is used in the
//! exploration phase of optimizer. For example, [`CommutateJoinRule`] just swaps the inputs of
//! inner join with eq condition.
//! 3. Implementation rule. It transforms logical operator to physical operator to provide
//! physical implementation. For example, [`Join2HashJoinRule`] transforms inner join to hash join.
//!
//! ## Pattern
//!
//! A patten defines what expression the rule should operate on. With pattern definition, the
//! rule can avoid manipulating plan directly. This has several advantages:
//!
//! 1. Decouple rule and concrete optimizer implementation. This way we can apply rule to both
//! heuristic optimizer and cascades optimizer.
//! 2. Decouple rule application and iteration. This scan significantly simplify rule
//! implementation, since rule should only care about defining equivalent transformations.
//!
//! Let use the [`RemoveLimitRule`] to illustrate, its pattern is defined as following:
//! ```no
//! static ref REMOVE_LIMIT_RULE_PATTERN: Pattern = {
//!     pattern(|op| matches!(op, Logical(LogicalLimit(_))))
//!         .leaf(|op| matches!(op, Logical(LogicalLimit(_))))
//!     .finish()
//!};
//! ```
//!
//! When [`RemoveLimitRule`] is invoked by optimizer, its input/output is [`OptExpression`]
//! rather plan.
//!```no
//! [GroupExprId(0, 0) Limit(10)]                             [Operator Limit(5)]
//!              |                                                     |
//!              |                                                     |
//!              |                   RemoveLimitRule                   |
//! [GroupExprId(1, 0) Limit(5)]        -------->                 [GroupId (2)]
//!              |
//!              |
//!              |
//!         [GroupId(2)]
//!
//! ```
//!
//! Instead of manipulating plan directly, the optimizer generates [`OptExpression`] using rule's
//! pattern, and the rule generates equivalent transformation. Optimizer uses generated
//! transformation to manipulate internal plan representation, e.g. memo in cascades optimizer or
//! graph in heuristic optimizer.
mod pattern;
pub use pattern::*;
mod limit;
pub use limit::*;
mod join;
pub use join::*;
mod opt_expr;
mod table_scan;
pub use opt_expr::*;

use std::fmt::{Debug, Formatter};

use enum_dispatch::enum_dispatch;
use enumset::EnumSetType;
use std::convert::AsRef;
use strum_macros::AsRefStr;

pub use table_scan::*;

use crate::error::DolomiteResult;
use crate::optimizer::Optimizer;

pub type OptExprVec<O> = Vec<OptExpression<O>>;

pub struct RuleResult<O: Optimizer> {
    exprs: OptExprVec<O>,
}

impl<O: Optimizer> Default for RuleResult<O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<O: Optimizer> RuleResult<O> {
    pub fn new() -> Self {
        Self { exprs: vec![] }
    }

    pub fn add(&mut self, new_expr: OptExpression<O>) {
        self.exprs.push(new_expr);
    }

    pub fn results(self) -> impl Iterator<Item = OptExpression<O>> {
        self.exprs.into_iter()
    }
}

/// A rule should only focus on providing equivalent transformations of optimizer expressions.
#[enum_dispatch(RuleImpl)]
pub trait Rule {
    /// Apply a rule to match sub plan.
    fn apply<O: Optimizer>(
        &self,
        input: OptExpression<O>,
        ctx: &O,
        result: &mut RuleResult<O>,
    ) -> DolomiteResult<()>;

    /// Pattern for rule.
    fn pattern(&self) -> &Pattern;

    /// Use to identify each rule.
    ///
    /// This is used to avoid applying same rule repeatedly to same group expression.
    fn rule_id(&self) -> RuleId;

    /// Use to identify applying order of rules.
    fn rule_promise(&self) -> RulePromise;
}

#[enum_dispatch]
#[derive(Clone, AsRefStr)]
pub enum RuleImpl {
    // Rewrite rules
    PushLimitOverProjectionRule,
    RemoveLimitRule,
    PushLimitToTableScanRule,

    // Exploring rules
    CommutateJoinRule,

    // Implementation rules
    Join2HashJoinRule,
    Scan2TableScanRule,
}

#[derive(EnumSetType, Debug)]
pub enum RuleId {
    // Rewrite rules
    PushLimitOverProjection,
    RemoveLimit,
    PushLimitToTableScan,

    // Exploring rules
    CommutateJoin,

    // Implementation rules
    Join2HashJoin,
    Scan2TableScan,
}

pub enum RulePromise {
    LOW = 1,
    Medium = 2,
    High = 3,
}

impl Debug for RuleImpl {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use crate::cascades::{CascadesOptimizer, GroupExprId, GroupId};
    use crate::operator::LogicalOperator::{LogicalLimit, LogicalScan};
    use crate::operator::Operator::Logical;
    use crate::operator::{Limit, TableScan};
    use crate::rules::{CommutateJoinRule, OptExpression, RuleImpl};

    #[test]
    fn test_opt_expr_operator_format() {
        let tb1 = OptExpression::<CascadesOptimizer>::from(Logical(LogicalScan(
            TableScan::new("t1".to_string()),
        )));
        let tb2 = OptExpression::<CascadesOptimizer>::from(Logical(LogicalScan(
            TableScan::new("t2".to_string()),
        )));
        let opt_expr = OptExpression::<CascadesOptimizer>::with_operator(
            Logical(LogicalLimit(Limit::new(1))),
            vec![tb1, tb2],
        );

        println!("{:?}", opt_expr);
    }

    #[test]
    fn test_opt_expr_group_expr_format() {
        let tb1 = OptExpression::<CascadesOptimizer>::from(Logical(LogicalScan(
            TableScan::new("t1".to_string()),
        )));
        let tb2 = OptExpression::<CascadesOptimizer>::from(Logical(LogicalScan(
            TableScan::new("t2".to_string()),
        )));
        let opt_expr = OptExpression::<CascadesOptimizer>::with_expr_handle(
            GroupExprId::new(GroupId(10), 4),
            vec![tb1, tb2],
        );

        println!("{:?}", opt_expr);
    }

    #[test]
    fn test_rule_debug() {
        println!("{:?}", RuleImpl::from(CommutateJoinRule::new()));
    }
}

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
mod limit_push_down;
pub use limit_push_down::*;
mod join;
pub use join::*;
mod table_scan;

use std::fmt::{Debug, Formatter};
use std::ops::Index;

use anyhow::bail;
use enum_dispatch::enum_dispatch;
use enumset::EnumSetType;
use std::convert::AsRef;
use strum_macros::AsRefStr;

pub use table_scan::*;

use crate::error::OptResult;
use crate::operator::Operator;
use crate::optimizer::{OptExpr, Optimizer};
use crate::rules::OptExprNode::{ExprHandleNode, GroupHandleNode, OperatorNode};

pub type OptExprVec<O> = Vec<OptExpression<O>>;

/// One node in [`OptExpression`].
pub enum OptExprNode<O: Optimizer> {
    OperatorNode(Operator),
    ExprHandleNode(O::ExprHandle),
    GroupHandleNode(O::GroupHandle),
}

impl<O: Optimizer> Clone for OptExprNode<O> {
    fn clone(&self) -> Self {
        match self {
            OperatorNode(op) => OperatorNode(op.clone()),
            ExprHandleNode(handle) => ExprHandleNode(handle.clone()),
            GroupHandleNode(handle) => GroupHandleNode(handle.clone()),
        }
    }
}

/// Optimizer expression tree matches rule pattern. Used as input/output of optimizer rule.
///
/// When used as input, `node` must be an [`OptExprHandle`].
/// When used as output, `node` can be either of [`OptExprHandle`] or [`Operator`]. For example when
/// an `node` is created by rule after some transformation, it should be an [`Operator`]. If it's
/// created by cloning original node inputs, it should be an [`OptExprHandle`].
pub struct OptExpression<O: Optimizer> {
    node: OptExprNode<O>,
    inputs: OptExprVec<O>,
}

impl<O: Optimizer> Clone for OptExpression<O> {
    fn clone(&self) -> Self {
        Self {
            node: self.node.clone(),
            inputs: self.inputs.clone(),
        }
    }
}

impl<O: Optimizer> OptExpression<O> {
    pub fn with_operator<I>(operator: Operator, inputs: I) -> Self
    where
        I: IntoIterator<Item = Self>,
    {
        Self {
            node: OperatorNode(operator),
            inputs: inputs.into_iter().collect(),
        }
    }

    pub fn with_expr_handle<I>(opt_node: O::ExprHandle, inputs: I) -> Self
    where
        I: IntoIterator<Item = Self>,
    {
        Self {
            node: ExprHandleNode(opt_node),
            inputs: inputs.into_iter().collect(),
        }
    }

    /// Creates an opt expression with group handle.
    ///
    /// Note that group handle can only be leaf node, so it never inputs.
    pub fn with_group_handle(handle: O::GroupHandle) -> Self {
        Self {
            node: GroupHandleNode(handle),
            inputs: vec![],
        }
    }

    pub fn clone_with_inputs(&self, operator: Operator) -> Self {
        Self {
            node: OperatorNode(operator),
            inputs: self.inputs.clone(),
        }
    }

    pub fn inputs(&self) -> &[Self] {
        &self.inputs
    }

    pub fn node(&self) -> &OptExprNode<O> {
        &self.node
    }

    pub fn get_operator<'a>(&'a self, optimizer: &'a O) -> OptResult<&'a Operator> {
        match &self.node {
            ExprHandleNode(opt_node) => {
                Ok(optimizer.expr_at(opt_node.clone()).operator())
            }
            OperatorNode(op) => Ok(op),
            _ => bail!("Can't get operator from group handle!"),
        }
    }
}

impl<O: Optimizer> Debug for OptExpression<O> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.format(f, 0)
    }
}

/// Creates a leaf opt expression from operator.
impl<O: Optimizer> From<Operator> for OptExpression<O> {
    fn from(op: Operator) -> Self {
        OptExpression::<O>::with_operator(op, vec![])
    }
}

impl<O: Optimizer> OptExpression<O> {
    fn format(&self, f: &mut Formatter<'_>, level: usize) -> std::fmt::Result {
        let prefix = if level > 0 {
            let mut buffer = String::with_capacity(2 * level);
            for _ in 0..(level - 1) {
                buffer.push_str("  ");
            }
            buffer.push_str("--");
            buffer
        } else {
            "".to_string()
        };

        match &self.node {
            ExprHandleNode(handle) => write!(f, "{}{:?}\n", prefix, handle),
            GroupHandleNode(handle) => write!(f, "{}{:?}\n", prefix, handle),
            OperatorNode(operator) => write!(f, "{}{:?}\n", prefix, operator),
        }?;
        for input in &self.inputs {
            input.format(f, level + 1)?;
        }

        Ok(())
    }
}

/// Index of inputs.
impl<O: Optimizer> Index<usize> for OptExpression<O> {
    type Output = OptExpression<O>;

    fn index(&self, index: usize) -> &Self::Output {
        &self.inputs[index]
    }
}

pub struct RuleResult<O: Optimizer> {
    exprs: OptExprVec<O>,
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

#[enum_dispatch(RuleImpl)]
pub trait Rule {
    /// Apply a rule to match sub plan.
    fn apply<O: Optimizer>(
        &self,
        input: OptExpression<O>,
        ctx: &O,
        result: &mut RuleResult<O>,
    ) -> OptResult<()>;

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

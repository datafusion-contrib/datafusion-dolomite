use crate::error::DolomiteResult;
use crate::operator::Operator;
use crate::optimizer::{OptExpr, Optimizer};
use crate::rules::OptExprNode::{ExprHandleNode, GroupHandleNode, OperatorNode};
use crate::rules::OptExprVec;
use crate::utils::RootBuilder;
use anyhow::bail;
use std::fmt::{Debug, Formatter};
use std::ops::Index;

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

impl<O: Optimizer> PartialEq for OptExprNode<O> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (OperatorNode(this_op), OperatorNode(other_op)) => this_op == other_op,
            (ExprHandleNode(handle), ExprHandleNode(other_handle)) => {
                handle == other_handle
            }
            (GroupHandleNode(handle), GroupHandleNode(other_handle)) => {
                handle == other_handle
            }
            _ => false,
        }
    }
}

impl<O: Optimizer> Debug for OptExprNode<O> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            OperatorNode(op) => write!(f, "OperatorNode: {:?}", op),
            ExprHandleNode(handle) => write!(f, "ExprHandleNode: {:?}", handle),
            GroupHandleNode(handle) => write!(f, "GroupHandleNode: {:?}", handle),
        }
    }
}

impl<O: Optimizer> From<Operator> for OptExprNode<O> {
    fn from(t: Operator) -> Self {
        OperatorNode(t)
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

impl<O: Optimizer> PartialEq for OptExpression<O> {
    fn eq(&self, other: &Self) -> bool {
        self.node == other.node && self.inputs == other.inputs
    }
}

impl<O: Optimizer> OptExpression<O> {
    pub fn new_builder<N: Into<OptExprNode<O>>>(
        node: N,
    ) -> RootBuilder<Self, OptExprNode<O>> {
        RootBuilder::new(node.into())
    }

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

    pub fn get_operator<'a>(&'a self, optimizer: &'a O) -> DolomiteResult<&'a Operator> {
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
            ExprHandleNode(handle) => writeln!(f, "{}{:?}", prefix, handle),
            GroupHandleNode(handle) => writeln!(f, "{}{:?}", prefix, handle),
            OperatorNode(operator) => writeln!(f, "{}{:?}", prefix, operator),
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
impl<O: Optimizer> From<(OptExprNode<O>, Vec<OptExpression<O>>)> for OptExpression<O> {
    fn from(t: (OptExprNode<O>, Vec<OptExpression<O>>)) -> Self {
        OptExpression {
            node: t.0,
            inputs: t.1,
        }
    }
}

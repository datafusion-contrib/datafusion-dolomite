use crate::error::DolomiteResult;
use crate::operator::LogicalOperator::{LogicalJoin, LogicalLimit};
use crate::operator::Operator::Logical;
use crate::operator::{Join, Operator};
use crate::optimizer::OptimizerContext;
use crate::plan::{PlanNode, PlanNodeId, PlanNodeRef, Visitor};
use anyhow::bail;
use std::collections::HashMap;
use std::sync::Arc;

struct DecorrelatorContext {
    root: PlanNodeRef,
    correlated_plan: PlanNodeRef,

    has_correlated_expr: HashMap<PlanNodeId, bool>,
    optimizer_context: OptimizerContext,
}

impl DecorrelatorContext {
    fn push_down_dependent_join(self) -> DolomiteResult<PlanNodeRef> {
        todo!()
    }
}

struct Decorrelator {}

impl Visitor for Decorrelator {
    type C = DecorrelatorContext;
    type R = PlanNodeRef;

    fn visit(
        &self,
        mut context: DecorrelatorContext,
        current_node: PlanNodeRef,
    ) -> DolomiteResult<PlanNodeRef> {
        if !context.has_correlated_expr.get(&root.id()).unwrap() {
            let cross_join = PlanNode::new(
                context.optimizer_context.next_plan_node_id(),
                Operator::Logical(LogicalJoin(Join::new_cross_product())),
                vec![context.root.clone(), current_node],
            );

            Ok(Arc::new(cross_join))
        } else {
        }
    }
}

impl Decorrelator {
    fn push_down_dependent_join(
        &self,
        mut context: DecorrelatorContext,
        current_node: PlanNodeRef,
    ) -> DolomiteResult<PlanNodeRef> {
        match current_node.operator() {
            Logical(LogicalLimit(limit)) => {
                bail!("Limit is not supported in correlated subquery now!")
            }
        }
    }
}

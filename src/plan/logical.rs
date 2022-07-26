use std::sync::Arc;
use datafusion::prelude::{Expr, JoinType};
use crate::operator::LogicalOperator::{LogicalJoin, LogicalProjection, LogicalScan};
use crate::operator::Operator::Logical;
use crate::operator::{Join, Limit, LogicalOperator, Projection, TableScan};
use crate::plan::{Plan, PlanNode, PlanNodeId, PlanNodeRef};

pub struct LogicalPlanBuilder {
    root: Option<PlanNodeRef>,
    next_plan_node_id: PlanNodeId,
}

impl LogicalPlanBuilder {
    pub fn new() -> Self {
        Self {
            root: None,
            next_plan_node_id: 0,
        }
    }

    fn reset_root(&mut self, new_root: PlanNodeRef) -> &mut Self {
        self.root = Some(new_root);
        self.next_plan_node_id += 1;
        self
    }

    pub fn scan<S: Into<String>>(
        &mut self,
        limit: Option<usize>,
        table_name: S,
    ) -> &mut Self {
        let table_scan = match limit {
            Some(l) => TableScan::with_limit(table_name.into(), l),
            None => TableScan::new(table_name.into()),
        };
        let plan_node = Arc::new(PlanNode::new(
            self.next_plan_node_id,
            Logical(LogicalScan(table_scan)),
            vec![],
        ));

        self.reset_root(plan_node)
    }

    pub fn projection<I: IntoIterator<Item = Expr>>(&mut self, exprs: I) -> &mut Self {
        let projection = Projection::new(exprs);
        let plan_node = Arc::new(PlanNode::new(
            self.next_plan_node_id,
            Logical(LogicalProjection(projection)),
            vec![self.root.clone().unwrap()],
        ));

        self.reset_root(plan_node)
    }

    pub fn limit(&mut self, limit: usize) -> &mut Self {
        let limit = Limit::new(limit);
        let plan_node = Arc::new(PlanNode::new(
            self.next_plan_node_id,
            Logical(LogicalOperator::LogicalLimit(limit)),
            vec![self.root.clone().unwrap()],
        ));

        self.reset_root(plan_node)
    }

    pub fn join(
        &mut self,
        join_type: JoinType,
        condition: Expr,
        right: PlanNodeRef,
    ) -> &mut Self {
        let join = Join::new(join_type, condition);
        let plan_node = Arc::new(PlanNode::new(
            self.next_plan_node_id,
            Logical(LogicalJoin(join)),
            vec![self.root.clone().unwrap(), right],
        ));

        self.reset_root(plan_node)
    }

    /// Consume current plan, but not rest state, e.g. plan node id.
    ///
    /// This is useful for building multi child plan, e.g. join.
    pub fn build(&mut self) -> Plan {
        let ret = Plan {
            root: self.root.clone().unwrap(),
        };
        self.root = None;
        ret
    }
}

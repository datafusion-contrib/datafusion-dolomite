use std::sync::Arc;
use datafusion::logical_plan::JoinType;
use datafusion::prelude::Expr;
use crate::operator::Operator::Physical;
use crate::operator::PhysicalOperator::{PhysicalHashJoin, PhysicalTableScan};
use crate::operator::{Join, TableScan};
use crate::plan::{Plan, PlanNode, PlanNodeId, PlanNodeRef};

pub struct PhysicalPlanBuilder {
    root: PlanNodeRef,
    next_plan_node_id: PlanNodeId,
}

impl PhysicalPlanBuilder {
    fn reset_root(&mut self, new_root: PlanNodeRef) {
        self.root = new_root;
        self.next_plan_node_id += 1;
    }

    pub fn scan<S: Into<String>>(limit: Option<usize>, table_name: S) -> Self {
        let table_scan = match limit {
            Some(l) => TableScan::with_limit(table_name, l),
            None => TableScan::new(table_name.into()),
        };
        let plan_node = Arc::new(PlanNode::new(
            0,
            Physical(PhysicalTableScan(table_scan)),
            vec![],
        ));

        Self {
            root: plan_node,
            next_plan_node_id: 1,
        }
    }

    pub fn hash_join(
        mut self,
        join_type: JoinType,
        condition: Expr,
        right: PlanNodeRef,
    ) -> Self {
        let join = Join::new(join_type, condition);
        let plan_node = Arc::new(PlanNode::new(
            self.next_plan_node_id,
            Physical(PhysicalHashJoin(join)),
            vec![self.root.clone(), right],
        ));

        self.reset_root(plan_node);

        self
    }

    pub fn build(self) -> Plan {
        Plan { root: self.root }
    }
}

use crate::cascades::memo::Memo;
use crate::cascades::task::{schedule, OptimizeGroupTask};
use crate::cascades::{Group, GroupExpr, GroupExprId, GroupId};



use crate::cost::{INF};
use crate::error::OptResult;

use crate::optimizer::{Optimizer, OptimizerContext};
use crate::plan::Plan;
use crate::properties::PhysicalPropertySet;
use crate::rules::RuleImpl;

pub struct CascadesOptimizer {
    pub(super) required_prop: PhysicalPropertySet,
    pub(super) rules: Vec<RuleImpl>,
    pub(super) memo: Memo,
    pub(super) context: OptimizerContext,
}

impl Optimizer for CascadesOptimizer {
    type GroupHandle = GroupId;
    type ExprHandle = GroupExprId;
    type Group = Group;
    type Expr = GroupExpr;

    fn context(&self) -> &OptimizerContext {
        &self.context
    }

    fn group_at(&self, group_handle: GroupId) -> &Group {
        &self.memo[group_handle]
    }

    fn expr_at(&self, expr_handle: GroupExprId) -> &GroupExpr {
        &self.memo[expr_handle]
    }

    fn find_best_plan(mut self) -> OptResult<Plan> {
        let root_task =
            OptimizeGroupTask::new(self.memo.root_group_id(), self.required_prop.clone(), INF)
                .into();

        schedule(&mut self, root_task)?;

        println!("Memo after optimization: {:?}", self.memo);

        self.memo.best_plan(&self.required_prop)
    }
}

impl CascadesOptimizer {
    pub fn new(
        required_prop: PhysicalPropertySet,
        rules: Vec<RuleImpl>,
        plan: Plan,
        context: OptimizerContext,
    ) -> Self {
        Self {
            required_prop,
            rules,
            memo: Memo::from(plan),
            context,
        }
    }

    #[cfg(test)]
    pub fn new_for_test(plan: Plan) -> Self {
        Self {
            required_prop: PhysicalPropertySet::default(),
            rules: vec![],
            memo: Memo::from(plan),
            context: OptimizerContext::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::cascades::CascadesOptimizer;
    
    
    use crate::optimizer::{Optimizer, OptimizerContext};
    use crate::plan::{LogicalPlanBuilder, PhysicalPlanBuilder};
    use crate::properties::PhysicalPropertySet;
    use crate::rules::{CommutateJoinRule, Join2HashJoinRule, Scan2TableScanRule};
    use datafusion::logical_expr::col;
    use datafusion::logical_plan::Operator::Eq;
    use datafusion::logical_plan::{binary_expr, JoinType};

    #[test]
    fn test_optimize_join() {
        let plan = {
            let mut builder = LogicalPlanBuilder::new();
            let right = builder.scan(None, "t2").build().root();
            builder
                .scan(None, "t1")
                .join(
                    JoinType::Inner,
                    binary_expr(col("t1.c1"), Eq, col("t2.c2")),
                    right,
                )
                .build()
        };

        let optimizer = CascadesOptimizer::new(
            PhysicalPropertySet::default(),
            vec![
                CommutateJoinRule::new().into(),
                Join2HashJoinRule::new().into(),
                Scan2TableScanRule::new().into(),
            ],
            plan,
            OptimizerContext::default(),
        );

        let expected_plan = {
            let right = PhysicalPlanBuilder::scan(None, "t2").build().root();

            PhysicalPlanBuilder::scan(None, "t1")
                .hash_join(
                    JoinType::Inner,
                    binary_expr(col("t1.c1"), Eq, col("t2.c2")),
                    right,
                )
                .build()
        };

        assert_eq!(expected_plan, optimizer.find_best_plan().unwrap());
    }
}

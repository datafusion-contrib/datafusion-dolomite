use anyhow::bail;
use std::cmp::min;

use crate::error::OptResult;
use crate::operator::LogicalOperator::{LogicalLimit, LogicalProjection, LogicalScan};
use crate::operator::Operator::Logical;
use crate::operator::{Limit, TableScan};
use crate::optimizer::Optimizer;
use crate::rules::RuleId::{PushLimitOverProjection, PushLimitToTableScan, RemoveLimit};
use crate::rules::RulePromise::LOW;
use crate::rules::{
    pattern, OptExpression, PatterBuilder, Pattern, Rule, RuleId, RulePromise, RuleResult,
};

#[rustfmt::skip::macros(lazy_static)]
lazy_static! {
    static ref REMOVE_LIMIT_RULE_PATTERN: Pattern = {
        pattern(|op| matches!(op, Logical(LogicalLimit(_))))
          .leaf(|op| matches!(op, Logical(LogicalLimit(_))))
        .finish()
    };
    static ref PUSH_LIMIT_OVER_PROJECTION_PATTERN: Pattern = {
        pattern(|op| matches!(op, Logical(LogicalLimit(_))))
          .leaf(|op| matches!(op, Logical(LogicalProjection(_))))
        .finish()
    };
    static ref PUSH_LIMIT_TO_TABLE_SCAN_PATTERN: Pattern = {
        pattern(|op| matches!(op, Logical(LogicalLimit(_))))
          .leaf(|op| matches!(op, Logical(LogicalScan(_))))
        .finish()
    };
}

#[derive(Clone)]
pub struct PushLimitOverProjectionRule {}

impl PushLimitOverProjectionRule {
    pub fn new() -> Self {
        Self {}
    }
}

impl Rule for PushLimitOverProjectionRule {
    fn apply<O: Optimizer>(
        &self,
        opt_expr: OptExpression<O>,
        _ctx: &O,
        result: &mut RuleResult<O>,
    ) -> OptResult<()> {
        let limit = opt_expr.get_operator(&_ctx)?;
        let projection = opt_expr[0].get_operator(&_ctx)?;

        let new_limit = opt_expr[0].clone_with_inputs(limit.clone());
        let ret = OptExpression::with_operator(projection.clone(), vec![new_limit]);

        result.add(ret);

        Ok(())
    }

    fn pattern(&self) -> &Pattern {
        &PUSH_LIMIT_OVER_PROJECTION_PATTERN
    }

    fn rule_id(&self) -> RuleId {
        PushLimitOverProjection
    }

    fn rule_promise(&self) -> RulePromise {
        LOW
    }
}

#[derive(Clone)]
pub struct RemoveLimitRule {}

impl RemoveLimitRule {
    pub fn new() -> Self {
        Self {}
    }
}

impl Rule for RemoveLimitRule {
    fn apply<O: Optimizer>(
        &self,
        input: OptExpression<O>,
        _ctx: &O,
        result: &mut RuleResult<O>,
    ) -> OptResult<()> {
        if let (Logical(LogicalLimit(limit1)), Logical(LogicalLimit(limit2))) =
            (input.get_operator(&_ctx)?, input[0].get_operator(&_ctx)?)
        {
            let new_limit = min(limit1.limit(), limit2.limit());

            let ret = input[0].clone_with_inputs(Logical(LogicalLimit(Limit::new(new_limit))));

            result.add(ret);
            Ok(())
        } else {
            bail!("Pattern miss matched")
        }
    }

    fn pattern(&self) -> &Pattern {
        &REMOVE_LIMIT_RULE_PATTERN
    }

    fn rule_id(&self) -> RuleId {
        RemoveLimit
    }

    fn rule_promise(&self) -> RulePromise {
        LOW
    }
}

#[derive(Clone)]
pub struct PushLimitToTableScanRule {}

impl PushLimitToTableScanRule {
    pub fn new() -> Self {
        Self {}
    }
}

impl Rule for PushLimitToTableScanRule {
    fn apply<O: Optimizer>(
        &self,
        input: OptExpression<O>,
        ctx: &O,
        result: &mut RuleResult<O>,
    ) -> OptResult<()> {
        if let (Logical(LogicalLimit(limit)), Logical(LogicalScan(scan))) =
            (input.get_operator(&ctx)?, input[0].get_operator(&ctx)?)
        {
            let new_limit = scan
                .limit()
                .map(|l1| min(l1, limit.limit()))
                .unwrap_or(limit.limit());

            let ret = OptExpression::from(Logical(LogicalScan(TableScan::with_limit(
                scan.table_name(),
                new_limit,
            ))));

            result.add(ret);

            Ok(())
        } else {
            bail!("Pattern miss matched!")
        }
    }

    fn pattern(&self) -> &Pattern {
        &PUSH_LIMIT_TO_TABLE_SCAN_PATTERN
    }

    fn rule_id(&self) -> RuleId {
        PushLimitToTableScan
    }

    fn rule_promise(&self) -> RulePromise {
        LOW
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::catalog::schema::MemorySchemaProvider;
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::logical_expr::col;
    use serde_json::Value;

    use crate::heuristic::{HepOptimizer, MatchOrder};
    use crate::optimizer::{Optimizer, OptimizerContext};
    use crate::plan::{LogicalPlanBuilder, Plan};

    use crate::rules::{
        PushLimitOverProjectionRule, PushLimitToTableScanRule, RemoveLimitRule, Rule, RuleImpl,
    };

    fn build_hep_optimizer(rules: Vec<RuleImpl>, plan: Plan) -> HepOptimizer {
        let schema = {
            let json = r#"{
                "fields": [
                    {
                        "name": "c1",
                        "nullable": false,
                        "type": {
                            "name": "utf8"
                        },
                        "children": []
                    },
                    {
                        "name": "c2",
                        "nullable": false,
                        "type": {
                            "name": "utf8"
                        },
                        "children": []
                    }
                ],
                "metadata": {}
            }"#;
            let value: Value = serde_json::from_str(json).unwrap();
            let schema = Schema::from(&value).unwrap();
            Arc::new(schema)
        };

        let table_provider = Arc::new(EmptyTable::new(Arc::new
            ((&*schema).clone().into())));


        let optimizer_context = OptimizerContext {
            catalog: Arc::new(MemorySchemaProvider::new())
        };

        optimizer_context.catalog.register_table("t1".to_string(), table_provider.clone()).unwrap();

        HepOptimizer::new(MatchOrder::TopDown, 1000, rules, plan, optimizer_context).unwrap()
    }

    #[test]
    fn test_push_limit() {
        let original_plan = LogicalPlanBuilder::new()
            .scan(None, "t1".to_string())
            .limit(5)
            .projection(vec![col("c1")] )
            .limit(10)
            .build();

        println!("Original plan: {:?}", original_plan);

        let optimizer = build_hep_optimizer(
            vec![
                PushLimitOverProjectionRule::new().into(),
                RemoveLimitRule::new().into(),
                PushLimitToTableScanRule::new().into(),
            ],
            original_plan,
        );

        let optimized_plan = optimizer.find_best_plan().unwrap();
        let expected_plan = {
            let raw_plan = LogicalPlanBuilder::new()
                .scan(Some(5), "t1".to_string())
                .projection(vec![col("c1")] )
                .build();

            let optimizer = build_hep_optimizer(
                vec![
                ],
                raw_plan,
            );

            optimizer.find_best_plan().unwrap()
        };

        assert_eq!(optimized_plan,  expected_plan);
    }

    #[test]
    fn test_push_limit_over_projection_pattern() {
        let original_plan = LogicalPlanBuilder::new()
            .scan(None, "t1".to_string())
            .limit(5)
            .projection(vec![col("c1")] )
            .limit(10)
            .build();

        let rule = PushLimitOverProjectionRule::new();
        assert!((rule.pattern().predict)(&original_plan.root().operator()));
    }
}

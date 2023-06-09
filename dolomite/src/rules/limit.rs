use anyhow::bail;
use std::cmp::min;

use crate::error::DolomiteResult;
use crate::operator::LogicalOperator::{LogicalLimit, LogicalProjection, LogicalScan};
use crate::operator::Operator::Logical;
use crate::operator::{Limit, TableScan};
use crate::optimizer::Optimizer;
use crate::rules::RuleId::{PushLimitOverProjection, PushLimitToTableScan, RemoveLimit};
use crate::rules::RulePromise::LOW;
use crate::rules::{OptExpression, Pattern, Rule, RuleId, RulePromise, RuleResult};
use crate::utils::TreeBuilder;

#[rustfmt::skip::macros(lazy_static)]
lazy_static! {
    static ref REMOVE_LIMIT_RULE_PATTERN: Pattern = {
        Pattern::new_builder(|op| matches!(op, Logical(LogicalLimit(_))))
          .leaf_node(|op| matches!(op, Logical(LogicalLimit(_))))
        .end()
    };
    static ref PUSH_LIMIT_OVER_PROJECTION_PATTERN: Pattern = {
        Pattern::new_builder(|op| matches!(op, Logical(LogicalLimit(_))))
          .leaf_node(|op| matches!(op, Logical(LogicalProjection(_))))
        .end()
    };
    static ref PUSH_LIMIT_TO_TABLE_SCAN_PATTERN: Pattern = {
        Pattern::new_builder(|op| matches!(op, Logical(LogicalLimit(_))))
          .leaf_node(|op| matches!(op, Logical(LogicalScan(_))))
        .end()
    };
}

#[derive(Clone, Default)]
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
    ) -> DolomiteResult<()> {
        let limit = opt_expr.get_operator(_ctx)?;
        let projection = opt_expr[0].get_operator(_ctx)?;

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

#[derive(Clone, Default)]
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
    ) -> DolomiteResult<()> {
        if let (Logical(LogicalLimit(limit1)), Logical(LogicalLimit(limit2))) =
            (input.get_operator(_ctx)?, input[0].get_operator(_ctx)?)
        {
            let new_limit = min(limit1.limit(), limit2.limit());

            let ret =
                input[0].clone_with_inputs(Logical(LogicalLimit(Limit::new(new_limit))));

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

#[derive(Clone, Default)]
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
    ) -> DolomiteResult<()> {
        if let (Logical(LogicalLimit(limit)), Logical(LogicalScan(scan))) =
            (input.get_operator(ctx)?, input[0].get_operator(ctx)?)
        {
            let new_limit = scan
                .limit()
                .map(|l1| min(l1, limit.limit()))
                .unwrap_or_else(|| limit.limit());

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
    use datafusion::logical_expr::col;
    use maplit::hashmap;

    use crate::heuristic::Binding;
    use crate::operator::LogicalOperator::{
        LogicalLimit, LogicalProjection, LogicalScan,
    };
    use crate::operator::{Limit, Operator, Projection, TableScan};
    use crate::plan::LogicalPlanBuilder;

    use crate::rules::{
        OptExpression, PushLimitOverProjectionRule, PushLimitToTableScanRule,
        RemoveLimitRule, Rule, RuleResult,
    };
    use crate::test_utils::build_hep_optimizer_for_test;
    use crate::test_utils::table_provider_from_schema;
    use crate::utils::TreeBuilder;

    const T1_SCHEMA_JSON: &str = r#"{
                "fields": [
                    {
                        "name": "c1",
                        "nullable": false,
                        "data_type": "Utf8",
                        "dict_id": 0,
                        "dict_is_ordered": false,
                        "metadata": {}
                    },
                    {
                        "name": "c2",
                        "nullable": false,
                        "data_type": "Utf8",
                        "dict_id": 0,
                        "dict_is_ordered": false,
                        "metadata": {}
                    }
                ],
                "metadata": {}
            }"#;

    #[test]
    fn test_push_limit_over_projection_pattern() {
        let original_plan = LogicalPlanBuilder::new()
            .scan(None, "t1".to_string())
            .limit(5)
            .projection(vec![col("c1")])
            .limit(10)
            .build();

        let rule = PushLimitOverProjectionRule::new();
        assert!((rule.pattern().predict)(original_plan.root().operator()));
    }

    #[test]
    fn test_limit_merge() {
        let original_plan = LogicalPlanBuilder::new()
            .scan(None, "t1".to_string())
            .limit(5)
            .limit(10)
            .build();

        let optimizer = build_hep_optimizer_for_test(
            hashmap!("t1".to_string() => table_provider_from_schema(T1_SCHEMA_JSON)),
            original_plan,
        );

        let rule = RemoveLimitRule::new();

        let opt_expr = Binding::new(optimizer.root_node_id(), rule.pattern(), &optimizer)
            .next()
            .unwrap();
        let table_scan_group_id = opt_expr[0][0].node().clone();

        let mut result = RuleResult::new();

        rule.apply(opt_expr, &optimizer, &mut result).unwrap();

        let expected_opt_expr =
            OptExpression::new_builder::<Operator>(LogicalLimit(Limit::new(5)).into())
                .leaf(table_scan_group_id)
                .end();

        assert_eq!(1, result.exprs.len());
        assert_eq!(expected_opt_expr, result.exprs[0]);
    }

    #[test]
    fn test_push_limit_to_table_scan() {
        let original_plan = LogicalPlanBuilder::new()
            .scan(None, "t1".to_string())
            .limit(5)
            .build();

        let optimizer = build_hep_optimizer_for_test(
            hashmap!("t1".to_string() => table_provider_from_schema(T1_SCHEMA_JSON)),
            original_plan,
        );

        let rule = PushLimitToTableScanRule::new();

        let opt_expr = Binding::new(optimizer.root_node_id(), rule.pattern(), &optimizer)
            .next()
            .unwrap();

        let mut result = RuleResult::new();

        rule.apply(opt_expr, &optimizer, &mut result).unwrap();

        let expected_opt_expr = OptExpression::new_builder::<Operator>(
            LogicalScan(TableScan::with_limit("t1", 5)).into(),
        )
        .end();

        assert_eq!(1, result.exprs.len());
        assert_eq!(expected_opt_expr, result.exprs[0]);
    }

    #[test]
    fn test_push_limit_over_projection() {
        let original_plan = LogicalPlanBuilder::new()
            .scan(None, "t1".to_string())
            .projection(vec![col("c1")])
            .limit(10)
            .build();

        let optimizer = build_hep_optimizer_for_test(
            hashmap!("t1".to_string() => table_provider_from_schema(T1_SCHEMA_JSON)),
            original_plan,
        );

        let rule = PushLimitOverProjectionRule::new();

        let opt_expr = Binding::new(optimizer.root_node_id(), rule.pattern(), &optimizer)
            .next()
            .unwrap();

        let table_scan_group_id = opt_expr[0][0].node().clone();

        let mut result = RuleResult::new();

        rule.apply(opt_expr, &optimizer, &mut result).unwrap();

        let expected_opt_expr = OptExpression::new_builder::<Operator>(
            LogicalProjection(Projection::new(vec![col("c1")])).into(),
        )
        .begin::<Operator>(LogicalLimit(Limit::new(10)).into())
        .leaf(table_scan_group_id)
        .end()
        .end();

        assert_eq!(1, result.exprs.len());
        assert_eq!(expected_opt_expr, result.exprs[0]);
    }
}

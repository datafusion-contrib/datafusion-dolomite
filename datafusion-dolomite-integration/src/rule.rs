use crate::conversion::{from_df_logical, to_df_logical};
use datafusion::common::DataFusionError;
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::optimizer::OptimizerRule;
use datafusion::optimizer::OptimizerConfig;
use dolomite::heuristic::{HepOptimizer, MatchOrder};
use dolomite::optimizer::{Optimizer, OptimizerContext};

use dolomite::rules::RuleImpl;

/// An adapter converts [`HeuristicOptimizer`] into datafusion's optimizer rule.
///
/// It works as followings:
/// ```no
/// Datafusion logical plan -> Our logical plan -> Heuristic optimizer -> Our logical plan ->
/// Datafusion logical plan
/// ```
pub struct DFOptimizerAdapterRule {
    /// Our rules
    rules: Vec<RuleImpl>,
    /// Optimizer Context
    optimizer_context: OptimizerContext,
}

impl OptimizerRule for DFOptimizerAdapterRule {
    fn try_optimize(
        &self,
        df_plan: &LogicalPlan,
        _optimizer_config: &dyn OptimizerConfig,
    ) -> datafusion::common::Result<Option<LogicalPlan>> {
        println!("Beginning to execute heuristic optimizer");
        let plan = from_df_logical(df_plan)
            .map_err(|e| DataFusionError::Plan(format!("{:?}", e)))?;

        // Construct heuristic optimizer here
        let hep_optimizer = HepOptimizer::new(
            MatchOrder::TopDown,
            1000,
            self.rules.clone(),
            plan,
            self.optimizer_context.clone(),
        )
        .map_err(|e| DataFusionError::Plan(format!("{:?}", e)))?;
        let optimized_plan = hep_optimizer
            .find_best_plan()
            .map_err(|e| DataFusionError::Plan(format!("{:?}", e)))?;

        to_df_logical(&optimized_plan)
            .map_err(|e| DataFusionError::Plan(format!("{:?}", e)))
            .map(Some)
    }

    fn name(&self) -> &str {
        "DFOptimizerAdapterRule"
    }
}

#[cfg(test)]
mod tests {
    use crate::rule::DFOptimizerAdapterRule;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::catalog::schema::MemorySchemaProvider;
    use datafusion::catalog::TableReference;
    use datafusion::common::ToDFSchema;
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::datasource::DefaultTableSource;
    use datafusion::logical_expr::col;
    use datafusion::optimizer::optimizer::OptimizerRule;
    use datafusion_expr::logical_plan::TableScan as DFTableScan;
    use datafusion_expr::logical_plan::{LogicalPlan, LogicalPlanBuilder};
    use datafusion_optimizer::OptimizerContext as DFOptimizerContext;
    use dolomite::optimizer::OptimizerContext;
    use dolomite::rules::{
        PushLimitOverProjectionRule, PushLimitToTableScanRule, RemoveLimitRule,
    };
    use std::sync::Arc;

    #[test]
    fn test_limit_push_down() {
        let schema = {
            let json = r#"{
                "fields": [
                    {
                        "name": "c1",
                        "nullable": false,
                        "data_type":  "Utf8",
                        "dict_id": 0,
                        "dict_is_ordered": false,
                        "metadata": {}
                    },
                    {
                        "name": "c2",
                        "nullable": false,
                        "data_type":  "Utf8",
                        "dict_id": 0,
                        "dict_is_ordered": false,
                        "metadata": {}
                    }
                ],
                "metadata": {}
            }"#;
            let schema: Schema = serde_json::from_str(json).unwrap();
            Arc::new(schema)
        };

        let table_provider = Arc::new(EmptyTable::new(Arc::new((*schema).clone())));

        // Construct datafusion logical plan
        let df_logical_plan = {
            let source = Arc::new(DefaultTableSource::new(table_provider.clone()));
            let df_scan = DFTableScan {
                table_name: TableReference::from("t1").to_owned_reference(),
                source,
                projection: None,
                projected_schema: (*schema).clone().to_dfschema_ref().unwrap(),
                filters: vec![],
                fetch: None,
            };

            LogicalPlanBuilder::from(LogicalPlan::TableScan(df_scan))
                .limit(0, Some(10))
                .unwrap()
                .project(vec![col("c1")])
                .unwrap()
                .limit(0, Some(5))
                .unwrap()
                .build()
                .unwrap()
        };

        let optimized_plan = {
            let optimizer_context = OptimizerContext {
                catalog: Arc::new(MemorySchemaProvider::new()),
            };

            optimizer_context
                .catalog
                .register_table("t1".to_string(), table_provider)
                .unwrap();

            let rule = DFOptimizerAdapterRule {
                rules: vec![
                    PushLimitOverProjectionRule::new().into(),
                    RemoveLimitRule::new().into(),
                    PushLimitToTableScanRule::new().into(),
                ],
                optimizer_context,
            };

            rule.try_optimize(&df_logical_plan, &DFOptimizerContext::default())
                .unwrap()
        };

        let expected_plan = {
            let source = Arc::new(DefaultTableSource::new(Arc::new(EmptyTable::new(
                Arc::new((*schema).clone()),
            ))));

            let df_scan = DFTableScan {
                table_name: TableReference::from("t1").to_owned_reference(),
                source,
                projection: None,
                projected_schema: (*schema).clone().to_dfschema_ref().unwrap(),
                filters: vec![],
                fetch: Some(5),
            };

            LogicalPlanBuilder::from(LogicalPlan::TableScan(df_scan))
                .project(vec![col("c1")])
                .unwrap()
                .build()
                .unwrap()
        };

        assert_eq!(
            format!("{:?}", expected_plan),
            format!("{:?}", optimized_plan.unwrap())
        );
    }
}

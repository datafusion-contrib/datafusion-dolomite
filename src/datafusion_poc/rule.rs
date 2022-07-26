use std::sync::Arc;
use datafusion::common::DataFusionError;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::optimizer::OptimizerRule;
use crate::heuristic::{HepOptimizer, MatchOrder};
use crate::optimizer::{Optimizer, OptimizerContext};
use crate::plan::{Plan, PlanNode};
use crate::rules::RuleImpl;

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
  fn optimize(&self, df_plan: &LogicalPlan, _execution_props: &ExecutionProps) ->
  datafusion::common::Result<LogicalPlan> {
    println!("Beginning to execute heuristic optimizer");
    let plan = Plan::new(Arc::new(PlanNode::try_from(df_plan)
        .map_err(|e| DataFusionError::Plan(format!("{:?}", e)))?
    ));

    // Construct heuristic optimizer here
    let hep_optimizer = HepOptimizer::new(MatchOrder::TopDown, 1000, self.rules.clone(), plan,
                                          self.optimizer_context.clone())
        .map_err(|e| DataFusionError::Plan(format!("{:?}", e)))?;
    let optimized_plan = hep_optimizer.find_best_plan()
        .map_err(|e| DataFusionError::Plan(format!("{:?}", e)))?;

    LogicalPlan::try_from(&*optimized_plan.root())
        .map_err(|e| DataFusionError::Plan(format!("{:?}", e)))
  }

  fn name(&self) -> &str {
    "DFOptimizerAdapterRule"
  }
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;
  use datafusion::arrow::datatypes::Schema;
  use datafusion::catalog::schema::MemorySchemaProvider;
  use datafusion::common::ToDFSchema;
  use datafusion::datasource::empty::EmptyTable;
  use datafusion::execution::context::ExecutionProps;
  use datafusion::logical_expr::col;
  use datafusion::logical_plan::{LogicalPlan, LogicalPlanBuilder};
  use datafusion::optimizer::optimizer::OptimizerRule;
  use serde_json::Value;
  use crate::datafusion_poc::rule::DFOptimizerAdapterRule;
  use crate::rules::{PushLimitOverProjectionRule, PushLimitToTableScanRule, RemoveLimitRule};
  use datafusion::logical_plan::plan::{TableScan as DFTableScan, DefaultTableSource};
  use crate::optimizer::OptimizerContext;

  #[test]
  fn test_limit_push_down() {
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

    // Construct datafusion logical plan
    let df_logical_plan = {
      let source = Arc::new(DefaultTableSource::new(table_provider.clone()));
      let df_scan = DFTableScan {
        table_name: "t1".to_string(),
        source,
        projection: None,
        projected_schema:
        (&*schema).clone().to_dfschema_ref().unwrap(),
        filters: vec![],
        limit: None,
      };

      LogicalPlanBuilder::from(LogicalPlan::TableScan(df_scan))
          .limit(10).unwrap()
          .project(vec![col("c1")]).unwrap()
          .limit(5).unwrap()
          .build().unwrap()
    };


    let optimized_plan = {
      let optimizer_context = OptimizerContext {
        catalog: Arc::new(MemorySchemaProvider::new())
      };

      optimizer_context.catalog.register_table("t1".to_string(), table_provider.clone()).unwrap();

      let rule = DFOptimizerAdapterRule {
        rules: vec![
          PushLimitOverProjectionRule::new().into(),
          RemoveLimitRule::new().into(),
          PushLimitToTableScanRule::new().into(),
        ],
        optimizer_context,
      };

      rule.optimize(&df_logical_plan, &ExecutionProps::new()).unwrap()
    };

    let expected_plan = {
      let source = Arc::new(DefaultTableSource::new(Arc::new(EmptyTable::new(Arc::new
          ((&*schema).clone().into())))));

      let df_scan = DFTableScan {
        table_name: "t1".to_string(),
        source,
        projection: None,
        projected_schema: (&*schema).clone().to_dfschema_ref().unwrap(),
        filters: vec![],
        limit: Some(5),
      };

      LogicalPlanBuilder::from(LogicalPlan::TableScan(df_scan))
          .project(vec![col("c1")]).unwrap()
          .build().unwrap()
    };

    assert_eq!(format!("{:?}", expected_plan), format!("{:?}", optimized_plan));
  }
}



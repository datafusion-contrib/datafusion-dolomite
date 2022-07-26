use std::sync::Arc;
use datafusion::common::DataFusionError;
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use crate::cascades::CascadesOptimizer;
use crate::datafusion_poc::plan::plan_node_to_df_physical_plan;
use crate::optimizer::{Optimizer, OptimizerContext};
use crate::plan::{Plan, PlanNode};
use crate::properties::PhysicalPropertySet;
use crate::rules::RuleImpl;
use async_trait::async_trait;


/// A query planner converting logical plan to physical plan.
///
/// It works as following:
/// ```no
/// Datafusion logical plan -> Our logical plan -> CBO -> Our physical plan -> Datafusion
/// physical plan
/// ```
pub struct DFQueryPlanner {
  rules: Vec<RuleImpl>,
  optimizer_ctx: OptimizerContext,
}

#[async_trait]
impl QueryPlanner for DFQueryPlanner {
  async fn create_physical_plan(&self, df_logical_plan: &LogicalPlan, session_state: &SessionState) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
    println!("Beginning to execute heuristic optimizer");
    let logical_plan = Plan::new(Arc::new(PlanNode::try_from(df_logical_plan)
        .map_err(|e| DataFusionError::Plan(format!("{:?}", e)))?
    ));

    let optimizer = CascadesOptimizer::new(
      PhysicalPropertySet::default(),
      self.rules.clone(),
      logical_plan,
      self.optimizer_ctx.clone(),
    );

    let physical_plan = optimizer.find_best_plan()
        .map_err(|e| DataFusionError::Plan(format!("{:?}", e)))?;

    plan_node_to_df_physical_plan(&*physical_plan.root(),
                                  session_state,
                                  &self.optimizer_ctx,
    ).await.map_err(|e| DataFusionError::Plan(format!("Physical planner error: {:?}", e)))
  }
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;
  use datafusion::arrow::datatypes::{Field, Schema};
  use datafusion::catalog::schema::MemorySchemaProvider;
  use datafusion::common::ToDFSchema;
  use datafusion::datasource::empty::EmptyTable;
  use datafusion::execution::context::{QueryPlanner, SessionState};
  use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
  use datafusion::logical_expr::{LogicalPlan};
  use datafusion::logical_plan::{JoinType, LogicalPlanBuilder};
  use datafusion::prelude::SessionConfig;
  use serde_json::Value;
  use crate::datafusion_poc::planner::DFQueryPlanner;
  use crate::optimizer::OptimizerContext;
  use crate::rules::{CommutateJoinRule, Join2HashJoinRule, Scan2TableScanRule};
  use datafusion::logical_plan::plan::{TableScan as DFTableScan, DefaultTableSource};
  use datafusion::physical_plan::displayable;

  fn table_schema(prefix: &str) -> Arc<Schema> {
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
    let new_fields: Vec<Field> = schema.fields().iter().map(|f| Field::new(format!("{}_{}", prefix, f
        .name()).as_str(), f.data_type()
                                                                               .clone(), f.is_nullable())).collect();
    Arc::new(Schema::new(new_fields))
  }

  fn table_source(prefix: &str) -> Arc<DefaultTableSource> {
    let schema = table_schema(prefix);
    let table_provider = Arc::new(EmptyTable::new(Arc::new
        ((&*schema).clone().into())));

    Arc::new(DefaultTableSource::new(table_provider.clone()))
  }

  #[tokio::test]
  async fn test_plan_join() {
    let t1_source = table_source("t1");
    let t2_source = table_source("t2");

    // Construct datafusion logical plan
    let df_logical_plan = {
      let df_scan_t1 = DFTableScan {
        table_name: "t1".to_string(),
        source: t1_source.clone(),
        projection: None,
        projected_schema: table_schema("t1").clone().to_dfschema_ref().unwrap(),
        filters: vec![],
        limit: None,
      };

      let df_scan_t1_plan = LogicalPlanBuilder::from(LogicalPlan::TableScan(df_scan_t1))
          .build().unwrap();

      let df_scan_t2 = DFTableScan {
        table_name: "t2".to_string(),
        source: t2_source.clone(),
        projection: None,
        projected_schema: table_schema("t2").clone().to_dfschema_ref().unwrap(),
        filters: vec![],
        limit: None,
      };

      LogicalPlanBuilder::from(LogicalPlan::TableScan(df_scan_t2))
          .join(&df_scan_t1_plan, JoinType::Inner, (vec!["t1_c1"], vec!["t2_c2"])).unwrap()
          .build().unwrap()
    };

    let planner = {
      let optimizer_context = OptimizerContext {
        catalog: Arc::new(MemorySchemaProvider::new())
      };

      optimizer_context.catalog.register_table("t1".to_string(), t1_source.table_provider.clone(),
      ).unwrap();
      optimizer_context.catalog.register_table("t2".to_string(), t2_source.table_provider.clone(),
      ).unwrap();

      Arc::new(DFQueryPlanner {
        rules: vec![
          CommutateJoinRule::new().into(),
          Join2HashJoinRule::new().into(),
          Scan2TableScanRule::new().into(),
        ],
        optimizer_ctx: optimizer_context,
      })
    };

    let session_state = {
      let session_config = SessionConfig::new()
          .with_target_partitions(48)
          .with_information_schema(true);
      SessionState::with_config_rt(
        session_config,
        Arc::new(RuntimeEnv::new(RuntimeConfig::default()).unwrap()),
      )
          .with_query_planner(planner.clone())
    };

    let optimized_result = planner.create_physical_plan(&df_logical_plan, &session_state).await
        .unwrap();

    let expected_plan = r#"HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(Column { name: "t2_c2", index: 1 }, Column { name: "t1_c1", index: 0 })]
  EmptyExec: produce_one_row=false
  EmptyExec: produce_one_row=false
"#;

    assert_eq!(expected_plan, format!("{}", displayable(&*optimized_result).indent()));
  }
}

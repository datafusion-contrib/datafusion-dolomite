mod schema;

use crate::utils::schema::MySchemaProvider;
use datafusion_dolomite_integration::plan::try_convert;
use datafusion_sql::planner::SqlToRel;
use datafusion_sql::sqlparser::dialect::GenericDialect;
use datafusion_sql::sqlparser::parser::Parser;
use dolomite::optimizer::Optimizer;
use dolomite::plan::Plan;
use std::sync::Arc;

pub struct TestCase {
    pub sql: String,
    pub expected_optimized_plan: String,
}

pub trait OptimizerFactory {
    type O: Optimizer;
    fn create(&self, plan: Plan) -> Self::O;
}

pub struct TestCaseRunner<F> {
    dialect: GenericDialect,
    catalog: MySchemaProvider,
    optimizer_factory: F,
}

impl<F: OptimizerFactory> TestCaseRunner<F> {
    pub fn run(&self, test_case: TestCase) {
        let original_plan = self.to_logical_plan(&test_case.sql);

        let optimized_plan = self
            .optimizer_factory
            .create(original_plan)
            .find_best_plan()
            .unwrap();
    }

    fn to_logical_plan(&self, sql: &str) -> Plan {
        let mut ast = Parser::parse_sql(&self.dialect, sql).unwrap();
        let sql_to_rel = SqlToRel::new(&self.catalog);
        let df_plan = sql_to_rel.sql_statement_to_plan(ast.remove(0)).unwrap();
        let plan_node = try_convert(&df_plan).unwrap();
        Plan::new(Arc::new(plan_node))
    }
}

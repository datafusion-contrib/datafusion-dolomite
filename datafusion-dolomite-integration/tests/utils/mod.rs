pub mod hep_optimizer;
pub mod schema;

use crate::utils::schema::MySchemaProvider;
use anyhow::Context;
use datafusion_dolomite_integration::plan::try_convert;
use datafusion_sql::planner::SqlToRel;
use datafusion_sql::sqlparser::dialect::GenericDialect;
use datafusion_sql::sqlparser::parser::Parser;
use dolomite::optimizer::Optimizer;
use dolomite::plan::explain::explain_to_string;
use dolomite::plan::Plan;
use serde::Deserialize;
use serde::Serialize;
use std::fmt::Debug;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
pub struct TestCase {
    pub sql: String,
    pub expected_optimized_plan: String,
}

pub trait OptimizerFactory {
    type O: Optimizer;
    fn create(&self, plan: Plan) -> Self::O;
}

pub struct TestCaseRunner<F> {
    /// Input file path.
    pub paths: Vec<PathBuf>,
    pub dialect: GenericDialect,
    pub catalog: Arc<MySchemaProvider>,
    pub optimizer_factory: F,
}

impl<F: OptimizerFactory> TestCaseRunner<F> {
    pub fn run(self) {
        for path in &self.paths {
            let file = File::options()
                .read(true)
                .open(&path)
                .with_context(|| format!("Failed to open test case file: {:?}", &path))
                .unwrap();

            let test_cases: Vec<TestCase> = serde_yaml::from_reader(file)
                .with_context(|| {
                    format!("Failed to load test cases from file: {:?}", &path)
                })
                .unwrap();

            for test_case in test_cases {
                self.run_case(&path, test_case);
            }
        }
    }

    fn run_case<P: AsRef<Path> + Debug>(&self, path: &P, test_case: TestCase) {
        let original_plan = self.to_logical_plan(&test_case.sql);

        let optimized_plan = self
            .optimizer_factory
            .create(original_plan)
            .find_best_plan()
            .unwrap();

        let optimized_plan_string = explain_to_string(&optimized_plan).unwrap();

        assert_eq!(
            test_case.expected_optimized_plan, optimized_plan_string,
            "Sql plan for {} in {:?} is different.",
            test_case.sql, path
        );
    }

    fn to_logical_plan(&self, sql: &str) -> Plan {
        let mut ast = Parser::parse_sql(&self.dialect, sql).unwrap();
        let sql_to_rel = SqlToRel::new(&*self.catalog);
        let df_plan = sql_to_rel.sql_statement_to_plan(ast.remove(0)).unwrap();
        let plan_node = try_convert(&df_plan).unwrap();
        Plan::new(Arc::new(plan_node))
    }
}

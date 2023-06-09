use crate::utils::hep_optimizer::HepOptimizerFactoryBuilder;
use crate::utils::schema::create_table_source;
use crate::utils::schema::MySchemaProvider;
use crate::utils::TestCaseRunner;
use arrow_schema::{DataType, Field};
use datafusion_sql::sqlparser::dialect::GenericDialect;
use dolomite::optimizer::OptimizerContext;
use dolomite::rules::{
    PushLimitOverProjectionRule, PushLimitToTableScanRule, RemoveLimitRule,
};
use maplit::hashmap;
use std::path::PathBuf;
use std::sync::Arc;

mod utils;

#[test]
fn test_limit_rules() {
    let catalog = Arc::new(MySchemaProvider::new(hashmap! {
        "t1".to_string() => create_table_source(vec![
                Field::new("f1", DataType::Int32, false),
                Field::new("f2", DataType::Utf8, false),
                Field::new("last_name", DataType::Utf8, false),
                Field::new("state", DataType::Utf8, false)])
    }));

    let runner = TestCaseRunner {
        paths: vec![PathBuf::from("resources/limit.yaml")],
        dialect: GenericDialect {},
        catalog: catalog.clone(),
        optimizer_factory: HepOptimizerFactoryBuilder::with_optimizer_context(
            OptimizerContext { catalog },
        )
        .add_rules(vec![
            PushLimitOverProjectionRule::new().into(),
            RemoveLimitRule::new().into(),
            PushLimitToTableScanRule::new().into(),
        ])
        .build(),
    };

    runner.run()
}

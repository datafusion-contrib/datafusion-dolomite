use crate::heuristic::{HepOptimizer, MatchOrder};
use crate::optimizer::OptimizerContext;
use crate::plan::Plan;
use arrow_schema::Schema;
use datafusion::catalog::schema::MemorySchemaProvider;
use datafusion::datasource::empty::EmptyTable;
use datafusion::datasource::TableProvider;
use std::collections::HashMap;
use std::sync::Arc;

pub fn build_hep_optimizer_for_test(
    tables: HashMap<String, Arc<dyn TableProvider>>,
    plan: Plan,
) -> HepOptimizer {
    let optimizer_context = OptimizerContext {
        catalog: Arc::new(MemorySchemaProvider::new()),
    };

    for (table_name, table_provider) in tables {
        optimizer_context
            .catalog
            .register_table(table_name, table_provider)
            .unwrap();
    }

    HepOptimizer::new(
        MatchOrder::TopDown,
        usize::MAX,
        vec![],
        plan,
        optimizer_context,
    )
    .unwrap()
}

pub fn table_provider_from_schema(json: &str) -> Arc<dyn TableProvider> {
    let schema = {
        let schema: Schema = serde_json::from_str(json).unwrap();
        Arc::new(schema)
    };

    Arc::new(EmptyTable::new(Arc::new((*schema).clone())))
}

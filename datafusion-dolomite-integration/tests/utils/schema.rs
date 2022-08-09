use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::schema::SchemaProvider;
use datafusion::common::DataFusionError;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{AggregateUDF, Expr, ScalarUDF, TableSource};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::{project_schema, ExecutionPlan};
use datafusion_sql::planner::ContextProvider;
use datafusion_sql::TableReference;
use dolomite::error::DFResult;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

pub struct MyTable {
    table_schema: SchemaRef,
}

pub struct MySchemaProvider {
    tables: HashMap<String, Arc<MyTable>>,
}

impl MySchemaProvider {
    pub fn new(tables: HashMap<String, Arc<MyTable>>) -> Self {
        Self { tables }
    }
}

pub fn create_table_source(fields: Vec<Field>) -> Arc<MyTable> {
    Arc::new(MyTable {
        table_schema: Arc::new(Schema::new_with_metadata(fields, HashMap::new())),
    })
}

impl ContextProvider for MySchemaProvider {
    fn get_table_provider(&self, name: TableReference) -> DFResult<Arc<dyn TableSource>> {
        match self.tables.get(name.table()) {
            Some(table) => Ok(table.clone()),
            _ => Err(DataFusionError::Plan(format!(
                "Table not found: {}",
                name.table()
            ))),
        }
    }

    fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
        None
    }

    fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
        None
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        None
    }
}

impl SchemaProvider for MySchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables.keys().map(Clone::clone).collect()
    }

    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        self.tables
            .get(name)
            .map(|t| t.clone() as Arc<dyn TableProvider>)
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}

impl TableSource for MyTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.table_schema.clone()
    }
}

#[async_trait]
impl TableProvider for MyTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.table_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _ctx: &SessionState,
        projection: &Option<Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        // even though there is no data, projections apply
        let projected_schema = project_schema(&self.table_schema, projection.as_ref())?;
        Ok(Arc::new(
            EmptyExec::new(false, projected_schema).with_partitions(1),
        ))
    }
}

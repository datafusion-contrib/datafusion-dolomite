use std::sync::Arc;

use datafusion::logical_plan::DFSchema;

#[derive(Clone, PartialEq, Debug)]
pub struct LogicalProperty {
    schema: Arc<DFSchema>,
}

impl LogicalProperty {
    pub fn new(schema: DFSchema) -> Self {
        Self {
            schema: Arc::new(schema)
        }
    }

    pub fn schema(&self) -> &DFSchema {
        &self.schema
    }
}

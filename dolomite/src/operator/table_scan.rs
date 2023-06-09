use crate::error::DolomiteResult;
use crate::operator::{
    DerivePropContext, DerivePropResult, DisplayFields, OperatorTrait,
    PhysicalOperatorTrait,
};
use crate::optimizer::Optimizer;
use crate::properties::{LogicalProperty, PhysicalPropertySet};
use anyhow::anyhow;
use datafusion::common::{DFField, DFSchema};
use futures::executor::block_on;
use std::fmt::Formatter;

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct TableScan {
    limit: Option<usize>,
    table_name: String,
}

impl TableScan {
    pub fn new<S: Into<String>>(table_name: S) -> Self {
        Self {
            limit: None,
            table_name: table_name.into(),
        }
    }

    pub fn with_limit<S: Into<String>>(table_name: S, limit: usize) -> Self {
        Self {
            limit: Some(limit),
            table_name: table_name.into(),
        }
    }

    pub fn limit(&self) -> Option<usize> {
        self.limit
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }
}

impl PhysicalOperatorTrait for TableScan {
    fn derive_properties<O: Optimizer>(
        &self,
        _context: DerivePropContext<O>,
    ) -> DolomiteResult<Vec<DerivePropResult>> {
        Ok(vec![DerivePropResult {
            output_prop: PhysicalPropertySet::default(),
            input_required_props: vec![],
        }])
    }
}

impl OperatorTrait for TableScan {
    fn derive_logical_prop<O: Optimizer>(
        &self,
        _handle: O::ExprHandle,
        optimizer: &O,
    ) -> DolomiteResult<LogicalProperty> {
        let schema = block_on(optimizer.context().catalog.table(&self.table_name))
            .ok_or_else(|| anyhow!("Table {:?} not exists", &self.table_name))?
            .schema();

        let table_fields = schema
            .fields()
            .iter()
            .map(|f| DFField::from_qualified(&self.table_name, f.clone()))
            .collect();

        let table_schema =
            DFSchema::new_with_metadata(table_fields, schema.metadata().clone())?;
        Ok(LogicalProperty::new(table_schema))
    }
}

impl DisplayFields for TableScan {
    fn display(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("");
        s.field("table_name", &self.table_name);
        if let Some(limit) = self.limit {
            s.field("limit", &limit);
        }
        s.finish()
    }
}

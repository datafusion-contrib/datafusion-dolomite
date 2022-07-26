use anyhow::{anyhow};
use datafusion::common::ToDFSchema;
use crate::cost::Cost;
use crate::error::OptResult;
use crate::operator::{DerivePropContext, DerivePropResult, OperatorTrait, PhysicalOperatorTrait};
use crate::optimizer::Optimizer;
use crate::properties::{LogicalProperty, PhysicalPropertySet};

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
  ) -> OptResult<Vec<DerivePropResult>> {
    Ok(vec![DerivePropResult {
      output_prop: PhysicalPropertySet::default(),
      input_required_props: vec![],
    }])
  }

  fn cost<O: Optimizer>(&self, _expr_handle: O::ExprHandle, _optimizer: &O) -> OptResult<Cost> {
    Ok(Cost::from(1.0))
  }
}

impl OperatorTrait for TableScan {
  fn derive_logical_prop<O: Optimizer>(&self, _handle: O::ExprHandle,
                                       optimizer: &O) -> OptResult<LogicalProperty> {
    let schema = optimizer.context().catalog.table(&self.table_name)
                        .ok_or_else(|| anyhow!("Table {:?} not exists", &self.table_name))?
                        .schema();
    Ok(LogicalProperty::new(schema.to_dfschema()?))
  }
}

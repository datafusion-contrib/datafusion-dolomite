use datafusion::prelude::JoinType;

use crate::cost::Cost;
use crate::error::OptResult;
use crate::operator::{DerivePropContext, DerivePropResult, OperatorTrait, PhysicalOperatorTrait};
use crate::optimizer::{OptExpr, OptGroup, Optimizer};
use crate::properties::{LogicalProperty, PhysicalPropertySet};
use crate::Expr;

/// Logical join operator.
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct Join {
    join_type: JoinType,
    expr: Expr,
}

impl Join {
    pub fn new(join_type: JoinType, expr: Expr) -> Self {
        Self { join_type, expr }
    }

    pub fn join_type(&self) -> JoinType {
        self.join_type
    }

    pub fn expr(&self) -> &Expr {
        &self.expr
    }
}

impl PhysicalOperatorTrait for Join {
    fn derive_properties<O: Optimizer>(
        &self,
        _context: DerivePropContext<O>,
    ) -> OptResult<Vec<DerivePropResult>> {
        Ok(vec![DerivePropResult {
            output_prop: PhysicalPropertySet::default(),
            input_required_props: vec![
                PhysicalPropertySet::default(),
                PhysicalPropertySet::default(),
            ],
        }])
    }

    fn cost<O: Optimizer>(&self, _expr_handle: O::ExprHandle, _optimizer: &O) -> OptResult<Cost> {
        Ok(Cost::from(1.0))
    }
}

impl OperatorTrait for Join {
    fn derive_logical_prop<O: Optimizer>(&self,
                                         handle: O::ExprHandle,
                                         optimizer: &O) -> OptResult<LogicalProperty> {
        let left_prop = optimizer.group_at(optimizer.expr_at(handle.clone()).input_at(0, optimizer))
            .logical_prop();
        let right_prop = optimizer.group_at(optimizer.expr_at(handle.clone()).input_at(1, optimizer))
            .logical_prop();

        let schema = left_prop.schema().join(right_prop.schema())?;

        Ok(LogicalProperty::new(schema))
    }
}

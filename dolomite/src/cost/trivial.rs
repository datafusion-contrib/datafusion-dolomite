use crate::cost::Cost;
use crate::error::DolomiteResult;
use crate::operator::Join;
use crate::operator::Operator::{Logical, Physical};
use crate::operator::PhysicalOperator::PhysicalHashJoin;
use crate::optimizer::{OptExpr, Optimizer};
use anyhow::bail;

#[derive(Default)]
pub struct SimpleCostModel {}

impl SimpleCostModel {
    pub(super) fn cost<O: Optimizer>(&self, expr: &O::Expr) -> DolomiteResult<Cost> {
        match expr.operator() {
            Physical(PhysicalHashJoin(join)) => self.hash_join_cost(join),
            Physical(_) => self.default_cost(),
            Logical(_) => bail!("No cost for logical operator."),
        }
    }
}

impl SimpleCostModel {
    fn hash_join_cost(&self, _join: &Join) -> DolomiteResult<Cost> {
        Ok(Cost::from(1.0))
    }

    fn default_cost(&self) -> DolomiteResult<Cost> {
        Ok(Cost::from(1.0))
    }
}

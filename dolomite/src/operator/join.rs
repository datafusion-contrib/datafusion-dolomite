use anyhow::bail;
use datafusion::common::ScalarValue;
use std::fmt::Formatter;

use crate::error::DolomiteResult;
use crate::operator::JoinDirection::{Left, Right};
use crate::operator::JoinType::{Anti, FullOuter, Inner, Outer, Semi};
use crate::operator::{
    DerivePropContext, DerivePropResult, DisplayFields, OperatorTrait,
    PhysicalOperatorTrait,
};
use crate::optimizer::{OptExpr, OptGroup, Optimizer};
use crate::properties::{LogicalProperty, PhysicalPropertySet};
use crate::Expr;
use datafusion::prelude::JoinType as DFJoinType;

/// See [`JoinType`].
#[derive(Debug, Hash, PartialEq, Eq, Copy, Clone)]
pub enum JoinDirection {
    Left,
    Right,
}

#[derive(Debug, Hash, PartialEq, Eq, Copy, Clone)]
pub enum JoinType {
    Inner,
    FullOuter,
    /// Left or right outer join.
    Outer(JoinDirection),
    /// Left or right anti join.
    Anti(JoinDirection),
    /// Left or right semi join.
    Semi(JoinDirection),
    /// Left or right marker join.
    ///
    /// Outputs one boolean column to indicate whether joined. For more details please refer to
    /// [The Complete Story of Joins (in HyPer)] (https://www.btw2017.informatik.uni-stuttgart.de/slidesandpapers/F1-10-37/paper_web.pdf)
    Marker(JoinDirection),
    /// Left or right single join.
    /// Output exactly one joined row. For more details please refer to
    /// [The Complete Story of Joins (in HyPer)] (https://www.btw2017.informatik.uni-stuttgart.de/slidesandpapers/F1-10-37/paper_web.pdf)
    Single(JoinDirection),
}

impl TryFrom<DFJoinType> for JoinType {
    type Error = anyhow::Error;

    fn try_from(df_join_type: DFJoinType) -> Result<Self, Self::Error> {
        Ok(match df_join_type {
            DFJoinType::Inner => Inner,
            DFJoinType::Left => Outer(Left),
            DFJoinType::Right => Outer(Right),
            DFJoinType::Full => FullOuter,
            DFJoinType::Anti => Anti(Left),
            DFJoinType::Semi => Semi(Left),
        })
    }
}

impl TryFrom<JoinType> for DFJoinType {
    type Error = anyhow::Error;

    fn try_from(join_type: JoinType) -> Result<Self, Self::Error> {
        match join_type {
            Inner => Ok(DFJoinType::Inner),
            FullOuter => Ok(DFJoinType::Full),
            Outer(Left) => Ok(DFJoinType::Left),
            Outer(Right) => Ok(DFJoinType::Right),
            Anti(Left) => Ok(DFJoinType::Anti),
            Semi(Left) => Ok(DFJoinType::Semi),
            t => bail!(
                "Unable to convert dolomite join type {:?} to datafusion join type",
                t
            ),
        }
    }
}

/// Logical join operator.
#[derive(Clone, Debug, Hash, PartialEq)]
pub struct Join {
    join_type: JoinType,
    expr: Expr,
}

impl Join {
    pub fn new(join_type: JoinType, expr: Expr) -> Self {
        Self { join_type, expr }
    }

    /// Cross product is inner join without comparison condition.
    pub fn new_cross_product() -> Self {
        Self::new(
            JoinType::Inner,
            Expr::Literal(ScalarValue::Boolean(Some(true))),
        )
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
    ) -> DolomiteResult<Vec<DerivePropResult>> {
        Ok(vec![DerivePropResult {
            output_prop: PhysicalPropertySet::default(),
            input_required_props: vec![
                PhysicalPropertySet::default(),
                PhysicalPropertySet::default(),
            ],
        }])
    }
}

impl OperatorTrait for Join {
    fn derive_logical_prop<O: Optimizer>(
        &self,
        handle: O::ExprHandle,
        optimizer: &O,
    ) -> DolomiteResult<LogicalProperty> {
        let left_prop = optimizer
            .group_at(optimizer.expr_at(handle.clone()).input_at(0, optimizer))
            .logical_prop();
        let right_prop = optimizer
            .group_at(optimizer.expr_at(handle).input_at(1, optimizer))
            .logical_prop();

        let schema = left_prop.schema().join(right_prop.schema())?;

        Ok(LogicalProperty::new(schema))
    }
}

impl DisplayFields for Join {
    fn display(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("")
            .field("join_type", &self.join_type)
            .field("expr", &self.expr)
            .finish()
    }
}

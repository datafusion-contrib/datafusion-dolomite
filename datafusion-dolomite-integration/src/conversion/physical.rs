use crate::conversion::expr_to_df_join_condition;
use anyhow::{anyhow, bail};
use datafusion::common::{DataFusionError, ToDFSchema};

use datafusion::execution::context::SessionState;

use datafusion::physical_plan::hash_join::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::join_utils::JoinOn;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use datafusion_physical_expr::create_physical_expr;
use dolomite::error::DFResult;
use dolomite::error::DolomiteResult;

use dolomite::operator::Operator::Physical;
use dolomite::operator::PhysicalOperator::{
    PhysicalHashJoin, PhysicalProjection, PhysicalTableScan,
};

use dolomite::optimizer::OptimizerContext;
use dolomite::plan::{Plan, PlanNode};
use futures::future::BoxFuture;
use std::sync::Arc;

/// Convert dolomite physical plan to datafusion physical plan.
pub fn to_df_physical<'a>(
    plan: &'a Plan,
    session_state: &'a SessionState,
    ctx: &'a OptimizerContext,
) -> BoxFuture<'a, DolomiteResult<Arc<dyn ExecutionPlan>>> {
    Box::pin(async {
        let root = plan.root();
        plan_node_to_df_physical_plan(&*root, session_state, ctx).await
    })
}

fn plan_node_to_df_physical_plan<'a>(
    plan_node: &'a PlanNode,
    session_state: &'a SessionState,
    ctx: &'a OptimizerContext,
) -> BoxFuture<'a, DolomiteResult<Arc<dyn ExecutionPlan>>> {
    Box::pin(async move {
        let mut inputs = Vec::with_capacity(plan_node.inputs().len());
        for input in plan_node.inputs().iter() {
            let input_plan =
                plan_node_to_df_physical_plan(&**input, session_state, ctx).await?;
            inputs.push(input_plan);
        }

        match plan_node.operator() {
            Physical(PhysicalProjection(projection)) => {
                let input = inputs.remove(0);
                let input_schema = input.schema();
                let input_df_schema = input_schema.clone().to_dfschema()?;

                let physical_exprs = projection
                    .expr()
                    .iter()
                    .map(|e| {
                        tuple_err((
                            create_physical_expr(
                                e,
                                &input_df_schema,
                                &*input_schema,
                                &session_state.execution_props,
                            ),
                            create_physical_name(e),
                        ))
                    })
                    .collect::<DFResult<Vec<_>>>()?;

                Ok(Arc::new(ProjectionExec::try_new(physical_exprs, input)?)
                    as Arc<dyn ExecutionPlan>)
            }
            Physical(PhysicalHashJoin(hash_join)) => {
                let physical_left = inputs.remove(0);
                let physical_right = inputs.remove(0);
                let left_df_schema = physical_left.schema().to_dfschema()?;
                let right_df_schema = physical_right.schema().to_dfschema()?;
                let join_on = expr_to_df_join_condition(hash_join.expr())?
                    .iter()
                    .map(|(l, r)| {
                        Ok((
                            datafusion_physical_expr::expressions::Column::new(
                                &l.name,
                                left_df_schema.index_of_column(l)?,
                            ),
                            datafusion_physical_expr::expressions::Column::new(
                                &r.name,
                                right_df_schema.index_of_column(r)?,
                            ),
                        ))
                    })
                    .collect::<DFResult<JoinOn>>()?;

                Ok(Arc::new(HashJoinExec::try_new(
                    physical_left,
                    physical_right,
                    join_on,
                    None,
                    &hash_join.join_type(),
                    PartitionMode::CollectLeft,
                    &true,
                )?) as Arc<dyn ExecutionPlan>)
            }
            Physical(PhysicalTableScan(table_scan)) => {
                let source =
                    ctx.catalog.table(table_scan.table_name()).ok_or_else(|| {
                        anyhow!(format!("Table not found: {}", table_scan.table_name()))
                    })?;
                Ok(source
                    .scan(session_state, &None, &[], None)
                    .await
                    .map_err(|e| anyhow!(e))?
                    as Arc<dyn ExecutionPlan>)
            }
            op => bail!("Can't convert plan to data fusion logical plan: {:?}", op),
        }
    })
}

/// A simplified port of `DefaultPlanner`
fn create_physical_name(e: &Expr) -> DFResult<String> {
    match e {
        Expr::Column(c) => Ok(c.flat_name()),
        e => Err(DataFusionError::Internal(format!(
            "Create physical expression name from {:?} \
    not supported!",
            e
        ))),
    }
}

/// Copied from data fusion.
fn tuple_err<T, R>(value: (DFResult<T>, DFResult<R>)) -> DFResult<(T, R)> {
    match value {
        (Ok(e), Ok(e1)) => Ok((e, e1)),
        (Err(e), Ok(_)) => Err(e),
        (Ok(_), Err(e1)) => Err(e1),
        (Err(e), Err(_)) => Err(e),
    }
}

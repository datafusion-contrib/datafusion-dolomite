use std::sync::Arc;
use anyhow::{anyhow, bail};
use datafusion::common::{Column, DataFusionError, ScalarValue, ToDFSchema};
use datafusion::datasource::empty::EmptyTable;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{and, LogicalPlan};
use datafusion::logical_plan::{JoinConstraint};
use crate::error::OptResult;
use crate::Expr;
use crate::Expr::{Column as ExprColumn};
use crate::operator::{Join, Limit, LogicalOperator, Projection, TableScan};
use crate::operator::LogicalOperator::{LogicalJoin, LogicalLimit, LogicalProjection, LogicalScan};
use crate::operator::Operator::{Logical, Physical};
use crate::plan::{PlanNode, PlanNodeIdGen};
use datafusion::logical_plan::plan::{Projection as DFProjection, Limit as DFLimit, Join as
DFJoin, TableScan as DFTableScan, DefaultTableSource};
use datafusion::logical_plan::{Operator as DFOperator};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::hash_join::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::join_utils::JoinOn;
use datafusion::physical_plan::planner::create_physical_expr;
use datafusion::physical_plan::projection::ProjectionExec;
use futures::future::BoxFuture;
use crate::datafusion_poc::DFResult;
use crate::operator::PhysicalOperator::{PhysicalHashJoin, PhysicalProjection, PhysicalTableScan};
use crate::optimizer::OptimizerContext;

/// Convert data fusion logical plan to our plan.
impl<'a> TryFrom<&'a LogicalPlan> for PlanNode {
  type Error = anyhow::Error;

  fn try_from(value: &'a LogicalPlan) -> Result<Self, anyhow::Error> {
    let mut plan_node_id_gen = PlanNodeIdGen::new();
    df_logical_plan_to_plan_node(&value, &mut plan_node_id_gen)
  }
}

fn df_logical_plan_to_plan_node(df_plan: &LogicalPlan, id_gen: &mut PlanNodeIdGen) ->
OptResult<PlanNode> {
  let id = id_gen.next();
  let (operator, inputs) = match df_plan {
    LogicalPlan::Projection(projection) => {
      let operator = LogicalOperator::LogicalProjection(Projection::new(projection.expr.clone()));
      let inputs = vec![df_logical_plan_to_plan_node(&projection.input, id_gen)?];
      (operator, inputs)
    }
    LogicalPlan::Limit(limit) => {
      let operator = LogicalOperator::LogicalLimit(Limit::new(limit.n));
      let inputs = vec![df_logical_plan_to_plan_node(&limit.input, id_gen)?];
      (operator, inputs)
    }
    LogicalPlan::Join(join) => {
      let join_cond = join.on.iter()
          .map(|(left, right)| ExprColumn(left.clone()).eq(ExprColumn(right.clone())))
          .reduce(|a, b| and(a, b))
          .unwrap_or(Expr::Literal(ScalarValue::Boolean(Some(true))));
      let operator = LogicalOperator::LogicalJoin(Join::new(join.join_type, join_cond));
      let inputs = vec![df_logical_plan_to_plan_node(&join.left, id_gen)?,
                        df_logical_plan_to_plan_node(&join.right, id_gen)?,
      ];
      (operator, inputs)
    }
    LogicalPlan::TableScan(scan) => {
      let operator = LogicalOperator::LogicalScan(TableScan::new(&scan.table_name));
      let inputs = vec![];
      (operator, inputs)
    }
    plan => {
      bail!("Unsupported datafusion logical plan: {:?}", plan);
    }
  };

  Ok(PlanNode::new(id, Logical(operator), inputs.into_iter().map(Arc::new).collect()))
}

/// Converting logical plan to df logical plan.
impl<'a> TryFrom<&'a PlanNode> for LogicalPlan {
  type Error = anyhow::Error;

  fn try_from(plan_node: &'a PlanNode) -> OptResult<Self> {
    plan_node_to_df_logical_plan(plan_node)
  }
}

fn expr_to_df_join_condition(expr: &Expr) -> OptResult<Vec<(Column, Column)>> {
  match expr {
    Expr::BinaryExpr {
      left,
      op,
      right
    } if matches!(op, DFOperator::Eq) => {
      match (&**left, &**right) {
        (ExprColumn(left_col), ExprColumn(right_col)) => Ok(vec![(left_col.clone(), right_col
            .clone()
        )]),
        _ => bail!("Unsupported join condition to convert to datafusion join condition: {:?}",
          expr)
      }
    }
    _ => bail!("Unsupported join condition to convert to datafusion join condition: {:?}",
          expr)
  }
}

fn plan_node_to_df_logical_plan(plan_node: &PlanNode) -> OptResult<LogicalPlan> {
  let mut inputs = plan_node.inputs().iter()
      .map(|p| LogicalPlan::try_from(&**p))
      .collect::<OptResult<Vec<LogicalPlan>>>()?;

  match plan_node.operator() {
    Logical(LogicalProjection(projection)) => {
      let df_projection = DFProjection {
        expr: Vec::from(projection.expr()),
        input: Arc::new(inputs.remove(0)),
        schema: Arc::new(plan_node.logical_prop().unwrap().schema().clone()),
        alias: None,
      };

      Ok(LogicalPlan::Projection(df_projection))
    }
    Logical(LogicalLimit(limit)) => {
      let df_limit = DFLimit {
        n: limit.limit(),
        input: Arc::new(inputs.remove(0)),
      };

      Ok(LogicalPlan::Limit(df_limit))
    }
    Logical(LogicalJoin(join)) => {
      let df_join = DFJoin {
        left: Arc::new(inputs.remove(0)),
        right: Arc::new(inputs.remove(0)),
        on: expr_to_df_join_condition(join.expr())?,
        join_type: join.join_type(),
        join_constraint: JoinConstraint::On,
        schema: Arc::new(plan_node.logical_prop().unwrap().schema().clone()),
        null_equals_null: true,
      };

      Ok(LogicalPlan::Join(df_join))
    }
    Logical(LogicalScan(scan)) => {
      let schema = Arc::new(plan_node.logical_prop().unwrap().schema().clone());
      let source = Arc::new(DefaultTableSource::new(Arc::new(EmptyTable::new(Arc::new
          ((&*schema).clone().into())))));
      let df_scan = DFTableScan {
        table_name: scan.table_name().to_string(),
        source,
        projection: None,
        projected_schema: schema,
        filters: vec![],
        limit: scan.limit(),
      };

      Ok(LogicalPlan::TableScan(df_scan))
    }
    op => bail!("Can't convert plan to data fusion logical plan: {:?}", op)
  }
}

pub fn plan_node_to_df_physical_plan<'a>(
  plan_node: &'a PlanNode,
  session_state: &'a SessionState,
  ctx: &'a OptimizerContext) -> BoxFuture<'a, OptResult<Arc<dyn ExecutionPlan>>> {
  Box::pin(async move {
    let mut inputs = Vec::with_capacity(plan_node.inputs().len());
    for input in plan_node.inputs().iter() {
      let input_plan = plan_node_to_df_physical_plan(&**input, session_state, ctx).await?;
      inputs.push(input_plan);
    }

    match plan_node.operator() {
      Physical(PhysicalProjection(projection)) => {
        let input = inputs.remove(0);
        let input_schema = input.schema();
        let input_df_schema = input_schema.clone().to_dfschema()?;

        let physical_exprs = projection.expr()
            .iter()
            .map(|e| tuple_err((create_physical_expr(e, &input_df_schema, &*input_schema,
                                                     &session_state
                                                         .execution_props), create_physical_name(e))))
            .collect::<DFResult<Vec<_>>>()?;

        Ok(Arc::new(ProjectionExec::try_new(
          physical_exprs,
          input,
        )?) as Arc<dyn ExecutionPlan>)
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
                datafusion_physical_expr::expressions::Column::new(&l.name, left_df_schema
                    .index_of_column
                    (l)?),
                datafusion_physical_expr::expressions::Column::new(&r.name, right_df_schema.index_of_column(r)?),
              ))
            })
            .collect::<DFResult<JoinOn>>()?;

        Ok(Arc::new(HashJoinExec::try_new(
          physical_left,
          physical_right,
          join_on,
          &hash_join.join_type(),
          PartitionMode::CollectLeft,
          &true,
        )?) as Arc<dyn ExecutionPlan>)
      }
      Physical(PhysicalTableScan(table_scan)) => {
        let source = ctx.catalog.table(table_scan.table_name())
            .ok_or_else(|| anyhow!(format!("Table not found: {}", table_scan
              .table_name())))?;
        Ok(source.scan(&None, &vec![], None).await
            .map_err(|e| anyhow!(e))? as Arc<dyn ExecutionPlan>)
      }
      op => bail!("Can't convert plan to data fusion logical plan: {:?}", op)
    }
  })
}

/// A simplified port of `DefaultPlanner`
fn create_physical_name(e: &Expr) -> DFResult<String> {
  match e {
    Expr::Column(c) => {
      Ok(c.flat_name())
    }
    e => Err(DataFusionError::Internal(format!("Create physical expression name from {:?} \
    not supported!", e)))
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

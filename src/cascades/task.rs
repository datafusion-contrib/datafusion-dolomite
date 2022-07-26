use anyhow::bail;
use log::info;
use std::mem::swap;

use crate::cascades::binding::Binding;
use crate::cascades::memo::GroupExprKey;
use crate::cascades::task::OptimizeInputsTaskState::{
    AfterOptimizeInput, BeforeOptimizeInput, Init, Invalid, OptimizeSelf,
};
use crate::cascades::task::TaskControl::{Done, Yield};
use crate::cascades::{CascadesOptimizer, GroupExprId, GroupId};
use crate::cost::Cost;
use crate::error::OptResult;
use crate::operator::PhysicalOperatorTrait;
use crate::operator::{DerivePropContext, DerivePropResult, Operator};
use crate::optimizer::OptExpr;
use crate::properties::PhysicalPropertySet;
use crate::rules::{Rule, RuleImpl, RuleResult};
use enum_dispatch::enum_dispatch;
use itertools::Itertools;

#[enum_dispatch]
pub(super) enum TaskImpl {
    ApplyRuleTask,
    OptimizeExpressionTask,
    OptimizeInputsTask,
    ExploreGroupTask,
    OptimizeGroupTask,
}

enum TaskControl {
    Yield {
        this: TaskImpl,
        dependencies: Vec<TaskImpl>,
    },
    Done {
        dependencies: Vec<TaskImpl>,
    },
}

impl TaskControl {
    fn done() -> Self {
        Done {
            dependencies: vec![],
        }
    }

    fn done_with_deps(deps: Vec<TaskImpl>) -> Self {
        Done { dependencies: deps }
    }
}

#[enum_dispatch(TaskImpl)]
trait Task {
    fn execute(self, ctx: &mut CascadesOptimizer) -> OptResult<TaskControl>;
}

pub(super) fn schedule(context: &mut CascadesOptimizer, root: TaskImpl) -> OptResult<()> {
    let mut tasks = Vec::new();
    tasks.push(root);

    while let Some(cur_task) = tasks.pop() {
        match cur_task.execute(context)? {
            Yield {
                this,
                mut dependencies,
            } => {
                tasks.push(this);
                tasks.append(&mut dependencies);
            }
            Done { mut dependencies } => {
                tasks.append(&mut dependencies);
            }
        }
    }

    Ok(())
}

pub(super) struct ApplyRuleTask {
    rule: RuleImpl,
    /// The logical group expression to apply rule to.
    group_expr_id: GroupExprId,
    required_prop: PhysicalPropertySet,
    upper_bound: Cost,
}

impl Task for ApplyRuleTask {
    fn execute(self, ctx: &mut CascadesOptimizer) -> OptResult<TaskControl> {
        if ctx.memo[self.group_expr_id].is_rule_applied(self.rule.rule_id()) {
            return Ok(TaskControl::done());
        }

        info!(
            "Beginning to apply rule {:?} to group expression {:?}",
            self.rule, self.group_expr_id
        );

        let opt_exprs = {
            Binding::new(self.group_expr_id, self.rule.pattern(), &ctx.memo)
                .into_iter()
                .collect::<Vec<_>>()
        };
        let mut other_tasks = Vec::new();

        for opt_node in opt_exprs {
            let mut result = RuleResult::new();
            self.rule.apply(opt_node, ctx, &mut result)?;
            for result_node in result.results() {
                info!(
                    "Result of applying rule {:?} to group expression {:?}: {:?}",
                    self.rule, self.group_expr_id, result_node
                );
                let group_expr_id = {
                    ctx.memo
                        .insert_opt_expression(&result_node, Some(self.group_expr_id.group_id))
                };

                if ctx.memo[group_expr_id].is_logical() {
                    other_tasks.push(
                        OptimizeExpressionTask {
                            group_expr_id: self.group_expr_id,
                            required_prop: self.required_prop.clone(),
                            upper_bound: self.upper_bound,
                        }
                        .into(),
                    );
                } else {
                    other_tasks.push(
                        OptimizeInputsTask {
                            group_expr_id,
                            required_prop: self.required_prop.clone(),
                            upper_bound: self.upper_bound,
                            state: Init,
                        }
                        .into(),
                    );
                }
            }
        }

        // This method should only be called after finishing apply rules to all bindings,
        // otherwise bindings maybe invalid.
        // TODO: Enable group merging later
        // ctx.memo.merge_duplicate_groups();

        ctx.memo[self.group_expr_id].set_rule_applied(self.rule.rule_id());
        Ok(TaskControl::done_with_deps(other_tasks))
    }
}

/// Optimize a logical group expression by applying rules.
///
/// If `exploring` is true, only exploring rules are applied.
/// Otherwise both exploration and implementation rules will be applied.
pub(super) struct OptimizeExpressionTask {
    /// Logical group expression to be optimized.
    group_expr_id: GroupExprId,
    required_prop: PhysicalPropertySet,
    upper_bound: Cost,
}

impl Task for OptimizeExpressionTask {
    fn execute(self, ctx: &mut CascadesOptimizer) -> OptResult<TaskControl> {
        let group_expr = &ctx.memo[self.group_expr_id];
        let apply_rule_tasks = ctx
            .rules
            .iter()
            .filter(|rule| !group_expr.is_rule_applied(rule.rule_id()))
            .sorted_by_key(|rule| rule.rule_promise() as u32)
            .map(|rule| {
                ApplyRuleTask {
                    rule: rule.clone(),
                    group_expr_id: self.group_expr_id,
                    required_prop: self.required_prop.clone(),
                    upper_bound: self.upper_bound,
                }
                .into()
            })
            .collect::<Vec<_>>();

        let explore_input_group_tasks = group_expr
            .input_group_ids()
            .map(|group_id| {
                ExploreGroupTask {
                    group_id,
                    required_prop: self.required_prop.clone(),
                    upper_bound: self.upper_bound,
                }
                .into()
            })
            .collect::<Vec<_>>();

        let mut tasks = Vec::<TaskImpl>::with_capacity(
            explore_input_group_tasks.len() + apply_rule_tasks.len(),
        );

        tasks.extend(apply_rule_tasks.into_iter());
        // We put explore inputs task before apply rule tasks
        tasks.extend(explore_input_group_tasks.into_iter());

        Ok(TaskControl::done_with_deps(tasks))
    }
}

/// Optimize physical group expression for required property.
#[derive(Debug)]
pub(super) struct OptimizeInputsTask {
    /// Physical group expression id to be optimized.
    group_expr_id: GroupExprId,
    /// Required property
    required_prop: PhysicalPropertySet,
    upper_bound: Cost,
    state: OptimizeInputsTaskState,
}

#[derive(Debug)]
enum OptimizeInputsTaskState {
    Init,
    BeforeOptimizeInput {
        derive_results: Vec<DerivePropResult>,
        derive_idx: usize,
        input_idx: usize,
        accumulated_cost: Cost,
        best_input_group_expr: Vec<GroupExprId>,
    },
    AfterOptimizeInput {
        derive_results: Vec<DerivePropResult>,
        derive_idx: usize,
        input_idx: usize,
        accumulated_cost: Cost,
        best_input_group_expr: Vec<GroupExprId>,
    },
    OptimizeSelf {
        derive_results: Vec<DerivePropResult>,
        derive_idx: usize,
        accumulated_cost: Cost,
        best_input_group_expr: Vec<GroupExprId>,
    },
    Invalid,
}

impl OptimizeInputsTask {
    fn do_init(mut self, ctx: &mut CascadesOptimizer) -> OptResult<TaskControl> {
        println!("Current state {:?} for OptimizeInputsTask", &self);
        let memo = &ctx.memo;
        // Derive children required properties
        let operator = memo[self.group_expr_id].operator().as_physical().unwrap();

        let derive_results = operator.derive_properties(DerivePropContext {
            required_prop: &self.required_prop,
            expr_handle: self.group_expr_id,
            optimizer: ctx,
        })?;

        let operator_cost = self.operator_cost(ctx)?;
        let new_state = if memo[self.group_expr_id].inputs().len() == 0 {
            // Without children we should go to optimize self directly
            OptimizeSelf {
                derive_results,
                derive_idx: 0,
                accumulated_cost: operator_cost,
                best_input_group_expr: vec![],
            }
        } else {
            BeforeOptimizeInput {
                derive_results,
                derive_idx: 0,
                input_idx: 0,
                accumulated_cost: operator_cost,
                best_input_group_expr: Vec::with_capacity(memo[self.group_expr_id].inputs_len(ctx)),
            }
        };

        self.state = new_state;

        Ok(Yield {
            this: self.into(),
            dependencies: vec![],
        })
    }

    fn do_before_optimize_input(mut self, ctx: &CascadesOptimizer) -> OptResult<TaskControl> {
        println!("Current state {:?} for OptimizeInputsTask", &self);
        let mut new_state = Invalid;
        swap(&mut new_state, &mut self.state);
        match new_state {
            BeforeOptimizeInput {
                derive_results,
                derive_idx,
                input_idx,
                accumulated_cost,
                best_input_group_expr,
            } => {
                self.state = AfterOptimizeInput {
                    derive_results,
                    derive_idx,
                    input_idx,
                    accumulated_cost,
                    best_input_group_expr,
                };

                let task = OptimizeGroupTask {
                    group_id: ctx.memo[self.group_expr_id].input_at(input_idx, ctx),
                    required_prop: self.required_prop.clone(),
                    upper_bound: self.upper_bound - accumulated_cost,
                }
                .into();

                Ok(Yield {
                    this: self.into(),
                    dependencies: vec![task],
                })
            }
            _ => bail!("Should not happen!"),
        }
    }

    fn do_after_optimize_input(mut self, ctx: &CascadesOptimizer) -> OptResult<TaskControl> {
        println!("Current state {:?} for OptimizeInputsTask", &self);
        let mut new_state = Invalid;
        swap(&mut new_state, &mut self.state);
        match new_state {
            AfterOptimizeInput {
                derive_results,
                derive_idx,
                input_idx,
                mut accumulated_cost,
                mut best_input_group_expr,
            } => {
                let input_group_id = ctx.memo[self.group_expr_id].input_at(input_idx, ctx);
                let input_required_prop =
                    &derive_results[derive_idx].input_required_props[input_idx];

                if let Some(winner) = ctx.memo[input_group_id].winner(&input_required_prop) {
                    // Found a good plan for this required property
                    best_input_group_expr.push(winner.group_expr_id);
                    accumulated_cost += winner.lowest_cost;

                    // last input of current derive result
                    if (input_idx + 1) == ctx.memo[self.group_expr_id].inputs_len(ctx) {
                        // Go to optimize self for current derive result
                        self.state = OptimizeSelf {
                            derive_results,
                            derive_idx,
                            accumulated_cost,
                            best_input_group_expr,
                        };
                    } else {
                        // Go to next input
                        self.state = BeforeOptimizeInput {
                            derive_results,
                            derive_idx,
                            input_idx: input_idx + 1,
                            accumulated_cost,
                            best_input_group_expr,
                        }
                    }
                } else {
                    // We can't find a good enough plan for this required property, so move to
                    // next derive result
                    if (derive_idx + 1) < derive_results.len() {
                        self.state = BeforeOptimizeInput {
                            derive_results,
                            derive_idx: derive_idx + 1,
                            input_idx: 0,
                            accumulated_cost: self.operator_cost(ctx)?,
                            best_input_group_expr: Vec::with_capacity(self.inputs_len(ctx)),
                        }
                    }
                }

                if matches!(self.state, Invalid) {
                    Ok(TaskControl::done())
                } else {
                    Ok(Yield {
                        this: self.into(),
                        dependencies: vec![],
                    })
                }
            }
            _ => bail!("Should not compute to this state"),
        }
    }

    fn do_optimize_self(mut self, ctx: &mut CascadesOptimizer) -> OptResult<TaskControl> {
        println!("Current state {:?} for OptimizeInputsTask", &self);
        let mut new_state = Invalid;
        swap(&mut new_state, &mut self.state);
        match new_state {
            OptimizeSelf {
                derive_results,
                derive_idx,
                mut accumulated_cost,
                best_input_group_expr: _best_input_group_expr,
            } => {
                loop {
                    if accumulated_cost > self.upper_bound {
                        // Go to next derive result
                        break;
                    }

                    let derive_result = &derive_results[derive_idx];
                    ctx.memo[self.group_expr_id.group_id].update_winner(
                        self.group_expr_id,
                        &derive_result.output_prop,
                        &derive_result.input_required_props,
                        accumulated_cost,
                    );

                    // Try to append enforcers.
                    let enforcers = PhysicalPropertySet::append_enforcers(
                        &self.required_prop,
                        &derive_result.output_prop,
                    );

                    if !enforcers.is_empty() {
                        let mut cur_output_prop = derive_result.output_prop.clone();
                        for enforcer in enforcers {
                            let group_expr_key = GroupExprKey {
                                operator: Operator::Physical(enforcer.operator.clone()),
                                inputs: vec![self.group_expr_id.group_id],
                            };

                            let group_expr_id = ctx.memo.insert_group_expression(
                                group_expr_key,
                                Some(self.group_expr_id.group_id),
                            );

                            accumulated_cost += enforcer.operator.cost(group_expr_id, ctx)?;
                            ctx.memo[self.group_expr_id.group_id].update_winner(
                                self.group_expr_id,
                                &enforcer.output_prop,
                                &vec![cur_output_prop.clone()],
                                accumulated_cost,
                            );
                            cur_output_prop = enforcer.output_prop.clone();
                        }
                    }

                    if accumulated_cost < self.upper_bound {
                        self.upper_bound = accumulated_cost;
                    }

                    break;
                }

                if (derive_idx + 1) == derive_results.len() {
                    Ok(TaskControl::done())
                } else {
                    self.state = BeforeOptimizeInput {
                        derive_results,
                        derive_idx: derive_idx + 1,
                        input_idx: 0,
                        accumulated_cost: self.operator_cost(ctx)?,
                        best_input_group_expr: Vec::with_capacity(self.inputs_len(ctx)),
                    };

                    Ok(Yield {
                        this: self.into(),
                        dependencies: vec![],
                    })
                }
            }
            _ => bail!("Should not compute to this state"),
        }
    }

    fn operator_cost(&self, ctx: &CascadesOptimizer) -> OptResult<Cost> {
        ctx.memo[self.group_expr_id]
            .operator()
            .as_physical()
            .unwrap()
            .cost(self.group_expr_id, ctx)
    }

    fn inputs_len(&self, ctx: &CascadesOptimizer) -> usize {
        ctx.memo[self.group_expr_id].inputs_len(ctx)
    }
}

impl Task for OptimizeInputsTask {
    fn execute(self, ctx: &mut CascadesOptimizer) -> OptResult<TaskControl> {
        match self.state {
            Init => self.do_init(ctx),
            BeforeOptimizeInput { .. } => self.do_before_optimize_input(ctx),
            AfterOptimizeInput { .. } => self.do_after_optimize_input(ctx),
            OptimizeSelf { .. } => self.do_optimize_self(ctx),
            Invalid => bail!("Should not happen!"),
        }
    }
}

/// Optimizes a group for [`PhysicalPropertySet`].
pub(super) struct OptimizeGroupTask {
    group_id: GroupId,
    /// Required property
    required_prop: PhysicalPropertySet,
    upper_bound: Cost,
}

impl OptimizeGroupTask {
    pub(super) fn new(
        group_id: GroupId,
        required_prop: PhysicalPropertySet,
        upper_bound: Cost,
    ) -> Self {
        Self {
            group_id,
            required_prop,
            upper_bound,
        }
    }
}

impl Task for OptimizeGroupTask {
    fn execute(self, ctx: &mut CascadesOptimizer) -> OptResult<TaskControl> {
        info!(
            "Beginning to optimize group {:?} for physical property: {:?}",
            self.group_id, self.required_prop
        );

        let group = &ctx.memo[self.group_id];
        if let Some(result) = group.winner(&self.required_prop) {
            info!(
                "Winner for physical property {:?} in group {:?} found: {:?}, just return",
                self.required_prop, self.group_id, result
            );
            return Ok(TaskControl::done());
        }

        info!(
            "Winner for physical property {:?} in group {:?} not found, do other optimization.",
            self.required_prop, self.group_id
        );
        let mut tasks = Vec::with_capacity(group.expr_count());

        for group_expr_id in group.logical_group_expr_ids() {
            tasks.push(
                OptimizeExpressionTask {
                    group_expr_id,
                    required_prop: self.required_prop.clone(),
                    upper_bound: self.upper_bound,
                }
                .into(),
            );
        }

        // We run physical group optimization first so that we can set cost upper bound to do
        // early pruning.
        for group_expr_id in group.physical_group_expr_ids() {
            tasks.push(
                OptimizeInputsTask {
                    group_expr_id,
                    required_prop: self.required_prop.clone(),
                    upper_bound: self.upper_bound,
                    state: Init,
                }
                .into(),
            );
        }

        Ok(TaskControl::done_with_deps(tasks))
    }
}

/// Explores a group by applying exploring rules.
pub(super) struct ExploreGroupTask {
    group_id: GroupId,
    required_prop: PhysicalPropertySet,
    upper_bound: Cost,
}

impl Task for ExploreGroupTask {
    fn execute(self, ctx: &mut CascadesOptimizer) -> OptResult<TaskControl> {
        if ctx.memo[self.group_id].explored {
            return Ok(TaskControl::done());
        }

        let tasks = ctx.memo[self.group_id]
            .logical_group_expr_ids()
            .into_iter()
            .map(|group_expr_id| {
                OptimizeExpressionTask {
                    group_expr_id,
                    required_prop: self.required_prop.clone(),
                    upper_bound: self.upper_bound,
                }
                .into()
            })
            .collect();

        // This is correct since currently we have only single thread scheduler.
        ctx.memo[self.group_id].explored = true;

        Ok(TaskControl::done_with_deps(tasks))
    }
}

#[cfg(test)]
mod tests {
    use crate::cascades::task::{ApplyRuleTask, Task};
    use crate::cascades::{CascadesOptimizer, GroupId};
    use crate::cost::INF;
    use crate::operator::Join;
    use crate::operator::LogicalOperator::LogicalJoin;
    use crate::operator::Operator::Logical;
    use crate::plan::LogicalPlanBuilder;
    use crate::properties::PhysicalPropertySet;
    use crate::rules::CommutateJoinRule;
    use datafusion::logical_plan::Operator::Eq;
    use datafusion::logical_plan::{binary_expr, JoinType};
    use datafusion::prelude::col;

    #[test]
    fn test_apply_rule_task() {
        let plan = {
            let mut builder = LogicalPlanBuilder::new();
            let right = builder.scan(None, "t1").build().root();
            builder
                .scan(None, "t2")
                .join(
                    JoinType::Inner,
                    binary_expr(col("t1.c1"), Eq, col("t2.c2")),
                    right,
                )
                .build()
        };

        let mut optimizer = CascadesOptimizer::new_for_test(plan);

        let task = ApplyRuleTask {
            rule: CommutateJoinRule::new().into(),
            group_expr_id: optimizer.memo[optimizer.memo.root_group_id()].logical_group_expr_ids()
                [0],
            required_prop: PhysicalPropertySet::default(),
            upper_bound: INF,
        };

        task.execute(&mut optimizer).unwrap();

        let root_group = &optimizer.memo[optimizer.memo.root_group_id()];
        assert_eq!(2, root_group.logical_group_exprs.len());

        let group_expressions = root_group.logical_group_expr_ids();

        // First group expression
        assert_eq!(
            &Logical(LogicalJoin(Join::new(
                JoinType::Inner,
                binary_expr(col("t1.c1"), Eq, col("t2.c2"))
            ))),
            optimizer.memo[group_expressions[0]].operator()
        );
        assert_eq!(
            vec![GroupId(0), GroupId(1)],
            optimizer.memo[group_expressions[0]].inputs()
        );

        // Second group expression
        assert_eq!(
            &Logical(LogicalJoin(Join::new(
                JoinType::Inner,
                binary_expr(col("t1.c1"), Eq, col("t2.c2"))
            ))),
            optimizer.memo[group_expressions[1]].operator()
        );
        assert_eq!(
            vec![GroupId(1), GroupId(0)],
            optimizer.memo[group_expressions[1]].inputs()
        );
    }
}

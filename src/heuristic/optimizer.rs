use std::collections::HashMap;
use anyhow::{ensure};
use petgraph::Direction;


use crate::error::OptResult;
use crate::heuristic::binding::Binding;
use crate::heuristic::graph::{HepOptimizerNode, PlanGraph};
use crate::heuristic::HepNodeId;
use crate::operator::OperatorTrait;
use crate::optimizer::{OptExpr, Optimizer, OptimizerContext};
use crate::plan::{Plan, PlanNodeId, PlanNodeRef};

use crate::rules::{OptExpression, Rule, RuleImpl, RuleResult};
use crate::rules::OptExprNode::{ExprHandleNode, GroupHandleNode, OperatorNode};


/// Match order of plan tree.
#[derive(Copy, Clone)]
pub enum MatchOrder {
  BottomUp,
  TopDown,
}

pub struct HepOptimizer {
  match_order: MatchOrder,
  /// Max number of iteration
  max_iter_times: usize,
  rules: Vec<RuleImpl>,
  pub(super) graph: PlanGraph,
  context: OptimizerContext,
}

impl Optimizer for HepOptimizer {
  type Expr = HepOptimizerNode;
  type ExprHandle = HepNodeId;
  type Group = HepOptimizerNode;
  type GroupHandle = HepNodeId;

  fn context(&self) -> &OptimizerContext {
    &self.context
  }

  fn group_at(&self, group_handle: HepNodeId) -> &HepOptimizerNode {
    &self.graph.graph[group_handle]
  }

  fn expr_at(&self, expr_handle: HepNodeId) -> &HepOptimizerNode {
    &self.graph.graph[expr_handle]
  }

  fn find_best_plan(mut self) -> OptResult<Plan> {
    for _times in 0..self.max_iter_times {
      // The plan no longer changes after iteration
      let mut fixed_point = true;
      let node_ids = self.graph.nodes_iter(self.match_order.clone());
      for node_id in node_ids {
        let expr_handle = node_id;

        for rule in &*self.rules.clone() {
          println!(
            "Trying to apply rule {:?} to expression {:?}",
            rule,
            self.expr_at(expr_handle).operator()
          );
          if self.apply_rule(rule.clone(), expr_handle.clone())? {
            println!(
              "Plan after applying rule {:?} is {:?}",
              rule,
              self.graph.to_plan()
            );
            fixed_point = false;
            break;
          } else {
            println!(
              "Skipped applying rule {:?} to expression {:?}",
              rule,
              self.expr_at(expr_handle).operator()
            );
          }
        }

        if !fixed_point {
          break;
        }
      }

      if fixed_point {
        break;
      }
    }

    Ok(self.graph.to_plan())
  }
}

impl HepOptimizer {
  pub fn new(
    match_order: MatchOrder,
    max_iter_times: usize,
    rules: Vec<RuleImpl>,
    plan: Plan,
    context: OptimizerContext,
  ) -> OptResult<Self> {
    let mut optimizer = Self {
      match_order,
      max_iter_times,
      rules,
      graph: PlanGraph::default(),
      context,
    };
    optimizer.init_with_plan(plan)?;
    Ok(optimizer)
  }

  fn apply_rule(&mut self, rule: RuleImpl, expr_handle: HepNodeId) -> OptResult<bool> {
    let original_hep_node_id = expr_handle;
    if let Some(opt_node) = Binding::new(expr_handle, &*rule.pattern(), self).next() {
      let mut results = RuleResult::new();
      rule.apply(opt_node, self, &mut results)?;

      for (idx, new_expr) in results.results().enumerate() {
        ensure!(
                    idx < 1,
                    "Rewrite rule should not return no more than 1 result."
                );
        return self.replace_opt_expression(new_expr, original_hep_node_id);
      }

      // No transformation generated.
      return Ok(false);
    } else {
      Ok(false)
    }
  }

  /// Replace relational expression with optimizer rule result.
  ///
  /// # Return
  ///
  /// The return value indicates whether graph changed.
  fn replace_opt_expression(
    &mut self,
    opt_node: OptExpression<HepOptimizer>,
    origin_node_id: HepNodeId,
  ) -> OptResult<bool> {
    let new_hep_node_id = self.insert_opt_node(&opt_node)?;
    if new_hep_node_id != origin_node_id {
      // Redirect parents's child to new node
      let parent_node_ids: Vec<HepNodeId> = self
          .graph
          .graph
          .neighbors_directed(origin_node_id, Direction::Incoming)
          .collect();
      for parent in parent_node_ids {
        self.graph.graph.add_edge(parent, new_hep_node_id, ());
      }
      self.graph.graph.remove_node(origin_node_id);

      if self.graph.root == origin_node_id {
        self.graph.root = new_hep_node_id;
      }

      Ok(true)
    } else {
      Ok(false)
    }
  }

  fn insert_opt_node(&mut self, opt_expr: &OptExpression<HepOptimizer>) -> OptResult<HepNodeId> {
    match opt_expr.node() {
      ExprHandleNode(expr_handle) => Ok(*expr_handle),
      GroupHandleNode(group_handle) => Ok(*group_handle),
      OperatorNode(operator) => {
        let input_hep_node_ids: Vec<HepNodeId> = opt_expr
            .inputs()
            .iter()
            .map(|input_expr| self.insert_opt_node(&*input_expr))
            .collect::<OptResult<Vec<HepNodeId>>>()?;

        let hep_node = HepOptimizerNode {
          // Currently this id is fake.
          id: HepNodeId::default(),
          operator: operator.clone(),
          logical_prop: None,
          stat: None,
          physical_props: None,
        };

        let new_node_id = self.graph.graph.add_node(hep_node);
        // reset node id
        self.graph.graph[new_node_id].id = new_node_id;
        for input_hep_node_id in input_hep_node_ids {
          self.graph.graph.add_edge(new_node_id, input_hep_node_id, ());
        }

        // TODO: Derive logical prop, stats here
        let logical_prop = operator.derive_logical_prop(new_node_id, self)?;
        self.graph.graph[new_node_id].logical_prop = Some(logical_prop);
        Ok(new_node_id)
      }
    }
  }

  fn init_with_plan(&mut self, plan: Plan) -> OptResult<()> {
    let mut parents = HashMap::<PlanNodeId, Vec<HepNodeId>>::new();
    let mut node_id_map = HashMap::<PlanNodeId, HepNodeId>::new();

    for plan_node_ref in plan.bfs_iterator().collect::<Vec<PlanNodeRef>>().into_iter().rev() {
      parents
          .entry(plan_node_ref.id())
          .or_insert_with(Vec::new);
      for input in plan_node_ref.inputs() {
        parents.get_mut(&plan_node_ref.id()).unwrap()
            .push(node_id_map.get(&input.id()).cloned().unwrap());
      }
      let plan_node_id = self.insert_opt_node(&OptExpression::<HepOptimizer>::with_operator(plan_node_ref
                                                                                                .operator().clone(),
                                                                                            parents.get(&plan_node_ref.id())
                                                                                                .unwrap().iter()
                                                                                                .map(|id|
                                                                                                    OptExpression::with_expr_handle(id.clone(), vec![])),
      ))?;
      node_id_map.insert(plan_node_ref.id(), plan_node_id);
    }

    self.graph.root = *node_id_map.get(&(&*plan.root()).id()).unwrap();
    Ok(())
  }
}

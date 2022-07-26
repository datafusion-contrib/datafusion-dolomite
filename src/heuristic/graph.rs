use std::collections::HashMap;

use std::sync::Arc;

use petgraph::prelude::{NodeIndex, StableGraph};
use petgraph::visit::Bfs;
use petgraph::{Directed, Direction};

use crate::heuristic::{HepOptimizer, MatchOrder};
use crate::operator::{Operator};
use crate::optimizer::{OptExpr, OptExprHandle, OptGroup, OptGroupHandle};
use crate::plan::{Plan, PlanNode, PlanNodeBuilder, PlanNodeId, PlanNodeRef};
use crate::properties::{LogicalProperty, PhysicalPropertySet};
use crate::stat::Statistics;

type HepGraph = StableGraph<HepOptimizerNode, (), Directed, PlanNodeId>;
pub type HepNodeId = NodeIndex<PlanNodeId>;

pub struct HepOptimizerNode {
    pub(super) id: HepNodeId,
    pub(super) operator: Operator,
    pub(super) logical_prop: Option<LogicalProperty>,
    pub(super) stat: Option<Statistics>,
    pub(super) physical_props: Option<PhysicalPropertySet>,
}

/// A plan should be a single root dag.
#[derive(Default)]
pub(super) struct PlanGraph {
    pub(super) graph: HepGraph,
    pub(super) root: HepNodeId,
}

impl PlanGraph {
    pub(super) fn nodes_iter(
        &self,
        match_order: MatchOrder,
    ) -> Box<dyn Iterator<Item = HepNodeId>> {
        match match_order {
            MatchOrder::TopDown => Box::new(self.top_down_node_iters()),
            MatchOrder::BottomUp => Box::new(self.bottom_up_node_iters()),
        }
    }


    /// Return node ids in bottom up order.
    fn bottom_up_node_iters(&self) -> impl Iterator<Item = HepNodeId> {
        let mut ids = Vec::with_capacity(self.graph.node_count());
        let mut bfs = Bfs::new(&self.graph, self.root);

        // Create plan node for each `HepOptimizerNode`
        while let Some(node_id) = bfs.next(&self.graph) {
            ids.push(node_id);
        }

        ids.into_iter().rev()
    }

    /// Return node ids in bottom up order.
    fn top_down_node_iters(&self) -> impl Iterator<Item = HepNodeId> {
        let mut ids = Vec::with_capacity(self.graph.node_count());
        let mut bfs = Bfs::new(&self.graph, self.root);

        // Create plan node for each `HepOptimizerNode`
        while let Some(node_id) = bfs.next(&self.graph) {
            ids.push(node_id);
        }

        ids.into_iter()
    }

    pub(super) fn to_plan(&self) -> Plan {
        let next_plan_node_id = 1u32;
        let mut hep_node_id_to_plan_node = HashMap::<HepNodeId, PlanNodeRef>::new();
        // Traverse nodes in bottom up order, when visiting a node, its children all inserted
        // into map
        for node_id in self.bottom_up_node_iters() {
            let node = &self.graph[node_id];
            let inputs: Vec<PlanNodeRef> = self
                .graph
                .neighbors_directed(node_id, Direction::Outgoing)
                .map(|node_id| hep_node_id_to_plan_node.get(&node_id).unwrap().clone())
                .collect();

            let plan_node = PlanNodeBuilder::new(next_plan_node_id, &node.operator)
                .with_statistics(node.stat.clone())
                .with_logical_prop(node.logical_prop.clone())
                .with_physical_props(node.physical_props.clone())
                .add_inputs(inputs)
                .build();
            hep_node_id_to_plan_node.insert(node_id, Arc::new(plan_node));
        }

        hep_node_id_to_plan_node
            .get(&self.root)
            .map(|plan_node| Plan::new(plan_node.clone()))
            .unwrap()
    }
}

impl<'a> From<&'a PlanNode> for HepOptimizerNode {
    fn from(t: &'a PlanNode) -> Self {
        Self {
            id: HepNodeId::default(),
            operator: t.operator().clone(),
            logical_prop: t.logical_prop().cloned(),
            stat: t.stat().cloned(),
            physical_props: t.physical_props().cloned(),
        }
    }
}

impl OptGroup for HepOptimizerNode {
    fn logical_prop(&self) -> &LogicalProperty {
        self.logical_prop.as_ref().unwrap()
    }
}

impl OptExpr for HepOptimizerNode {
    type InputHandle = HepNodeId;
    type O = HepOptimizer;

    fn operator(&self) -> &Operator {
        &self.operator
    }

    fn inputs_len(&self, opt: &HepOptimizer) -> usize {
        opt.graph
            .graph
            .neighbors_directed(self.id, Direction::Outgoing)
            .count()
    }

    fn input_at(&self, idx: usize, opt: &HepOptimizer) -> HepNodeId {
        opt.graph
            .graph
            .neighbors_directed(self.id, Direction::Outgoing)
            .nth(idx)
            .unwrap()
    }
}

impl OptExprHandle for HepNodeId {
    type O = HepOptimizer;
}

impl OptGroupHandle for HepNodeId {
    type O = HepOptimizer;
}

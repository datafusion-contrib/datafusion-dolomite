use std::collections::HashSet;
use std::mem::swap;
use std::sync::Arc;


use crate::properties::{LogicalProperty, PhysicalPropertySet};
use crate::stat::Statistics;

mod logical;
pub use logical::*;
mod physical;
pub use physical::*;
use crate::operator::Operator;

pub type PlanNodeId = u32;

pub type PlanNodeRef = Arc<PlanNode>;

pub struct PlanNodeIdGen {
    next: PlanNodeId
}

impl PlanNodeIdGen {
    pub fn new() -> Self {
        Self {
            next: 0
        }
    }
    pub fn next(&mut self) -> PlanNodeId {
        self.next += 1;
        self.next
    }
}

/// One node in a plan.
///
/// This is used in both input and output of an optimizer. Given that we may have many different
/// phases in query optimization, we use one data structure to represent a plan.
#[derive(Debug)]
pub struct PlanNode {
    id: PlanNodeId,
    operator: Operator,
    inputs: Vec<PlanNodeRef>,
    logical_prop: Option<LogicalProperty>,
    stat: Option<Statistics>,
    physical_props: Option<PhysicalPropertySet>,
}

/// The `eq` should ignore `id`.
impl PartialEq for PlanNode {
    fn eq(&self, other: &Self) -> bool {
        self.operator == other.operator
            && self.inputs == other.inputs
            && self.logical_prop == other.logical_prop
            && self.stat == other.stat
            && self.physical_props == other.physical_props
    }
}

/// A query plan.
///
/// A query plan is a single root dag(directed acyclic graph). It can be used in many places, for
/// example, logical plan after validating an ast, a physical plan after completing optimizer.
#[derive(PartialEq, Debug)]
pub struct Plan {
    root: PlanNodeRef,
}

/// Breath first iterator of a single root dag plan.
struct BFSPlanNodeIter {
    visited: HashSet<PlanNodeId>,
    cur_level: Vec<PlanNodeRef>,
    next_level: Vec<PlanNodeRef>,
}

impl Iterator for BFSPlanNodeIter {
    type Item = PlanNodeRef;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_level.is_empty() {
            swap(&mut self.cur_level, &mut self.next_level);
        }

        if let Some(p) = self.cur_level.pop() {
            for input in &p.inputs {
                if !self.visited.contains(&input.id) {
                    self.next_level.push(input.clone());
                    self.visited.insert(input.id);
                }
            }

            Some(p)
        } else {
            None
        }
    }
}

impl Plan {
    pub fn new(root: PlanNodeRef) -> Self {
        Self { root }
    }

    pub fn root(&self) -> PlanNodeRef {
        self.root.clone()
    }

    pub fn bfs_iterator(&self) -> impl Iterator<Item = PlanNodeRef> {
        let mut visited = HashSet::new();
        visited.insert(self.root.id);

        BFSPlanNodeIter {
            cur_level: vec![self.root.clone()],
            next_level: vec![],
            visited,
        }
    }
}

impl PlanNode {
    pub fn new(id: PlanNodeId, operator: Operator, inputs: Vec<PlanNodeRef>) -> Self {
        Self {
            id,
            operator,
            inputs,
            logical_prop: None,
            stat: None,
            physical_props: None,
        }
    }

    pub fn operator(&self) -> &Operator {
        &self.operator
    }

    pub fn id(&self) -> PlanNodeId {
        self.id
    }

    pub fn inputs(&self) -> &[PlanNodeRef] {
        &self.inputs
    }

    pub fn logical_prop(&self) -> Option<&LogicalProperty> {
        self.logical_prop.as_ref()
    }

    pub fn stat(&self) -> Option<&Statistics> {
        self.stat.as_ref()
    }

    pub fn physical_props(&self) -> Option<&PhysicalPropertySet> {
        self.physical_props.as_ref()
    }
}

pub struct PlanNodeBuilder {
    plan_node: PlanNode,
}

impl PlanNodeBuilder {
    pub fn new(id: PlanNodeId, operator: &Operator) -> Self {
        Self {
            plan_node: PlanNode {
                id,
                operator: operator.clone(),
                inputs: vec![],
                logical_prop: None,
                stat: None,
                physical_props: None,
            },
        }
    }

    pub fn add_inputs<I>(mut self, inputs: I) -> Self
    where
        I: IntoIterator<Item = PlanNodeRef>,
    {
        self.plan_node.inputs.extend(inputs);
        self
    }

    pub fn with_logical_prop(mut self, logical_prop: Option<LogicalProperty>) -> Self {
        self.plan_node.logical_prop = logical_prop;
        self
    }

    pub fn with_statistics(mut self, stat: Option<Statistics>) -> Self {
        self.plan_node.stat = stat;
        self
    }

    pub fn with_physical_props(
        mut self,
        physical_props: Option<PhysicalPropertySet>,
    ) -> Self {
        self.plan_node.physical_props = physical_props;
        self
    }

    pub fn build(self) -> PlanNode {
        self.plan_node
    }
}

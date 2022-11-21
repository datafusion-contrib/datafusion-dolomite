use datafusion::catalog::schema::{MemorySchemaProvider, SchemaProvider};
use std::fmt::Debug;
use std::sync::Arc;

use crate::error::DolomiteResult;
use crate::operator::Operator;
use crate::plan::{Plan, PlanNodeId, PlanNodeIdGen};
use crate::properties::LogicalProperty;

/// Context for optimization. Includes access to catalog, session variables.
#[derive(Clone)]
pub struct OptimizerContext {
    pub catalog: Arc<dyn SchemaProvider>,
    plan_node_gen: PlanNodeIdGen,
}

impl Default for OptimizerContext {
    fn default() -> Self {
        Self {
            catalog: Arc::new(MemorySchemaProvider::default()),
            plan_node_gen: PlanNodeIdGen::default(),
        }
    }
}

impl OptimizerContext {
    pub fn next_plan_node_id(&mut self) -> PlanNodeId {
        self.plan_node_gen.gen_next()
    }
}

/// Optimizer interface.
///
/// All information required by optimizer, such as rule set, input plan, required property are
/// passed by optimizer in constructor, since different optimizer may require different information.
///
/// The concepts of `group` and `group expression` are borrowed from cascades optimizer. Each
/// `group` consists of several `group expressions`, and all group expressions represents
/// logically same plan, e.g. return same result set. In heuristic optimizer, they are same
/// thing, just a node in plan graph.
pub trait Optimizer {
    type GroupHandle: OptGroupHandle<O = Self>;
    type ExprHandle: OptExprHandle<O = Self>;
    type Group: OptGroup;
    type Expr: OptExpr<O = Self, InputHandle = Self::GroupHandle>;

    /// These methods are accessed by rules.
    fn context(&self) -> &OptimizerContext;
    fn group_at(&self, group_handle: Self::GroupHandle) -> &Self::Group;
    fn expr_at(&self, expr_handle: Self::ExprHandle) -> &Self::Expr;

    /// Entry point to drive optimization process.
    fn find_best_plan(self) -> DolomiteResult<Plan>;
}

pub trait OptExpr {
    type O: Optimizer;
    type InputHandle: OptGroupHandle;

    fn operator(&self) -> &Operator;
    fn inputs_len(&self, opt: &Self::O) -> usize;
    fn input_at(&self, idx: usize, opt: &Self::O) -> Self::InputHandle;
}

pub trait OptGroup {
    fn logical_prop(&self) -> &LogicalProperty;
}

pub trait OptExprHandle: Clone + Debug + PartialEq + Eq {
    type O: Optimizer<ExprHandle = Self>;
}

pub trait OptGroupHandle: Clone + Debug + PartialEq + Eq {
    type O: Optimizer<GroupHandle = Self>;
}

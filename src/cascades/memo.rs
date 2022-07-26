use anyhow::bail;
use std::cell::RefCell;
use std::cmp::{max, min};
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::hash::Hash;
use std::mem::swap;
use std::ops::{Index, IndexMut};
use std::rc::Rc;
use std::sync::Arc;

use enumset::EnumSet;
use itertools::Itertools;

use prettytable::{row, Table};

use crate::cascades::CascadesOptimizer;
use crate::cost::Cost;
use crate::error::OptResult;
use crate::operator::Operator;
use crate::optimizer::{OptExpr, OptExprHandle, OptGroup, OptGroupHandle};
use crate::plan::{Plan, PlanNode, PlanNodeId, PlanNodeRef};
use crate::properties::{LogicalProperty, PhysicalPropertySet};
use crate::rules::OptExprNode::{ExprHandleNode, GroupHandleNode, OperatorNode};
use crate::rules::{OptExprNode, OptExpression, Pattern, RuleId};
use crate::stat::Statistics;

/// Dynamic programming table used for storing expression groups.
pub(super) struct Memo {
    /// Used to avoid insert duplicate group expression.
    group_exprs: HashMap<GroupExprKey, GroupExprId>,
    groups: HashMap<GroupId, Group>,
    root_group_id: GroupId,
    next_group_id: GroupId,

    /// Records which group expression has been merged to.
    ///
    /// When we found group `a` and group `b` are equal, we merge group a into group b. And we
    /// need to move all group expression in `a` into `b`. In case we can't reference original
    /// group expression id in a, we maintain this map to refer to new group expression id.
    merged_group_expr: HashMap<GroupExprId, GroupExprId>,

    /// Records which group has been merged into.
    ///
    /// During execution of optimization tasks, we may find some groups are duplicated, and we
    /// will merge them in [`merge_duplicate_groups`] in some time. After a group has been merged
    /// into a new group, it's stored in this map.
    merged_groups: HashMap<GroupId, GroupId>,

    /// A temp buffer stores found duplicated but not merged groups.
    ///
    /// During optimization task execution, we may found some group are duplicated, but we don't
    /// merge them immediately, e.g. in [`ApplyRuleTask`] if can't merge them immediately since it
    /// may invalidate binding iterator. Instead we just mark them as duplicated and merge
    /// them after all bindings are exhausted. To avoid cascades merging, we always merge
    /// large group id into small group id.
    duplicated_groups: HashMap<GroupId, GroupId>,
}

impl Memo {
    pub(super) fn root_group_id(&self) -> GroupId {
        self.root_group_id
    }

    /// Find best plan from root group.
    pub(super) fn best_plan(&self, required_prop: &PhysicalPropertySet) -> OptResult<Plan> {
        let id_gen = {
            let id = Rc::new(RefCell::new(0u32));
            move || {
                *(*id).borrow_mut() += 1;
                *id.borrow()
            }
        };
        self.groups
            .get(&self.root_group_id)
            .unwrap()
            .best_plan_of(required_prop, &self, id_gen)
            .map(Plan::new)
    }

    /// Insert a rule result into memo and return group expression id.
    ///
    /// Note that some optimizer node maybe extracted from original group expression and remain
    /// unchanged, it just returns original group expression id.
    ///
    /// # Guarantee
    ///
    /// This method only creates new groups/group expression, but never remove group/group
    /// expressions. When this method finds duplicated groups during processing, but it only
    /// marks them in memo, and ***never merge groups***. This is because this method is called by
    /// `ApplyRuleTask`. The task may have found many bindings, and if we merge groups during
    /// insertion, the found bindings may be invalid.
    pub(super) fn insert_opt_expression(
        &mut self,
        opt_expr: &OptExpression<CascadesOptimizer>,
        target_group: Option<GroupId>,
    ) -> GroupExprId {
        match opt_expr.node() {
            // TODO: We should verify inputs in this case.
            ExprHandleNode(group_expr_id) => *group_expr_id,
            OperatorNode(operator) => {
                let input_groups = opt_expr
                    .inputs()
                    .iter()
                    .map(|input| match input.node() {
                        GroupHandleNode(group_id) => *group_id,
                        _ => self.insert_opt_expression(input, None).group_id,
                    })
                    .collect();

                let group_expr_key = GroupExprKey {
                    operator: operator.clone(),
                    inputs: input_groups,
                };

                self.insert_group_expression(group_expr_key, target_group)
            }
            GroupHandleNode(_) => {
                unreachable!("Should not insert group handle directly!")
            }
        }
    }

    pub(super) fn insert_group_expression(
        &mut self,
        group_expr_key: GroupExprKey,
        target_group: Option<GroupId>,
    ) -> GroupExprId {
        let existing_group_expr_id = self.group_exprs.get(&group_expr_key).map(|id| *id);
        let existing_group = existing_group_expr_id.map(|id| id.group_id);

        match (existing_group, target_group) {
            (Some(existing_group_id), Some(target_group_id)) => {
                if existing_group_id != target_group_id {
                    self.mark_duplicated_group(target_group_id, existing_group_id);
                }
                existing_group_expr_id.unwrap()
            }
            (Some(_), None) => existing_group_expr_id.unwrap(),
            (None, Some(target_group_id)) => {
                let new_group_expr_id =
                    self[target_group_id].insert_group_expr(GroupExpr::new(group_expr_key.clone()));
                self.group_exprs.insert(group_expr_key, new_group_expr_id);
                new_group_expr_id
            }
            (None, None) => {
                let group_expr = GroupExpr::new(group_expr_key.clone());
                let new_group_id = self.new_group();
                let new_group_expr_id = self[new_group_id].insert_group_expr(group_expr);
                self.group_exprs.insert(group_expr_key, new_group_expr_id);
                new_group_expr_id
            }
        }
    }

    /// Process `duplicate_groups` and merge them.
    pub(super) fn merge_duplicate_groups(&mut self) {
        let mut existing_duplicated_groups = HashMap::with_capacity(self.duplicated_groups.len());

        swap(&mut existing_duplicated_groups, &mut self.duplicated_groups);
        for (src, dest) in &existing_duplicated_groups {
            self.merge_group(*src, *dest);
        }

        // Example all group expressions to update group reference
        self.groups
            .values_mut()
            .for_each(|group| group.merge_group_mappings(&existing_duplicated_groups));

        // Update root group
        if let Some(target_group) = existing_duplicated_groups.get(&self.root_group_id) {
            self.root_group_id = *target_group;
        }

        // Update group expr mapping
        {
            let mut old_group_key_mapping = HashMap::with_capacity(self.group_exprs.len());
            swap(&mut old_group_key_mapping, &mut self.group_exprs);
            for (mut group_expr_key, group_expr_id) in old_group_key_mapping {
                group_expr_key.inputs = group_expr_key
                    .inputs
                    .iter()
                    .map(|group_id| *existing_duplicated_groups.get(group_id).unwrap_or(group_id))
                    .collect();
                let new_group_expr_id = self
                    .merged_group_expr
                    .get(&group_expr_id)
                    .unwrap_or(&group_expr_id);

                self.group_exprs.insert(group_expr_key, *new_group_expr_id);
            }
        }
    }

    fn merge_group(&mut self, src: GroupId, dest: GroupId) {
        let src_group = self.groups.remove(&src).unwrap();
        let dest_group = self.groups.get_mut(&dest).unwrap();

        // Merge logical group exprs
        for (group_expr_id, group_expr) in src_group.logical_group_exprs {
            let new_group_expr_id = dest_group.next_group_expr_id();
            dest_group
                .logical_group_exprs
                .insert(new_group_expr_id, group_expr);
            self.merged_group_expr
                .insert(group_expr_id, new_group_expr_id);
        }

        // Merge logical group exprs
        for (group_expr_id, group_expr) in src_group.physical_group_exprs {
            let new_group_expr_id = dest_group.next_group_expr_id();
            dest_group
                .physical_group_exprs
                .insert(new_group_expr_id, group_expr);
            self.merged_group_expr
                .insert(group_expr_id, new_group_expr_id);
        }

        // Merge best plan
        for mut src_best_plan in src_group.best_plans {
            // Update best plan's group expr id
            src_best_plan.1.group_expr_id = *self
                .merged_group_expr
                .get(&src_best_plan.1.group_expr_id)
                .unwrap();
            dest_group
                .best_plans
                .entry(src_best_plan.0)
                .and_modify(|r| {
                    if r.lowest_cost > src_best_plan.1.lowest_cost {
                        r.group_expr_id = src_best_plan.1.group_expr_id;
                    }
                })
                .or_insert(src_best_plan.1);
        }

        // Record in merged group
        self.merged_groups.insert(src, dest);
    }

    /// Mark `src_group_id` and `dest_group_id` are duplicated.
    fn mark_duplicated_group(&mut self, src_group_id: GroupId, dest_group_id: GroupId) {
        if src_group_id == dest_group_id {
            return;
        }

        // We always merge large group id into small group id.
        let (src, dest) = (
            min(src_group_id, dest_group_id),
            max(src_group_id, dest_group_id),
        );

        match (
            self.merged_groups.get(&src).cloned(),
            self.merged_groups.get(&dest).cloned(),
        ) {
            (Some(dest1), Some(dest2)) => {
                let last_dest1 = min(dest1, dest2);
                self.merged_groups.insert(src, last_dest1);
                self.merged_groups.insert(dest, last_dest1);
            }
            (Some(dest1), None) => {
                self.merged_groups.insert(src, min(dest1, dest));
            }
            (None, Some(dest2)) => {
                self.merged_groups.insert(src, dest2);
            }
            (None, None) => {
                self.merged_groups.insert(src, dest);
            }
        }
    }

    fn new_group(&mut self) -> GroupId {
        let new_group_id = self.next_group_id();
        let group = Group::new(new_group_id);
        self.groups.insert(group.group_id, group);
        new_group_id
    }

    fn next_group_id(&mut self) -> GroupId {
        let ret = self.next_group_id;
        self.next_group_id.0 += 1;
        ret
    }

    /// If current group not found, it will try to search merged group id.
    fn search_group(&self, group_id: GroupId) -> Option<&Group> {
        self.groups.get(&group_id).or_else(|| {
            self.merged_groups
                .get(&group_id)
                .and_then(|dup_group_id| self.groups.get(dup_group_id))
        })
    }

    fn get_group(&self, group_id: GroupId) -> Option<&Group> {
        self.groups.get(&group_id)
    }

    /// Similar to `get_group_expression`. If not found, will search merged group expression for it.
    fn search_group_expression(&self, group_expr_id: GroupExprId) -> Option<&GroupExpr> {
        self.get_group_expression(&group_expr_id).or_else(|| {
            self.merged_group_expr
                .get(&group_expr_id)
                .and_then(|merged_group_expr_id| self.get_group_expression(merged_group_expr_id))
        })
    }

    /// Get group expression indexed by `group_expr_id` without searching duplicated group
    /// expression.
    fn get_group_expression(&self, group_expr_id: &GroupExprId) -> Option<&GroupExpr> {
        self.groups.get(&group_expr_id.group_id).and_then(|group| {
            group
                .logical_group_exprs
                .get(group_expr_id)
                .or_else(|| group.physical_group_exprs.get(group_expr_id))
        })
    }
}

/// Converts `Plan` to `Memo`.
impl From<Plan> for Memo {
    fn from(plan: Plan) -> Self {
        let plan_nodes = plan.bfs_iterator().collect::<Vec<PlanNodeRef>>();
        let mut memo = Memo {
            group_exprs: HashMap::new(),
            groups: HashMap::new(),
            root_group_id: GroupId(0usize),
            next_group_id: GroupId(0usize),

            merged_group_expr: HashMap::new(),
            merged_groups: HashMap::new(),
            duplicated_groups: HashMap::new(),
        };

        let mut node_id_to_group_id = HashMap::with_capacity(plan_nodes.len());
        // Iterate plan from bottom up
        for node in plan_nodes.into_iter().rev() {
            let key = GroupExprKey {
                operator: node.operator().clone(),
                inputs: node
                    .inputs()
                    .iter()
                    .map(|node| *node_id_to_group_id.get(&node.id()).unwrap())
                    .collect(),
            };

            let group_id = memo.insert_group_expression(key, None).group_id;
            node_id_to_group_id.insert(node.id(), group_id);
        }

        // reset root group id
        memo.root_group_id = *node_id_to_group_id.get(&plan.root().id()).unwrap();

        memo
    }
}

impl Index<GroupId> for Memo {
    type Output = Group;

    fn index(&self, index: GroupId) -> &Group {
        self.groups.get(&index).unwrap()
    }
}

impl IndexMut<GroupId> for Memo {
    fn index_mut(&mut self, index: GroupId) -> &mut Self::Output {
        self.groups.get_mut(&index).unwrap()
    }
}

impl Index<GroupExprId> for Memo {
    type Output = GroupExpr;

    fn index(&self, index: GroupExprId) -> &Self::Output {
        &self[index.group_id][index]
    }
}

impl IndexMut<GroupExprId> for Memo {
    fn index_mut(&mut self, index: GroupExprId) -> &mut Self::Output {
        &mut self[index.group_id][index]
    }
}

impl Debug for Memo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "")?;
        writeln!(f, "Groups in memo:")?;
        writeln!(f, "")?;

        for group in self.groups.values() {
            writeln!(f, "{:?}", group)?;
        }

        // Print merged group expressions
        {
            writeln!(f, "Merged group expressions:")?;
            let mut table = Table::new();
            table.add_row(row![
                "Source Group Expression Id",
                "Target Group Expression Id"
            ]);
            for (src, dest) in &self.merged_group_expr {
                table.add_row(row![src, dest]);
            }

            writeln!(f, "{}", table)?;
        }

        // Print merged groups
        {
            writeln!(f, "Merged groups:")?;
            let mut table = Table::new();
            table.add_row(row!["Source Group Id", "Target Group Id"]);
            for (src, dest) in &self.merged_groups {
                table.add_row(row![src, dest]);
            }

            writeln!(f, "{}", table)?;
        }

        // Print found duplicated groups
        {
            writeln!(f, "Duplicated groups:")?;
            let mut table = Table::new();
            table.add_row(row!["Source Group Id", "Target Group Id"]);
            for (src, dest) in &self.duplicated_groups {
                table.add_row(row![src, dest]);
            }

            writeln!(f, "{}", table)?;
        }

        writeln!(f, "")
    }
}

/// A group id is an index of `groups` in `Memo`.
#[derive(Hash, Eq, PartialEq, Clone, Copy, Ord, PartialOrd)]
pub struct GroupId(pub usize);

impl Debug for GroupId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl Display for GroupId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl OptGroupHandle for GroupId {
    type O = CascadesOptimizer;
}

/// A group expression id is an index of `physical_group_exprs` or `logical_group_exprs` in `Group`.
#[derive(Hash, Eq, PartialEq, Clone, Copy)]
pub struct GroupExprId {
    pub(super) group_id: GroupId,
    pub(super) expr_id: usize,
}

impl Debug for GroupExprId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}.{:?}", self.group_id, self.expr_id)
    }
}

impl Display for GroupExprId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}.{:?}", self.group_id, self.expr_id)
    }
}

impl OptExprHandle for GroupExprId {
    type O = CascadesOptimizer;
}

impl GroupExprId {
    pub fn new(group_id: GroupId, expr_id: usize) -> Self {
        Self { group_id, expr_id }
    }
}

/// A group contains a set of logically equivalent `GroupExpression`s.
pub struct Group {
    group_id: GroupId,
    /// All expressions in a group should have same statistics.
    _stats: Option<Statistics>,
    _logical_prop: Option<LogicalProperty>,
    pub(super) logical_group_exprs: HashMap<GroupExprId, GroupExpr>,
    pub(super) physical_group_exprs: HashMap<GroupExprId, GroupExpr>,

    /// Lowest cost plans for each [`PhysicalPropertySet`].
    best_plans: HashMap<PhysicalPropertySet, OptimizationResult>,

    /// All logical expression has been explored.
    pub(super) explored: bool,

    next_expr_id: usize,
}

impl Debug for Group {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Group {:?}:", &self.group_id.0)?;

        let mut table = Table::new();
        table.add_row(row!["Group Expression Id", "Operator", "Inputs"]);
        for (group_expr_id, group_expr) in &self.logical_group_exprs {
            table.add_row(row![
                group_expr_id.expr_id,
                format!("{:?}", group_expr.key.operator),
                format!("{:?}", group_expr.key.inputs)
            ]);
        }

        for (group_expr_id, group_expr) in &self.physical_group_exprs {
            table.add_row(row![
                group_expr_id.expr_id,
                format!("{:?}", group_expr.key.operator),
                format!("{:?}", group_expr.key.inputs)
            ]);
        }

        writeln!(f, "{}", table)
    }
}

impl Index<GroupExprId> for Group {
    type Output = GroupExpr;

    fn index(&self, index: GroupExprId) -> &Self::Output {
        self.logical_group_exprs
            .get(&index)
            .or_else(|| self.physical_group_exprs.get(&index))
            .unwrap()
    }
}

impl IndexMut<GroupExprId> for Group {
    fn index_mut(&mut self, index: GroupExprId) -> &mut Self::Output {
        self.logical_group_exprs
            .get_mut(&index)
            .or_else(|| self.physical_group_exprs.get_mut(&index))
            .unwrap()
    }
}

impl OptGroup for Group {
    fn logical_prop(&self) -> &LogicalProperty {
        todo!()
    }
}

impl Group {
    fn new(group_id: GroupId) -> Self {
        Self {
            group_id,
            _stats: None,
            _logical_prop: None,
            logical_group_exprs: HashMap::new(),
            physical_group_exprs: HashMap::new(),
            best_plans: HashMap::new(),
            explored: false,
            next_expr_id: 0,
        }
    }

    pub(super) fn winner(
        &self,
        physical_prop_set: &PhysicalPropertySet,
    ) -> Option<&OptimizationResult> {
        self.best_plans.get(physical_prop_set)
    }

    pub(super) fn physical_group_expr_ids(&self) -> Vec<GroupExprId> {
        self.physical_group_exprs.keys().map(|k| *k).collect()
    }

    pub(super) fn logical_group_expr_ids(&self) -> Vec<GroupExprId> {
        if cfg!(test) {
            self.logical_group_exprs
                .keys()
                .map(|k| *k)
                .sorted_by_key(|g| g.expr_id + g.group_id.0)
                .collect()
        } else {
            self.logical_group_exprs.keys().map(|k| *k).collect()
        }
    }

    pub(super) fn update_winner(
        &mut self,
        group_expr_id: GroupExprId,
        output_prop: &PhysicalPropertySet,
        input_props: &[PhysicalPropertySet],
        cost: Cost,
    ) {
        if let Some(winner) = self.winner(output_prop) {
            if winner.lowest_cost < cost {
                return;
            }
        }

        // Insert new winner
        self.best_plans.insert(
            output_prop.clone(),
            OptimizationResult {
                lowest_cost: cost.clone(),
                group_expr_id,
            },
        );

        let group_expr = self.physical_group_exprs.get_mut(&group_expr_id).unwrap();
        group_expr.update_winner_input(output_prop, input_props, cost);
    }

    /// Number of group expressions.
    pub(super) fn expr_count(&self) -> usize {
        self.logical_group_exprs.len() + self.physical_group_exprs.len()
    }

    fn insert_group_expr(&mut self, group_expr: GroupExpr) -> GroupExprId {
        let group_expr_id = self.next_group_expr_id();

        match group_expr.key.operator {
            Operator::Logical(_) => {
                self.logical_group_exprs.insert(group_expr_id, group_expr);
            }
            Operator::Physical(_) => {
                self.physical_group_exprs.insert(group_expr_id, group_expr);
            }
        }

        group_expr_id
    }

    fn next_group_expr_id(&mut self) -> GroupExprId {
        let expr_id = self.next_expr_id;
        self.next_expr_id += 1;
        GroupExprId {
            group_id: self.group_id,
            expr_id,
        }
    }

    fn merge_group_mappings(&mut self, merge_group_mapping: &HashMap<GroupId, GroupId>) {
        self.logical_group_exprs
            .values_mut()
            .for_each(|group_expr| group_expr.update_group_ids(merge_group_mapping));
        self.physical_group_exprs
            .values_mut()
            .for_each(|group_expr| group_expr.update_group_ids(merge_group_mapping));
    }

    fn best_plan_of<G>(
        &self,
        prop: &PhysicalPropertySet,
        memo: &Memo,
        plan_id_gen: G,
    ) -> OptResult<PlanNodeRef>
    where
        G: Fn() -> PlanNodeId + Clone,
    {
        if let Some(winner) = self.winner(prop) {
            let best_group_expr = self
                .physical_group_exprs
                .get(&winner.group_expr_id)
                .unwrap();

            let winner_input = best_group_expr.output_prop_map.get(prop).unwrap();
            let plan_node_id = plan_id_gen();

            let input_plans = best_group_expr
                .key
                .inputs
                .iter()
                .zip(&winner_input.input_props)
                .map(|(group_id, input_prop)| {
                    memo[*group_id].best_plan_of(input_prop, memo, plan_id_gen.clone())
                })
                .try_collect()?;

            Ok(Arc::new(PlanNode::new(
                plan_node_id,
                best_group_expr.key.operator.clone(),
                input_plans,
            )))
        } else {
            bail!(
                "Plan with property {:?} not found in {:?}.",
                prop,
                self.group_id
            )
        }
    }
}

/// Base group expression information.
#[derive(Eq, PartialEq, Hash, Debug, Clone)]
pub(super) struct GroupExprKey {
    pub(super) operator: Operator,
    pub(super) inputs: Vec<GroupId>,
}

pub struct GroupExpr {
    /// Can be used to uniquely identify a group expression.
    ///
    /// It should not be changed after creation.
    key: GroupExprKey,

    /// Rules already applied to this group expression.
    applied_rules: EnumSet<RuleId>,

    /// Key is output property, while value is the winner sub plan's required properties and cost.
    output_prop_map: HashMap<PhysicalPropertySet, WinnerInput>,
}

impl OptExpr for GroupExpr {
    type InputHandle = GroupId;
    type O = CascadesOptimizer;

    fn operator(&self) -> &Operator {
        GroupExpr::operator(self)
    }

    fn inputs_len(&self, _opt: &CascadesOptimizer) -> usize {
        self.key.inputs.len()
    }

    fn input_at(&self, idx: usize, _opt: &CascadesOptimizer) -> GroupId {
        self.key.inputs[idx].clone()
    }
}

impl GroupExpr {
    pub(super) fn new(key: GroupExprKey) -> Self {
        Self {
            key,
            applied_rules: EnumSet::new(),
            output_prop_map: HashMap::new(),
        }
    }

    pub(super) fn is_rule_applied(&self, rule_id: RuleId) -> bool {
        self.applied_rules.contains(rule_id)
    }

    pub(super) fn input_group_ids(&self) -> impl Iterator<Item = GroupId> {
        self.key.inputs.clone().into_iter()
    }

    pub(super) fn set_rule_applied(&mut self, rule_id: RuleId) {
        self.applied_rules |= rule_id;
    }

    pub(super) fn matches_without_children(&self, pattern: &Pattern) -> bool {
        (pattern.predict)(self.operator())
            && (pattern
                .children
                .as_ref()
                .map(|c| c.len() == self.key.inputs.len())
                .unwrap_or(true))
    }

    fn derive_statistics(&self) -> Statistics {
        todo!()
    }

    fn derive_logical_property(&self) -> LogicalProperty {
        todo!()
    }

    pub fn operator(&self) -> &Operator {
        &self.key.operator
    }

    pub fn is_logical(&self) -> bool {
        matches!(self.operator(), Operator::Logical(_))
    }

    pub fn is_physical(&self) -> bool {
        matches!(self.operator(), Operator::Physical(_))
    }

    pub(super) fn inputs(&self) -> &[GroupId] {
        &self.key.inputs
    }

    fn update_winner_input(
        &mut self,
        output_prop: &PhysicalPropertySet,
        input_props: &[PhysicalPropertySet],
        cost: Cost,
    ) {
        if let Some(winner) = self.output_prop_map.get(output_prop) {
            if winner.lowest_cost < cost {
                return;
            }
        }

        self.output_prop_map.insert(
            output_prop.clone(),
            WinnerInput {
                input_props: input_props.to_vec(),
                lowest_cost: cost,
            },
        );
    }

    /// Update input group ids using merged group mapping.
    fn update_group_ids(&mut self, merge_group_mapping: &HashMap<GroupId, GroupId>) {
        self.key.inputs = self
            .key
            .inputs
            .iter()
            .map(|group_id| *merge_group_mapping.get(group_id).unwrap_or(group_id))
            .collect();
    }
}

/// The result of finding the lowest cost physical grouup expression for [`PhysicalPropertySet`].
#[derive(Debug)]
pub(super) struct OptimizationResult {
    pub(super) lowest_cost: Cost,
    /// Id of lowest cost physical group.
    pub(super) group_expr_id: GroupExprId,
}

pub(super) struct WinnerInput {
    pub(super) lowest_cost: Cost,
    /// Required properties of inputs.
    pub(super) input_props: Vec<PhysicalPropertySet>,
}

#[cfg(test)]
mod tests {

    use datafusion::logical_expr::col;
    use datafusion::logical_plan::binary_expr;
    use datafusion::prelude::JoinType;

    use crate::cascades::memo::Memo;
    use crate::operator::LogicalOperator::{
        LogicalJoin, LogicalLimit, LogicalProjection, LogicalScan,
    };
    use crate::operator::Operator::Logical;
    use crate::operator::{
        Join, Limit as LimitOp, Projection as ProjectionOp, TableScan as TableScanOp, TableScan,
    };
    use crate::plan::LogicalPlanBuilder;
    use datafusion::logical_plan::Operator::Eq;

    #[test]
    fn test_build_memo_from_plan() {
        let original_plan = LogicalPlanBuilder::new()
            .scan(None, "t1".to_string())
            .limit(5)
            .projection(vec![col("c1")])
            .build();

        let memo = Memo::from(original_plan);
        assert_eq!(3, memo.groups.len());

        let mut group_id = memo.root_group_id;
        // Test projection
        {
            let root_group = &memo[group_id];
            assert_eq!(1, root_group.logical_group_expr_ids().len());
            assert_eq!(0, root_group.physical_group_expr_ids().len());
            let root_group_expr = &memo[root_group.logical_group_expr_ids()[0]];
            assert_eq!(
                root_group_expr.key.operator,
                Logical(LogicalProjection(ProjectionOp::new(vec![col("c1")])))
            );

            group_id = root_group_expr.inputs()[0];
        }

        // Test limit
        {
            let group = &memo[group_id];
            assert_eq!(1, group.logical_group_expr_ids().len());
            assert_eq!(0, group.physical_group_expr_ids().len());
            let group_expr_id = group.logical_group_expr_ids()[0];
            let group_expr = &memo[group_expr_id];
            assert_eq!(
                group_expr.key.operator,
                Logical(LogicalLimit(LimitOp::new(5)))
            );

            // set next group id
            group_id = group_expr.inputs()[0];
        }

        // Table scan
        {
            let group = &memo[group_id];
            assert_eq!(1, group.logical_group_expr_ids().len());
            assert_eq!(0, group.physical_group_expr_ids().len());
            let group_expr_id = group.logical_group_expr_ids()[0];
            let group_expr = &memo[group_expr_id];
            assert_eq!(
                group_expr.key.operator,
                Logical(LogicalScan(TableScanOp::new("t1".to_string())))
            );
        }
    }

    #[test]
    fn build_plan_with_multi_child() {
        let plan = {
            let mut builder = LogicalPlanBuilder::new();
            let right = builder.scan(None, "t2").build().root();

            builder
                .scan(None, "t1")
                .join(
                    JoinType::Inner,
                    binary_expr(col("t1.c1"), Eq, col("t2.c2")),
                    right,
                )
                .build()
        };

        let memo = Memo::from(plan);

        println!("{:?}", memo);

        assert_eq!(3, memo.groups.len());

        let mut children_group_ids = Vec::new();
        // Test join
        {
            let root_group = &memo[memo.root_group_id];
            assert_eq!(1, root_group.logical_group_expr_ids().len());
            assert_eq!(0, root_group.physical_group_expr_ids().len());
            let root_group_expr = &memo[root_group.logical_group_expr_ids()[0]];
            assert_eq!(
                root_group_expr.key.operator,
                Logical(LogicalJoin(Join::new(
                    JoinType::Inner,
                    binary_expr(col("t1.c1"), Eq, col("t2.c2"))
                )))
            );

            assert_eq!(2, root_group_expr.key.inputs.len());
            children_group_ids = root_group_expr.key.inputs.to_vec();
        }

        // Test left table scan
        {
            let group = &memo[children_group_ids[0]];
            assert_eq!(1, group.logical_group_expr_ids().len());
            assert_eq!(0, group.physical_group_expr_ids().len());
            let group_expr = &memo[group.logical_group_expr_ids()[0]];
            assert_eq!(
                group_expr.key.operator,
                Logical(LogicalScan(TableScan::new("t1")))
            );

            assert_eq!(0, group_expr.key.inputs.len());
        }

        // Test right table scan
        {
            let group = &memo[children_group_ids[1]];
            assert_eq!(1, group.logical_group_expr_ids().len());
            assert_eq!(0, group.physical_group_expr_ids().len());
            let group_expr = &memo[group.logical_group_expr_ids()[0]];
            assert_eq!(
                group_expr.key.operator,
                Logical(LogicalScan(TableScan::new("t2")))
            );

            assert_eq!(0, group_expr.key.inputs.len());
        }
    }
}

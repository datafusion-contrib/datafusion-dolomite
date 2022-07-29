# datafusion-dolomite

An experimental query optimization framework written for datafusion. For detail design notes, please refer to 
[src/lib.rs](src/lib.rs).

# Example

To define a logical plan:

```
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
```

Use cascades optimizer to find best plan:
```
let optimizer = CascadesOptimizer::new(
    PhysicalPropertySet::default(),
    vec![
        CommutateJoinRule::new().into(),
        Join2HashJoinRule::new().into(),
        Scan2TableScanRule::new().into(),
    ],
    plan,
    OptimizerContext {},
);
optimizer.find_best_plan().unwrap()
```

# Current status

- [x] Heuristic optimizer framework.
- [x] Cascades style optimizer framework.
- [ ] Cost model.
- [ ] Statistics model.
- [ ] Optimization rules. Implemented some simple rules to verify optimizer framework.

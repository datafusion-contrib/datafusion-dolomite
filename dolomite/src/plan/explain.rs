use crate::plan::{Plan, PlanNode};
use ptree::print_config::UTF_CHARS;
use ptree::{write_tree_with, PrintConfig, Style, TreeItem};
use std::borrow::Cow;
use std::default::Default;
use std::io::{BufWriter, Write};

impl<'a> TreeItem for &'a PlanNode {
    type Child = Self;

    fn write_self<W: Write>(&self, f: &mut W, style: &Style) -> std::io::Result<()> {
        write!(f, "{}", style.paint(&self.operator))
    }

    fn children(&self) -> Cow<[Self::Child]> {
        Cow::from(
            self.inputs
                .iter()
                .map(|c| &**c)
                .collect::<Vec<&'a PlanNode>>(),
        )
    }
}

pub fn explain<W: Write>(plan: &Plan, output: &mut W) -> std::io::Result<()> {
    let config = PrintConfig {
        indent: 3,
        characters: UTF_CHARS.into(),
        ..Default::default()
    };
    write_tree_with(&&*plan.root, output, &config)
}

pub fn explain_to_string(plan: &Plan) -> std::io::Result<String> {
    let mut buf = BufWriter::new(Vec::new());

    // Do writing here.
    explain(plan, &mut buf)?;

    let bytes = buf.into_inner()?;
    Ok(String::from_utf8(bytes).unwrap())
}

#[cfg(test)]
mod tests {
    use crate::plan::explain::explain_to_string;
    use crate::plan::{LogicalPlanBuilder, PhysicalPlanBuilder};
    use datafusion::logical_expr::{binary_expr, col};
    use datafusion::prelude::JoinType;
    use datafusion_expr::Operator::Eq;

    #[test]
    fn test_explain_logical_plan() {
        let plan = LogicalPlanBuilder::new()
            .scan(None, "t1".to_string())
            .limit(5)
            .projection(vec![col("c1")])
            .limit(10)
            .build();

        let expected_result = "\
LogicalLimit { limit: 10 }
└─ LogicalProjection { expr: [c1] }
   └─ LogicalLimit { limit: 5 }
      └─ LogicalScan { table_name: \"t1\" }
";

        let result = explain_to_string(&plan).unwrap();

        assert_eq!(expected_result, result);
    }

    #[test]
    fn test_explain_physical_plan() {
        let plan = {
            let right = PhysicalPlanBuilder::scan(None, "t2").build().root();

            PhysicalPlanBuilder::scan(None, "t1")
                .hash_join(
                    JoinType::Inner,
                    binary_expr(col("t1.c1"), Eq, col("t2.c2")),
                    right,
                )
                .build()
        };

        let expected_result = "\
PhysicalHashJoin { join_type: Inner, expr: t1.c1 = t2.c2 }
├─ PhysicalTableScan { table_name: \"t1\" }
└─ PhysicalTableScan { table_name: \"t2\" }
";
        let result = explain_to_string(&plan).unwrap();
        assert_eq!(expected_result, result);
    }
}

use std::collections::HashMap;

use datafusion::prelude::Column;

/// Statistics of operator.
#[derive(Clone, PartialEq, Debug)]
pub struct Statistics {
    /// Total number of row count.
    ///
    /// This maybe an estimated value.
    row_count: f64,
    /// Statistics of each column.
    column_stats: HashMap<Column, ColumnStatistics>,
}

/// Statistics of one column.
#[derive(Clone, PartialEq, Debug)]
pub struct ColumnStatistics {
    /// Number of distinct value of a column.
    ndv: f64,
}

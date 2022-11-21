//! Decorrelate subqueries.
//!
//! It's heavily inspired by [DuckDB's implementation](https://github.com/duckdb/duckdb/blob/master/src/planner/subquery/flatten_dependent_join.cpp).
//!
//! For more details, please refer to `Unnesting Arbitrary Queries`.
//!
mod flatten_dependent_joins;
mod planner;

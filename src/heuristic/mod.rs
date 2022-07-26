//! Implementation of heuristic optimizer.
//!
//! Heuristic optimizer optimizes query plan by applying a batch of rewrite rules to query plan
//! until some condition is met, e.g. max number of iterations or reached fixed point. The
//! implementation is heavily inspired by [apache calcite](https://github.com/apache/calcite)'s
//! HepPlanner.
//! Heuristic optimization is useful in several cases. For example, it can be used to preprocess
//! logical plan before sending to cost base optimizer. Also, in oltp or time series database which
//! serves highly concurrent point queries(plan is relative simple, and query result is small),
//! we may only use heuristic optimizer to reduce optimization time.

mod optimizer;
pub use optimizer::*;
mod graph;
pub use graph::*;
mod binding;

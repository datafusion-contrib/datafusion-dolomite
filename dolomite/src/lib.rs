//! ## Background
//!
//! The query optimizer accepts an unoptimized logical query plan, and outputs an optimized physical
//! plan ready to be executed. In general query optimization process comes in two flavors: rule
//! based and cost based.
//!
//! Rule based optimization is relative simple. We apply a collection of optimization rules to a
//! query plan repeatedly until some condition is met, for example, a fix point (plan no longer
//! changes) or number of times. The optimization rule is substitution rule, e.g. the optimizer
//! substitutes rule generated new plan for original plan, and in general the new plan should be
//! better than original plan. Rule based optimization is quite useful in applying some heuristic
//! optimization rules, for example, remove unnecessary field reference, column pruning, etc.
//!
//! Cost based optimization tries to find plan with lowest cost by searching plan space. [2]
//! proposed a top-down searching strategy to enumerate possible plans, and used dynamic
//! programming to reduce duplicated computation. This is also the cost based optimization
//! framework implemented in this crate. There are also other searching strategies, for example,
//! [1] used a bottom-up searching strategy to enumerate possible plans and also used dynamic
//! programming to reduce search time.
//!
//!
//! ## Design
//!
//! * [`heuristic`] Heuristic optimizer implementation.
//! * [`cascades`] Cascades style cost based optimizer.
//! * [`operator`] Relational operators.
//! * [`properties`] Logical and physical properties.
//! * [`rules`] Optimization rule definition and implementation.
//!
//! ## Reference
//!
//! 1. Selinger, P. Griffiths, et al. "Access path selection in a relational database management
//! system." Readings in Artificial Intelligence and Databases. Morgan Kaufmann, 1989. 511-522.
//! 2. Graefe, G., 1995. The cascades framework for query optimization. IEEE Data Eng. Bull., 18(3),
//! pp.19-29.
//! 3. Soliman, M.A., Antova, L., Raghavan, V., El-Helw, A., Gu, Z., Shen, E., Caragea, G.C.,  
//! Garcia-Alvarado, C., Rahman, F., Petropoulos, M. and Waas, F., 2014, June.  Orca: a modular
//! query optimizer architecture for big data. In Proceedings of the 2014 ACM SIGMOD
//! international  conference on Management of data (pp. 337-348).
//! 4. Columnbia Project, https://github.com/yongwen/columbia

#[macro_use]
extern crate prettytable;
#[macro_use]
extern crate lazy_static;
extern crate core;

use datafusion::prelude::Expr;

pub mod cascades;
pub mod cost;
pub mod error;
pub mod heuristic;
pub mod operator;
pub mod optimizer;
pub mod plan;
pub mod properties;
pub mod rules;
pub mod stat;

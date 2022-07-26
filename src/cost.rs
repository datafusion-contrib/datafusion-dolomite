//! Defines cost model.



use derive_more::{Add, AddAssign, Sub, SubAssign, Sum};

pub const INF: Cost = Cost(f64::INFINITY);

#[derive(Copy, Clone, Debug, PartialOrd, PartialEq, Add, Sub, Sum, AddAssign, SubAssign)]
pub struct Cost(f64);

impl From<f64> for Cost {
    fn from(c: f64) -> Self {
        Cost(c)
    }
}

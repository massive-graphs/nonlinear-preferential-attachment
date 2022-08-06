#![feature(cell_update)]
#![feature(core_intrinsics)]

pub mod algorithm;
pub mod edge_writer;
pub mod parameters;
pub mod weight_function;

pub type Node = usize;
pub type Edge = (Node, Node);

pub mod prelude {
    use super::*;

    pub use super::{Edge, Node};
    pub use edge_writer::EdgeWriter;
    pub use weight_function::WeightFunction;
}

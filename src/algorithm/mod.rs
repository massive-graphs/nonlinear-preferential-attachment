use rand::Rng;
use crate::parameters::Parameters;
use super::prelude::*;
use crate::weight_function::WeightFunction;

pub mod algo_dynamic_weighted_index;
pub mod algo_poly_pa;

pub trait Algorithm : Sized {
    fn new(num_seed_nodes: Node, num_rand_nodes: Node, initial_degree: Node, without_replacement: bool, weight_function: WeightFunction) -> Self;
    fn set_seed_graph(&mut self, edges : impl Iterator<Item=Edge>);
    fn run(&mut self, rng : &mut impl Rng, writer: &mut impl EdgeWriter);

    fn from_parameters(opt: &Parameters) -> Self {
        let weight_function = WeightFunction::new(opt.exponent, opt.offset);
        assert!(weight_function.get(1) > 0.0);
        Self::new(opt.seed_nodes.unwrap(), opt.nodes, opt.initial_degree, opt.without_replacement, weight_function)
    }
}


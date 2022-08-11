use super::prelude::*;
use crate::parameters::Parameters;
use crate::weight_function::WeightFunction;
use rand::Rng;

pub mod algo_dynamic_weighted_index;
pub mod algo_parallel_poly_pa;
pub mod algo_poly_pa;
pub mod algo_poly_pa_prefetch;

pub trait Algorithm<R: Rng>: Sized {
    const IS_PARALLEL: bool;

    fn new(
        rng: R,
        num_threads: usize,
        num_seed_nodes: Node,
        num_rand_nodes: Node,
        initial_degree: Node,
        without_replacement: bool,
        resample: bool,
        weight_function: WeightFunction,
    ) -> Self;

    fn set_seed_graph_degrees(&mut self, degrees: impl Iterator<Item = Node>);
    fn run(&mut self, writer: &mut impl EdgeWriter);

    fn from_parameters(rng: R, opt: &Parameters) -> Self {
        let weight_function = WeightFunction::new(opt.exponent, opt.offset);
        assert!(weight_function.get(1) > 0.0);
        Self::new(
            rng,
            opt.num_threads.unwrap_or_else(|| {
                if Self::IS_PARALLEL {
                    num_cpus::get()
                } else {
                    1
                }
            }),
            opt.seed_nodes.unwrap(),
            opt.nodes,
            opt.initial_degree,
            opt.without_replacement,
            opt.resample_previous,
            weight_function,
        )
    }

    fn degrees(&self) -> Vec<Node>;

    fn number_of_edges(&self) -> usize {
        self.degrees().iter().sum::<usize>() / 2
    }
}

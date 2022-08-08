use super::prelude::*;
use crate::parameters::Parameters;
use crate::weight_function::WeightFunction;
use rand::Rng;

pub mod algo_dynamic_weighted_index;
pub mod algo_poly_pa;
pub mod algo_poly_pa_prefetch;

pub trait Algorithm<R: Rng>: Sized {
    fn new(
        rng: R,
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
            opt.seed_nodes.unwrap(),
            opt.nodes,
            opt.initial_degree,
            opt.without_replacement,
            opt.resample_previous,
            weight_function,
        )
    }
}

/// Under the assumption that we produce k distinct hosts, this function can be used to sample
/// which of the previous hosts, become also a host in the next round. This works well for
/// distributions with a few very high degree nodes (alpha >> 1).
/// For each input item, we output a single output item where `(u, True)` corresponds to a node
/// that is kept while `(u, False)` is discarded
fn reselect_previous<'a>(
    rng: &'a mut impl Rng,
    previous_nodes: impl Iterator<Item = (Node, f64)> + 'a,
    num_hosts: usize,
    mut total_weight: f64,
) -> impl Iterator<Item = (Node, bool)> + 'a {
    let mut num_hosts = num_hosts as i32;

    previous_nodes.map(move |(node, weight)| {
        let prob_reject = (1.0 - weight / total_weight).powf(num_hosts as f64);
        let do_accept = !rng.gen_bool(prob_reject);

        //total_weight -= weight;
        num_hosts -= do_accept as i32;

        (node, do_accept)
    })
}

#![allow(clippy::too_many_arguments)]

mod proposal_list;
mod run_length;
mod shared_state;
mod worker;

use super::*;
use proposal_list::ProposalList;
use shared_state::{NodeInfo, State};
use worker::Worker;

use crate::algorithm::algo_parallel_poly_pa::run_length::RunlengthSampler;
use atomic_float::AtomicF64;
use crossbeam::atomic::AtomicCell;
use itertools::Itertools;
use rand::SeedableRng;
use rand_distr::Distribution;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Barrier};
use std::thread;

const SCALE: f64 = 2.0 * (1u64 << 63) as f64;

pub struct AlgoParallelPolyPa<R: Rng + Send + Sync> {
    rng: R,
    num_threads: usize,
    state: Arc<State>,
}

impl<R: Rng + Send + Sync + SeedableRng + 'static> Algorithm<R> for AlgoParallelPolyPa<R> {
    const IS_PARALLEL: bool = true;

    fn new(
        rng: R,
        num_threads: usize,
        num_seed_nodes: Node,
        num_rand_nodes: Node,
        initial_degree: Node,
        without_replacement: bool,
        resample: bool,
        weight_function: WeightFunction,
    ) -> Self {
        assert!(!resample);
        let num_total_nodes = num_seed_nodes + num_rand_nodes;

        println!("NodeInfo: {}b", std::mem::size_of::<NodeInfo>());

        let runlength_sampler = RunlengthSampler::new(weight_function.clone(), initial_degree);

        Self {
            rng,
            num_threads,
            state: Arc::new(State {
                num_seed_nodes,
                num_total_nodes,
                initial_degree,
                without_replacement,
                weight_function,

                total_weight: AtomicF64::new(0.0),
                nodes: (0..num_total_nodes)
                    .into_iter()
                    .map(|_| NodeInfo::default())
                    .collect(),
                proposal_list: Arc::new(ProposalList::new(
                    7 * num_total_nodes / 3 + 10000,
                    num_threads,
                )),
                runlength_sampler,

                wmax: AtomicF64::new(0.0),
                max_degree: AtomicCell::new(0),
            }),
        }
    }

    fn set_seed_graph_degrees(&mut self, degrees: impl Iterator<Item = Node>) {
        let mut num_input_degrees = 0;

        for (node, degree) in degrees.enumerate() {
            self.state.sequential_set_degree(node, degree);
            num_input_degrees += 1;
        }

        assert_eq!(num_input_degrees, self.state.num_seed_nodes);

        for u in 0..self.state.num_seed_nodes {
            self.state.sequential_update_node_counts_in_proposal_list(u);
        }

        self.state.runlength_sampler.setup_epoch(
            self.state.num_seed_nodes,
            self.state.num_total_nodes,
            self.state.max_degree.load(),
            self.state.total_weight.load(Ordering::Acquire),
        );
    }

    fn run(&mut self, _writer: &mut impl EdgeWriter) {
        let num_threads = self.num_threads; // needed for capture down below
        let barrier = Arc::new(Barrier::new(num_threads));

        let handles = (0..self.num_threads)
            .into_iter()
            .map(|rank| {
                let barrier = barrier.clone();
                let rng = R::seed_from_u64(self.rng.gen()); // TODO: improve seeding
                let state = self.state.clone();

                thread::spawn(move || {
                    Worker::new(rng, state, barrier, rank, num_threads).run();
                })
            })
            .collect_vec();

        for handle in handles {
            handle.join().unwrap();
        }
    }

    fn degrees(&self) -> Vec<Node> {
        self.state.nodes.iter().map(|i| i.degree.load()).collect()
    }
}

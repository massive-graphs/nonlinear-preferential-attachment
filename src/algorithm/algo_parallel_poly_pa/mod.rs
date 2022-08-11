mod proposal_list;
mod shared_state;
mod worker;

use super::*;
use proposal_list::ProposalList;
use shared_state::{NodeInfo, State};
use worker::Worker;

use atomic_float::AtomicF64;
use crossbeam::atomic::AtomicCell;
use rand::SeedableRng;
use rand_distr::Distribution;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Barrier, Mutex};
use std::thread;

const SCALE: f64 = 2.0 * (1u64 << 63) as f64;
const NUM_THREADS: usize = 2;

pub struct AlgoParallelPolyPa<R: Rng + Send + Sync> {
    rng: R,
    state: Arc<State>,
}

impl<R: Rng + Send + Sync + SeedableRng + 'static> Algorithm<R> for AlgoParallelPolyPa<R> {
    fn new(
        rng: R,
        num_seed_nodes: Node,
        num_rand_nodes: Node,
        initial_degree: Node,
        without_replacement: bool,
        resample: bool,
        weight_function: WeightFunction,
    ) -> Self {
        assert!(!resample);
        let num_total_nodes = num_seed_nodes + num_rand_nodes;

        Self {
            rng,
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
                proposal_list: Arc::new(ProposalList::new(7 * num_total_nodes / 3, NUM_THREADS)),

                wmax: AtomicF64::new(0.0),
                max_degree: AtomicCell::new(0),

                epoch_starts_with_node: AtomicCell::new(num_seed_nodes),
                epoch_ends_with_node: AtomicCell::new(num_total_nodes),
            }),
        }
    }

    fn set_seed_graph_degrees(&mut self, degrees: impl Iterator<Item = Node>) {
        let mut num_input_degrees = 0;
        let mut max_degree = 0;

        for (node, degree) in degrees.enumerate() {
            self.state.sequential_set_degree(node, degree);
            max_degree = max_degree.max(degree);
            num_input_degrees += 1;
        }

        self.state.max_degree.fetch_max(max_degree);

        assert_eq!(num_input_degrees, self.state.num_seed_nodes);

        for u in 0..self.state.num_seed_nodes {
            self.state.sequential_update_node_counts_in_proposal_list(u);
        }
    }

    fn run(&mut self, _writer: &mut impl EdgeWriter) {
        let num_threads = NUM_THREADS;

        let mut handles = Vec::with_capacity(num_threads);
        let barrier = Arc::new(Barrier::new(NUM_THREADS));
        let mutex = Arc::new(Mutex::new(()));

        for rank in 0..num_threads {
            let local_barrier = barrier.clone();

            let local_rng = R::seed_from_u64(self.rng.gen()); // TODO: improve seeding
            let local_state = self.state.clone();
            let local_mutex = mutex.clone();

            handles.push(thread::spawn(move || {
                Worker::new(
                    local_rng,
                    local_state,
                    local_barrier,
                    local_mutex,
                    rank,
                    num_threads,
                )
                .run();
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }

    fn degrees(&self) -> Vec<Node> {
        self.state.nodes.iter().map(|i| i.degree.load()).collect()
    }
}

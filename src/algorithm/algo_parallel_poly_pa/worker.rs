use super::{proposal_list::Producer, *};
use crate::algorithm::algo_parallel_poly_pa::proposal_list::Sampler;
use crate::weight_function::Regime;
use itertools::Itertools;
use rand_distr::Geometric;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub struct Worker<R: Rng + Send + Sync> {
    rank: usize,
    num_threads: usize,

    rng: R,
    algo: Arc<State>,
    producer: Producer,
    sampler: Sampler,

    barrier: Arc<Barrier>,
    mutex: Arc<Mutex<()>>,

    hosts_produced_in_epoch: Vec<Node>,

    epoch_starts_with_node: usize,
    epoch_ends_with_node: usize,

    epoch_begin_total_weight: f64,
    total_weight: f64,
}

impl<R: Rng + Send + Sync> Worker<R> {
    pub(super) fn new(
        rng: R,
        algo: Arc<State>,
        barrier: Arc<Barrier>,
        mutex: Arc<Mutex<()>>,
        rank: usize,
        num_threads: usize,
    ) -> Self {
        let producer = Producer::new(algo.proposal_list.clone());
        let sampler = Sampler::new(algo.proposal_list.clone());

        let epoch_starts_with_node = algo.epoch_starts_with_node.load();
        let epoch_ends_with_node = algo.epoch_ends_with_node.load();
        Self {
            rank,
            num_threads,

            rng,
            algo,
            producer,
            sampler,

            barrier,
            mutex,

            hosts_produced_in_epoch: Vec::with_capacity(10000),

            epoch_starts_with_node,
            epoch_ends_with_node,
            epoch_begin_total_weight: 0.0,
            total_weight: 0.0,
        }
    }

    pub fn run(&mut self) {
        let mut num_epochs = 0;

        loop {
            num_epochs += 1;

            let ended_phase1_with = self.phase1_sample_independent_hosts();
            self.sampler.update_end();

            self.epoch_begin_total_weight = self.algo.total_weight.load(Ordering::Acquire);
            self.total_weight = self.epoch_begin_total_weight;

            // barrier
            let leader = self.barrier.wait().is_leader();
            let is_limiting_worker = ended_phase1_with == self.epoch_ends_with_node;

            self.epoch_ends_with_node = self.algo.epoch_ends_with_node.load();

            if leader {
                println!(
                    "Epoch {:>10} to {:>10}; len: {:>10}",
                    self.epoch_starts_with_node,
                    self.epoch_ends_with_node,
                    self.epoch_ends_with_node - self.epoch_starts_with_node
                );
            }

            {
                let mutex = self.mutex.clone();
                let _lock = mutex.lock().unwrap();
                self.phase2_update_proposal_list();

                self.algo.total_weight.fetch_add(
                    self.total_weight - self.epoch_begin_total_weight,
                    Ordering::AcqRel,
                );

                self.producer.free_unfinished_range();
            }

            self.barrier.wait();

            if self.epoch_ends_with_node >= self.algo.num_total_nodes {
                break;
            }

            if is_limiting_worker {
                self.algo.proposal_list.compact_unfinished_ranges();
                self.phase3_sample_collision();
            }

            // TODO: Implement _is_limiting

            self.epoch_starts_with_node = self.epoch_ends_with_node + 1;
            self.epoch_ends_with_node = self.algo.num_total_nodes;

            self.algo
                .epoch_ends_with_node
                .store(self.algo.num_total_nodes);

            self.barrier.wait(); // TODO: we can probably avoid this barrier
        }

        if self.rank == 1 {
            println!("Num epochs: {}", num_epochs);
        }
    }

    fn phase1_sample_independent_hosts(&mut self) -> Node {
        let mut hosts = Vec::with_capacity(self.algo.initial_degree);

        let total_weight: f64 = self.algo.total_weight.load(Ordering::Acquire);
        let max_degree = self.algo.max_degree.load();
        let weight_dmax = self.algo.weight_function.get(max_degree);
        let weight_dmin = self.algo.weight_function.get(self.algo.initial_degree);

        let mut local_stop_at_node = self.algo.num_total_nodes;

        let mut new_node = self.epoch_starts_with_node + self.rank;
        'sampling: while new_node < local_stop_at_node {
            debug_assert!(hosts.is_empty());
            let required_samples = self.sample_hosts(&mut hosts);

            // sample whether we need to insert a dependency into hosts
            {
                let nodes_in_epoch = new_node - self.epoch_starts_with_node;
                let hosts_in_epoch = nodes_in_epoch * self.algo.initial_degree;

                let upper_bound_on_total_weight = match self.algo.weight_function.regime() {
                    Regime::Sublinear => {
                        weight_dmin * nodes_in_epoch as f64 + hosts_in_epoch as f64
                        // TODO: hosts_in_epoc is crude; may use min-degree
                    }
                    Regime::Superlinear => {
                        let ub_dmax = max_degree + nodes_in_epoch;
                        let weight_ub_dmax = self.algo.weight_function.get(ub_dmax);

                        weight_dmin * nodes_in_epoch as f64
                            + (weight_ub_dmax - weight_dmax) * self.algo.initial_degree as f64
                    }
                    Regime::Linear => 2.0 * hosts_in_epoch as f64,
                } + total_weight;

                let prob_is_independent = total_weight / upper_bound_on_total_weight;

                if prob_is_independent < 1.0 {
                    let run_length = Geometric::new(1.0 - prob_is_independent)
                        .unwrap()
                        .sample(&mut self.rng);

                    if run_length < required_samples {
                        self.algo.epoch_ends_with_node.fetch_min(new_node);
                        break 'sampling;
                    }
                }
            }

            self.hosts_produced_in_epoch.append(&mut hosts);

            // TODO: May check stop_at less frequently
            {
                let stop_at = self.algo.epoch_ends_with_node.load();
                if stop_at < local_stop_at_node {
                    local_stop_at_node = stop_at;
                }
            }

            new_node += self.num_threads;
        }

        new_node
    }

    fn sample_hosts(&mut self, hosts: &mut Vec<Node>) -> u64 {
        let mut attempts = 0;
        let wmax_scaled = SCALE / self.algo.wmax.load(Ordering::Acquire);

        while hosts.len() < self.algo.initial_degree {
            hosts.push(loop {
                attempts += 1;
                let proposal = self.sampler.sample(&mut self.rng);

                if hosts.contains(&proposal) {
                    continue;
                }

                if self.do_accept_host(proposal, wmax_scaled) {
                    break proposal;
                }
            });
        }

        attempts
    }

    fn do_accept_host(&mut self, proposal: Node, wmax_scaled: f64) -> bool {
        let info = &self.algo.nodes[proposal];

        let weight = info.weight.load(Ordering::Acquire);
        let excess = weight / info.count.load() as f64;

        self.rng.gen::<u64>() < (excess * wmax_scaled) as u64
    }

    fn phase2_update_proposal_list(&mut self) {
        let num_nodes_contributed = (self
            .epoch_ends_with_node
            .saturating_sub(self.epoch_starts_with_node + self.rank))
            / self.num_threads;

        self.hosts_produced_in_epoch
            .truncate(num_nodes_contributed * self.algo.initial_degree);

        // we will use the hash map's arbitrary order to avoid congestion at high degree nodes
        let mut counts = self.hosts_produced_in_epoch.iter().copied().counts();
        counts.reserve(num_nodes_contributed);

        for u in ((self.epoch_starts_with_node + self.rank)..self.epoch_ends_with_node)
            .step_by(self.num_threads)
        {
            counts.insert(u, self.algo.initial_degree);
        }

        let assumed_nodes = (self.epoch_starts_with_node + num_nodes_contributed) as f64;

        for (node, degree_increase) in counts {
            let info = &self.algo.nodes[node];

            let old_degree = info.degree.fetch_add(degree_increase);
            let new_degree = old_degree + degree_increase;

            let new_weight = self.algo.weight_function.get(new_degree);
            let old_weight = info.weight.fetch_max(new_weight, Ordering::AcqRel);

            self.total_weight += new_weight - old_weight;

            let count = (assumed_nodes * new_weight / self.total_weight).ceil() as usize;

            if let Ok(old_count) =
                info.count
                    .fetch_update(|old| if old >= count { None } else { Some(count) })
            {
                self.producer.push(node, count - old_count);
            }
        }

        self.hosts_produced_in_epoch.clear();
    }

    fn phase3_sample_collision(&mut self) {
        let mut hosts = Vec::with_capacity(self.algo.initial_degree);

        // TODO: missing increased sampling odds for single host

        self.sample_hosts(&mut hosts);

        self.algo
            .sequential_set_degree(self.epoch_ends_with_node, self.algo.initial_degree);
        self.algo
            .sequential_update_node_counts_in_proposal_list(self.epoch_ends_with_node);

        for h in hosts {
            self.algo.sequential_increase_degree(h);
            self.algo.sequential_update_node_counts_in_proposal_list(h);
        }
    }
}

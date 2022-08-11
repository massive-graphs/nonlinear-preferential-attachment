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

    hosts_produced_in_epoch: Vec<Node>,

    epoch_starts_with_node: usize,
    epoch_ends_with_node: usize,

    epoch_begin_total_weight: f64,
    total_weight: f64,
    max_degree: Node,

    weight_min_degree: f64,
    weight_max_degree: f64,

    executes_phase3: bool,
}

impl<R: Rng + Send + Sync> Worker<R> {
    pub(super) fn new(
        rng: R,
        algo: Arc<State>,
        barrier: Arc<Barrier>,
        rank: usize,
        num_threads: usize,
    ) -> Self {
        let producer = Producer::new(algo.proposal_list.clone());
        let sampler = Sampler::new(algo.proposal_list.clone());

        let epoch_starts_with_node = 0;
        let epoch_ends_with_node = algo.num_seed_nodes;
        Self {
            rank,
            num_threads,

            rng,
            algo,
            producer,
            sampler,

            barrier,

            hosts_produced_in_epoch: Vec::with_capacity(10000),

            epoch_starts_with_node,
            epoch_ends_with_node,

            epoch_begin_total_weight: f64::NAN,
            total_weight: f64::NAN,
            max_degree: 0,

            weight_min_degree: f64::NAN,
            weight_max_degree: f64::NAN,

            executes_phase3: false,
        }
    }

    pub fn run(&mut self) {
        let mut num_epochs = 0;

        loop {
            num_epochs += 1;

            let phase1_ended_with;
            {
                self.setup_local_state_for_new_epoch();
                phase1_ended_with = self.phase1_sample_independent_hosts();
                self.sampler.update_end();
            }

            self.barrier.wait();

            {
                self.epoch_ends_with_node = self.algo.epoch_ends_with_node.load();
                self.executes_phase3 = phase1_ended_with == self.epoch_ends_with_node;

                self.report_progress();

                self.phase2_update_proposal_list();

                self.algo.total_weight.fetch_add(
                    self.total_weight - self.epoch_begin_total_weight,
                    Ordering::AcqRel,
                );

                self.producer.free_unfinished_range();

                if self.epoch_ends_with_node >= self.algo.num_total_nodes {
                    break;
                }
            }

            self.barrier.wait();

            {
                self.phase3_compaction_and_sampling();
            }

            self.barrier.wait();
        }

        if self.rank == 1 {
            println!("Num epochs: {}", num_epochs);
        }
    }

    fn setup_local_state_for_new_epoch(&mut self) {
        self.epoch_starts_with_node = self.epoch_ends_with_node;
        self.epoch_ends_with_node = self.algo.num_total_nodes;

        self.epoch_begin_total_weight = self.algo.total_weight.load(Ordering::Acquire);
        self.total_weight = self.epoch_begin_total_weight;
        self.max_degree = self.algo.max_degree.load();

        self.weight_max_degree = self.algo.weight_function.get(self.max_degree);
        self.weight_min_degree = self.algo.weight_function.get(self.algo.initial_degree);
    }

    fn phase1_sample_independent_hosts(&mut self) -> Node {
        let mut hosts = Vec::with_capacity(self.algo.initial_degree);

        let mut local_stop_at_node = self.algo.num_total_nodes;

        let start_node = self.epoch_starts_with_node + self.rank;
        let mut new_node = start_node;

        while new_node < local_stop_at_node {
            let _required_samples = self.sample_hosts(&mut hosts);

            // sample whether we need to insert a dependency into hosts
            if !self.was_sampling_run_independent(new_node, self.algo.initial_degree) {
                self.algo.epoch_ends_with_node.fetch_min(new_node + 1);
                break;
            }

            self.hosts_produced_in_epoch.append(&mut hosts);

            if (new_node - start_node) % 8 == 0 {
                local_stop_at_node = local_stop_at_node.min(self.algo.epoch_ends_with_node.load());
            }

            new_node += self.num_threads;
        }

        new_node + 1
    }

    fn was_sampling_run_independent(&mut self, node: usize, sampling_attempts: usize) -> bool {
        let prob_is_independent = self.probability_is_independent(node);

        let run_length = Geometric::new(1.0 - prob_is_independent)
            .unwrap()
            .sample(&mut self.rng);

        run_length > sampling_attempts as u64
    }

    fn probability_is_independent(&mut self, node: usize) -> f64 {
        let nodes_in_epoch = node - self.epoch_starts_with_node;
        let hosts_in_epoch = nodes_in_epoch * self.algo.initial_degree;

        let upper_bound_on_total_weight = match self.algo.weight_function.regime() {
            Regime::Sublinear => {
                self.weight_min_degree * nodes_in_epoch as f64 + hosts_in_epoch as f64
                // TODO: hosts_in_epoc is crude; may use min-degree
            }
            Regime::Superlinear => {
                let ub_dmax = self.max_degree + nodes_in_epoch;
                let weight_ub_dmax = self.algo.weight_function.get(ub_dmax);

                self.weight_min_degree * nodes_in_epoch as f64
                    + (weight_ub_dmax - self.weight_max_degree) * self.algo.initial_degree as f64
            }
            Regime::Linear => 2.0 * hosts_in_epoch as f64,
        } + self.total_weight;

        self.total_weight / upper_bound_on_total_weight
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
        let initial_degree = self.algo.initial_degree;

        let num_nodes_contributed = self.number_of_independent_nodes_contributed();
        let hosts_connected_to = num_nodes_contributed * initial_degree;

        debug_assert!(self.hosts_produced_in_epoch.len() >= hosts_connected_to);

        let host_degree_increases = self
            .hosts_produced_in_epoch
            .iter()
            .take(hosts_connected_to)
            .copied()
            .counts();

        let first_node = self.epoch_starts_with_node + self.rank;
        let own_degree_increases = (first_node..self.epoch_ends_with_node)
            .step_by(self.num_threads)
            .into_iter()
            .map(|u| (u, initial_degree));

        let assumed_nodes = (self.epoch_starts_with_node + num_nodes_contributed) as f64;

        own_degree_increases
            .chain(host_degree_increases.into_iter())
            .for_each(|(node, deg_inc)| self.increase_degree_of_node(node, deg_inc, assumed_nodes));

        self.hosts_produced_in_epoch.clear();
        self.algo.max_degree.fetch_max(self.max_degree);
    }

    fn number_of_independent_nodes_contributed(&mut self) -> usize {
        let nodes_in_epoch = self
            .epoch_ends_with_node
            .saturating_sub(self.epoch_starts_with_node);

        let first = (self.rank < nodes_in_epoch) as usize;
        let following = nodes_in_epoch.saturating_sub(self.rank + 1) / self.num_threads;

        (first + following).saturating_sub(self.executes_phase3 as usize)
    }

    fn increase_degree_of_node(
        &mut self,
        node: usize,
        degree_increase: Node,
        assumed_num_nodes: f64,
    ) {
        let info = &self.algo.nodes[node];

        let old_degree = info.degree.fetch_add(degree_increase);
        let new_degree = old_degree + degree_increase;
        self.max_degree = self.max_degree.max(new_degree);

        let new_weight = self.algo.weight_function.get(new_degree);
        let old_weight = info.weight.fetch_max(new_weight, Ordering::AcqRel);

        self.total_weight += new_weight - old_weight;

        let count = (assumed_num_nodes * new_weight / self.total_weight).ceil() as usize;

        if let Ok(old_count) =
            info.count
                .fetch_update(|old| if old >= count { None } else { Some(count) })
        {
            self.producer.push(node, count - old_count);
        }
    }

    fn phase3_compaction_and_sampling(&mut self) {
        if !self.executes_phase3 {
            return;
        }

        self.algo.proposal_list.compact_unfinished_ranges();
        self.phase3_sample_collision();

        debug_assert_eq!(
            self.compute_degree_sum(),
            self.algo.num_seed_nodes
                + 2 * (self.epoch_ends_with_node - self.algo.num_seed_nodes)
                    * self.algo.initial_degree
        );

        self.algo
            .epoch_ends_with_node
            .store(self.algo.num_total_nodes);
    }

    fn phase3_sample_collision(&mut self) {
        let mut hosts = Vec::with_capacity(self.algo.initial_degree);

        // TODO: missing increased sampling odds for single host

        self.sample_hosts(&mut hosts);

        let last_node = self.epoch_ends_with_node - 1;

        self.algo
            .sequential_set_degree(last_node, self.algo.initial_degree);
        self.algo
            .sequential_update_node_counts_in_proposal_list(last_node);

        for h in hosts {
            self.algo.sequential_increase_degree(h);
            self.algo.sequential_update_node_counts_in_proposal_list(h);
        }
    }

    fn report_progress(&mut self) {
        if false && self.is_leader_thread() {
            println!(
                "Epoch {:>10} to {:>10}; len: {:>10}",
                self.epoch_starts_with_node,
                self.epoch_ends_with_node,
                self.epoch_ends_with_node - self.epoch_starts_with_node
            );
        }
    }

    #[inline]
    fn is_leader_thread(&self) -> bool {
        self.rank == 0
    }

    fn compute_degree_sum(&self) -> usize {
        self.algo
            .nodes
            .iter()
            .map(|i| i.degree.load())
            .sum::<usize>()
    }
}

use super::{proposal_list::Writer, *};
use crate::algorithm::algo_parallel_poly_pa::proposal_list::Sampler;
use itertools::Itertools;
use std::intrinsics::{likely, unlikely};
use std::ops::Range;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use hurdles::Barrier;

pub struct Worker<R: Rng + Send + Sync> {
    rank: usize,
    num_threads: usize,

    rng: R,
    algo: Arc<State>,
    proposal_writer: Writer,
    proposal_sampler: Sampler,

    barrier: Barrier,

    hosts_linked_in_epoch: Vec<Node>,
    new_nodes: Vec<Node>,

    epoch_nodes: Range<usize>,

    total_weight_at_epoch_begin: f64,
    total_weight: f64,
    max_degree: Node,

    instant_last_report: Instant,
    instant_start: Instant,

    epoch_id: usize,
}

impl<R: Rng + Send + Sync> Worker<R> {
    pub(super) fn new(
        rng: R,
        algo: Arc<State>,
        barrier: Barrier,
        rank: usize,
        num_threads: usize,
    ) -> Self {
        let proposal_writer = Writer::new(algo.proposal_list.clone());
        let proposal_sampler = Sampler::new(algo.proposal_list.clone());

        let epoch_nodes = 0..algo.num_seed_nodes;

        let now = Instant::now();

        Self {
            rank,
            num_threads,

            rng,
            algo,
            proposal_writer,
            proposal_sampler,

            barrier,

            hosts_linked_in_epoch: Vec::with_capacity(10000),
            new_nodes: Vec::with_capacity(10000),

            epoch_nodes,

            total_weight_at_epoch_begin: f64::NAN,
            total_weight: f64::NAN,
            max_degree: 0,

            epoch_id: 0,
            instant_start: now,
            instant_last_report: now,
        }
    }

    pub fn run(&mut self) {
        loop {
            self.setup_local_state_for_new_epoch();

            assert!(self.hosts_linked_in_epoch.is_empty());

            if self.is_leader_thread() && self.epoch_id > 1 {
                self.new_nodes.push(self.epoch_nodes.start - 1);
                let mut hosts = std::mem::take(&mut self.hosts_linked_in_epoch);
                self.sample_hosts(&mut hosts, self.epoch_nodes.start, self.algo.initial_degree);
                self.hosts_linked_in_epoch = hosts;
            }

            {
                self.phase1_sample_independent_hosts();
            }

            self.barrier.wait();

            {
                self.epoch_nodes.end = self.algo.runlength_sampler.result();

                self.phase2_update_proposal_list();

                self.algo.total_weight.fetch_add(
                    self.total_weight - self.total_weight_at_epoch_begin,
                    Ordering::AcqRel,
                );

                self.proposal_writer.free_unfinished_range();
            }

            self.barrier.wait();

            if false {
                if self.is_leader_thread() {
                    self.phase3_compaction_and_sampling();
                }

                if self.rank + 1 == self.num_threads {
                    self.report_progress_sometimes();
                }
            }

            self.algo.runlength_sampler.setup_epoch(
                self.epoch_nodes.end,
                self.algo.num_total_nodes,
                self.algo.max_degree.load(),
                self.algo.total_weight.load(Ordering::Acquire),
            );

            if self.epoch_nodes.end >= self.algo.num_total_nodes {
                break;
            }

            //self.barrier.wait();

            {
                self.proposal_sampler.update_end();
            }
        }

        if self.is_leader_thread() {
            self.report_progress_forced();
        }
    }

    fn setup_local_state_for_new_epoch(&mut self) {
        self.epoch_nodes = self.epoch_nodes.end..self.algo.runlength_sampler.result();
        self.epoch_id += 1;

        self.total_weight_at_epoch_begin = self.algo.total_weight.load(Ordering::Acquire);
        self.total_weight = self.total_weight_at_epoch_begin;
        self.max_degree = self.algo.max_degree.load();
    }

    fn phase1_sample_independent_hosts(&mut self) {
        let start_node = self.epoch_nodes.start + self.rank;
        let mut hosts = std::mem::take(&mut self.hosts_linked_in_epoch);

        for node in (start_node..self.epoch_nodes.end - 1).step_by(self.num_threads) {
            if !self.algo.runlength_sampler.continue_with_node(
                &mut self.rng,
                node,
                self.algo.initial_degree,
            ) {
                break;
            }

            self.sample_hosts(&mut hosts, self.epoch_nodes.start, self.algo.initial_degree);
            self.new_nodes.push(node);
        }

        self.hosts_linked_in_epoch = hosts;
    }

    fn sample_hosts(&mut self, hosts: &mut Vec<Node>, new_node: Node, number: Node) -> u64 {
        let mut attempts = 0;
        let wmax_scaled = SCALE / self.algo.wmax.load(Ordering::Acquire);

        let begin = hosts.len();

        for _ in 0..number {
            hosts.push(loop {
                attempts += 1;
                let proposal = self.proposal_sampler.sample(&mut self.rng, new_node);

                unsafe {
                    std::intrinsics::prefetch_read_data(self.algo.nodes.as_ptr().add(proposal), 1);
                }

                if hosts.as_slice().split_at(begin).1.contains(&proposal) {
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
        let info = unsafe { self.algo.nodes.get_unchecked(proposal) };

        let weight = info.weight.load(Ordering::Acquire);
        let excess = weight / info.count.load() as f64;

        self.rng.gen::<u64>() < (excess * wmax_scaled) as u64
    }

    fn phase2_update_proposal_list(&mut self) {
        let initial_degree = self.algo.initial_degree;

        // discard nodes beyond epoch's end
        {
            let num_keep_nodes = self
                .new_nodes
                .iter()
                .filter(|&&u| u + 1 < self.epoch_nodes.end)
                .count();

            self.hosts_linked_in_epoch
                .truncate(num_keep_nodes * self.algo.initial_degree);

            self.new_nodes.truncate(num_keep_nodes);
        }

        let host_degree_increases = self.hosts_linked_in_epoch.iter().copied().counts();

        let new_nodes = std::mem::take(&mut self.new_nodes);
        let own_degree_increases = new_nodes.iter().map(|&u| (u, initial_degree));

        own_degree_increases
            .chain(host_degree_increases.into_iter())
            .for_each(|(node, deg_inc)| {
                self.increase_degree_of_node(node, deg_inc, self.epoch_nodes.end as f64)
            });

        self.new_nodes = new_nodes;

        self.hosts_linked_in_epoch.clear();
        self.new_nodes.clear();
        self.algo.max_degree.fetch_max(self.max_degree);
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

        if unlikely(old_weight >= new_weight) {
            // this can happen if another thread race us to this point; but not an issue,
            // since the other thread will take care of the "fallout"
            return;
        }

        self.total_weight += new_weight - old_weight;

        let count = (assumed_num_nodes * new_weight / self.total_weight).ceil() as usize;

        if let Ok(old_count) =
            info.count
                .fetch_update(|old| if old >= count { None } else { Some(count) })
        {
            self.proposal_writer.push(node, count - old_count);
        }
    }

    fn phase3_compaction_and_sampling(&mut self) {
        self.algo.proposal_list.compact_unfinished_ranges();
        self.phase3_sample_collision();

        debug_assert_eq!(
            self.compute_degree_sum(),
            self.algo.num_seed_nodes
                + 2 * (self.epoch_nodes.end - self.algo.num_seed_nodes) * self.algo.initial_degree
        );
    }

    fn phase3_sample_collision(&mut self) {
        let mut hosts = Vec::with_capacity(self.algo.initial_degree);

        // TODO: missing increased sampling odds for single host
        let last_node = self.epoch_nodes.end - 1;

        self.sample_hosts(&mut hosts, self.epoch_nodes.start, self.algo.initial_degree);

        self.algo
            .sequential_set_degree(last_node, self.algo.initial_degree);
        self.algo
            .sequential_update_node_counts_in_proposal_list(last_node);

        for h in hosts {
            self.algo.sequential_increase_degree(h);
            self.algo.sequential_update_node_counts_in_proposal_list(h);
        }
    }

    fn report_progress_sometimes(&mut self) {
        let now = Instant::now();
        let duration = now.duration_since(self.instant_last_report);

        if likely(duration.as_secs_f64() < 0.2) {
            return;
        }

        self.report_progress_now(now);
    }

    fn report_progress_forced(&mut self) {
        let now = Instant::now();
        self.report_progress_now(now);
    }

    fn report_progress_now(&mut self, now: Instant) {
        let elasped_ms = now.duration_since(self.instant_start).as_millis();
        self.instant_last_report = now;

        println!(
            "{:>7}ms Epoch {:>6} from {:>9} to {:>9} ({:>5.1} %); len: {:>5}",
            elasped_ms,
            self.epoch_id,
            self.epoch_nodes.start,
            self.epoch_nodes.end,
            100.0 * self.epoch_nodes.end as f64 / self.algo.num_total_nodes as f64,
            self.epoch_nodes.len()
        );
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

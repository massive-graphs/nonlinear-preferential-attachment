use super::*;
use rand_distr::Distribution;
use ringbuffer::{
    ConstGenericRingBuffer, RingBuffer, RingBufferExt, RingBufferRead, RingBufferWrite,
};
use std::cell::Cell;
use std::intrinsics::prefetch_read_data;

const SCALE: f64 = 2.0 * (1u64 << 63) as f64;

#[derive(Clone, Copy, Debug)]
struct NodeInfo {
    degree: Node,
    count: Node,
    weight: f64,
    excess: f64,
}

impl Default for NodeInfo {
    fn default() -> Self {
        Self {
            degree: 0,
            count: 1,
            weight: 0.0,
            excess: 0.0,
        }
    }
}

pub struct AlgoPolyPaPrefetch<R: Rng> {
    proposal_list: ProposalList<R>,
    num_total_nodes: Node,
    num_seed_nodes: Node,

    num_current_nodes: f64,

    initial_degree: Node,
    without_replacement: bool,
    resample: bool,
    weight_function: WeightFunction,

    nodes: Vec<NodeInfo>,
    total_weight: f64,
    wmax: f64,
    wmax_scaled: f64,

    num_samples: Cell<usize>,
    num_samples_to_reject: Cell<usize>,
}

impl<R: Rng> Algorithm<R> for AlgoPolyPaPrefetch<R> {
    fn new(
        rng: R,
        num_seed_nodes: Node,
        num_rand_nodes: Node,
        initial_degree: Node,
        without_replacement: bool,
        resample: bool,
        weight_function: WeightFunction,
    ) -> Self {
        let num_total_nodes = num_seed_nodes + num_rand_nodes;
        Self {
            num_seed_nodes,
            num_total_nodes,
            initial_degree,
            without_replacement,
            resample,
            weight_function,

            total_weight: 0.0,
            nodes: vec![Default::default(); num_total_nodes],
            proposal_list: ProposalList::new(rng, 11 * num_total_nodes / 10),

            num_current_nodes: 0.0,
            wmax: 0.0,
            wmax_scaled: 0.0,

            num_samples: Cell::new(0),
            num_samples_to_reject: Cell::new(0),
        }
    }

    fn set_seed_graph_degrees(&mut self, degrees: impl Iterator<Item = Node>) {
        let mut num_input_degrees = 0;

        for (degree, target) in degrees.zip(self.nodes.iter_mut()) {
            target.degree = degree;
            target.weight = self.weight_function.get(degree);
            self.total_weight += target.weight;

            num_input_degrees += 1;
        }

        assert_eq!(num_input_degrees, self.num_seed_nodes);
        self.num_current_nodes = self.num_seed_nodes as f64;

        for u in 0..self.num_seed_nodes {
            self.update_node_counts_in_proposal_list(u);
        }
        self.proposal_list.set_num_nodes(self.num_seed_nodes);
    }

    fn run(&mut self, writer: &mut impl EdgeWriter) {
        let mut hosts = vec![0; self.initial_degree];
        self.proposal_list.prefetch();

        for new_node in self.num_seed_nodes..self.num_total_nodes {
            if self.without_replacement {
                for i in 0..hosts.len() {
                    hosts[i] = self.sample_host(|u| hosts[0..i].contains(&u));
                }
            } else {
                for h in &mut hosts {
                    *h = self.sample_host(|_| false);
                }
            }

            self.num_current_nodes = (new_node + 1) as f64;

            // update neighbors
            for &h in &hosts {
                self.increase_degree(h);
                writer.add_edge(new_node, h);
            }

            self.set_degree(new_node, self.initial_degree);
            self.proposal_list.set_num_nodes(new_node + 1);
        }

        println!(
            "Proposals per node: {}",
            self.proposal_list.len() as f64 / self.num_total_nodes as f64
        );

        println!(
            "Samples per host:   {}",
            self.num_samples.get() as f64
                / ((self.num_total_nodes - self.num_seed_nodes) * self.initial_degree) as f64
        );

        println!(
            "Samples per host tr: {}",
            self.num_samples_to_reject.get() as f64
                / ((self.num_total_nodes - self.num_seed_nodes) * self.initial_degree) as f64
        );

        println!("Wmax: {}", self.wmax);

        println!(
            "Wmax-real: {:?}",
            self.nodes
                .iter()
                .map(|i| (i.degree, i.weight / i.count as f64))
                .fold((0, 0.0), |s, x| -> (Node, f64) {
                    if s.1 > x.1 {
                        s
                    } else {
                        x
                    }
                })
        );
    }
}

impl<R: Rng> AlgoPolyPaPrefetch<R> {
    fn sample_host(&mut self, reject_early: impl Fn(Node) -> bool) -> Node {
        loop {
            self.num_samples.update(|x| x + 1);
            let proposal = self.proposal_list.sample();

            //let proposal = *self.proposal_list.as_slice().choose(rng).unwrap() as usize;
            //let proposal= *prefetcher.sample(rng);

            //unsafe {
            //    prefetch_read_data(self.nodes.as_ptr().add(proposal), 2);
            //}
            if reject_early(proposal) {
                continue;
            }

            self.num_samples_to_reject.update(|x| x + 1);

            let info = self.nodes[proposal];

            let accept =
                self.proposal_list.rng().gen::<u64>() < (info.excess * self.wmax_scaled) as u64;
            //let accept = rng.gen_bool(info.excess / self.wmax);

            if accept {
                break proposal;
            }
        }
    }

    fn set_degree(&mut self, node: Node, degree: Node) {
        let info = &mut self.nodes[node];
        info.degree = degree;

        let weight_before = info.weight;
        info.weight = self.weight_function.get(degree);
        self.total_weight += info.weight - weight_before;

        self.update_node_counts_in_proposal_list(node);
    }

    fn update_node_counts_in_proposal_list(&mut self, node: Node) {
        let info = &mut self.nodes[node];
        let target_count =
            (self.num_current_nodes * info.weight / self.total_weight).ceil() as usize;

        self.proposal_list
            .push(node, target_count.saturating_sub(info.count));
        info.count = target_count;

        info.excess = info.weight / (info.count as f64);
        if self.wmax < info.excess {
            self.wmax = info.excess;
            self.wmax_scaled = SCALE / info.excess;
        }
    }

    fn increase_degree(&mut self, node: Node) {
        self.set_degree(node, self.nodes[node as usize].degree + 1);
    }
}

const PREFETCH_LEN: usize = 8;

struct ProposalList<R: Rng> {
    proposal_list: Vec<usize>,
    index_buffer: ConstGenericRingBuffer<usize, PREFETCH_LEN>,
    rng: R,
    num_nodes: usize,
    prefetched_size: usize,
}

impl<R: Rng> ProposalList<R> {
    pub fn new(rng: R, capacity: usize) -> Self {
        Self {
            rng,
            num_nodes: 0,
            proposal_list: Vec::with_capacity(capacity),
            index_buffer: Default::default(),
            prefetched_size: 0,
        }
    }

    pub fn push(&mut self, value: usize, count: usize) {
        for _ in 0..count {
            self.proposal_list.push(value);
        }
    }

    pub fn len(&self) -> usize {
        self.proposal_list.len()
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.proposal_list.is_empty()
    }

    pub fn set_num_nodes(&mut self, num: usize) {
        self.num_nodes = num;
        self.resample_after_growth();
    }

    pub fn rng(&mut self) -> &mut R {
        &mut self.rng
    }

    fn prefetch(&mut self) {
        assert!(self.index_buffer.capacity() > 0);

        let elements = self.num_nodes + self.proposal_list.len();
        self.prefetched_size = elements;

        while self.index_buffer.len() < PREFETCH_LEN {
            let index: usize = self.rng.gen_range(0..elements);

            unsafe {
                // the saturating_sub is a quick hack to avoid a conditional branch (sat_sub is
                // implemented via conditional move. Prefetching an illegal address is okay, and
                // we will always prefetch the same address. Thus the cache pollution is minimal)
                prefetch_read_data(
                    self.proposal_list
                        .as_ptr()
                        .add(index.saturating_sub(self.num_nodes)),
                    1,
                );
            };

            self.index_buffer.push(index);
        }
    }

    fn resample_after_growth(&mut self) {
        if self.prefetched_size == 0 {
            return;
        }

        let elements = self.num_nodes + self.proposal_list.len();
        let geom =
            rand_distr::Geometric::new(1.0 - (self.prefetched_size as f64) / (elements as f64))
                .unwrap();

        let mut buffer_index = -1;
        loop {
            buffer_index += geom.sample(&mut self.rng) as isize + 1;
            if buffer_index > self.index_buffer.len() as isize {
                break;
            }

            let proposal_index: usize = self.rng.gen_range(self.prefetched_size..elements);
            unsafe {
                // the saturating_sub is a quick hack to avoid a conditional branch (sat_sub is
                // implemented via conditional move. Prefetching an illegal address is okay, and
                // we will always prefetch the same address. Thus the cache pollution is minimal)
                prefetch_read_data(
                    self.proposal_list
                        .as_ptr()
                        .add(proposal_index.saturating_sub(self.num_nodes)),
                    1,
                );
            };

            *self.index_buffer.get_mut(buffer_index).unwrap() = proposal_index;
        }

        self.prefetched_size = elements;
    }

    fn sample(&mut self) -> usize {
        let index = self.index_buffer.dequeue().unwrap();
        self.prefetch();

        let read = unsafe {
            *self
                .proposal_list
                .get_unchecked(index.saturating_sub(self.num_nodes))
        };
        if index < self.num_nodes {
            index
        } else {
            read
        }
    }
}

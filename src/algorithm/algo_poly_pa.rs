use super::*;
use rand::prelude::SliceRandom;
use std::cell::Cell;

const SCALE: f64 = 2.0 * (1u64 << 63) as f64;

#[derive(Clone, Copy, Debug, Default)]
struct NodeInfo {
    degree: Node,
    count: Node,
    weight: f64,
    excess: f64,
}

pub struct AlgoPolyPa<R: Rng> {
    rng: R,
    num_total_nodes: Node,
    num_seed_nodes: Node,

    num_current_nodes: Node,

    initial_degree: Node,
    without_replacement: bool,
    resample: bool,
    weight_function: WeightFunction,

    nodes: Vec<NodeInfo>,
    proposal_list: Vec<Node>,
    total_weight: f64,
    wmax: f64,
    wmax_scaled: f64,

    num_samples: Cell<usize>,
    num_samples_to_reject: Cell<usize>,
}

impl<R: Rng> Algorithm<R> for AlgoPolyPa<R> {
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
            rng,
            num_seed_nodes,
            num_total_nodes,
            initial_degree,
            without_replacement,
            weight_function,
            resample,

            total_weight: 0.0,
            nodes: vec![Default::default(); num_total_nodes],
            proposal_list: Vec::with_capacity(7 * num_total_nodes / 3),

            num_current_nodes: 0,
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
        self.num_current_nodes = self.num_seed_nodes;

        for u in 0..self.num_seed_nodes {
            self.update_node_counts_in_proposal_list(u);
        }
    }

    fn run(&mut self, writer: &mut impl EdgeWriter) {
        let mut hosts = vec![0; self.initial_degree];

        for new_node in self.num_seed_nodes..self.num_total_nodes {
            if self.without_replacement {
                for i in 0..hosts.len() {
                    hosts[i] =
                        self.sample_host(|u| hosts[0..i].contains(&u));
                }
            } else {
                for h in &mut hosts {
                    *h = self.sample_host(|_| false);
                }
            }

            self.num_current_nodes = new_node;

            // update neighbors
            for &h in &hosts {
                self.increase_degree(h);
                writer.add_edge(new_node, h);
            }

            self.set_degree(new_node, self.initial_degree);
        }

        println!(
            "Proposals per node: {}",
            self.proposal_list.len() as f64 / self.num_current_nodes as f64
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

impl<R: Rng> AlgoPolyPa<R> {
    fn sample_host(&mut self, reject_early: impl Fn(Node) -> bool) -> Node {
        debug_assert!(!self.proposal_list.is_empty());
        loop {
            self.num_samples.update(|x| x + 1);
            let proposal = *self.proposal_list.as_slice().choose(&mut self.rng).unwrap() as usize;

            if reject_early(proposal) {
                continue;
            }

            self.num_samples_to_reject.update(|x| x + 1);

            let info = self.nodes[proposal];

            let accept = self.rng.gen::<u64>() < (info.excess * self.wmax_scaled) as u64;
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
            ((self.num_current_nodes as f64) * info.weight / self.total_weight).ceil() as usize;

        while info.count < target_count {
            self.proposal_list.push(node);
            info.count += 1;
        }

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

use super::*;
use rand::prelude::SliceRandom;
use std::collections::HashSet;

#[derive(Clone, Copy, Debug, Default)]
struct NodeInfo {
    degree: Node,
    count: Node,
    weight: f64,
}

pub struct AlgoPolyPa {
    num_total_nodes: Node,
    num_seed_nodes: Node,

    num_current_nodes: Node,

    initial_degree: Node,
    without_replacement: bool,
    weight_function: WeightFunction,

    nodes: Vec<NodeInfo>,
    proposal_list: Vec<Node>,
    total_weight: f64,
    wmax: f64,
}

impl Algorithm for AlgoPolyPa {
    fn new(
        num_seed_nodes: Node,
        num_rand_nodes: Node,
        initial_degree: Node,
        without_replacement: bool,
        weight_function: WeightFunction,
    ) -> Self {
        let num_total_nodes = num_seed_nodes + num_rand_nodes;
        Self {
            num_seed_nodes,
            num_total_nodes,
            initial_degree,
            without_replacement,
            weight_function,

            total_weight: 0.0,
            nodes: vec![Default::default(); num_total_nodes],
            proposal_list: Vec::with_capacity(3 * num_total_nodes),

            num_current_nodes: 0,
            wmax: 0.0,
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

    fn run(&mut self, rng: &mut impl Rng, writer: &mut impl EdgeWriter) {
        let mut hosts = HashSet::with_capacity(2 * self.initial_degree);

        for new_node in self.num_seed_nodes..self.num_total_nodes {
            for _ in 0..self.initial_degree {
                let h = self.sample_host(rng, |u| self.without_replacement && hosts.contains(&u));
                hosts.insert(h);
            }

            self.num_current_nodes = new_node;

            // update neighbors
            for &h in &hosts {
                self.increase_degree(h);
                writer.add_edge(new_node, h);
            }

            self.set_degree(new_node, self.initial_degree);

            hosts.clear();
        }
    }
}

impl AlgoPolyPa {
    fn sample_host(&self, rng: &mut impl Rng, reject_early: impl Fn(Node) -> bool) -> Node {
        debug_assert!(!self.proposal_list.is_empty());
        loop {
            let proposal = *self.proposal_list.as_slice().choose(rng).unwrap() as usize;

            if reject_early(proposal) {
                continue;
            }

            let info = self.nodes[proposal];

            if rng.gen_bool(info.weight / (info.count as f64) / self.wmax) {
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

        let excess = info.weight / (info.count as f64);
        if self.wmax < excess {
            self.wmax = excess;
        }
    }

    fn increase_degree(&mut self, node: Node) {
        self.set_degree(node, self.nodes[node as usize].degree + 1);
    }
}

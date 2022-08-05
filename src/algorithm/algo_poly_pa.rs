use super::*;

use rand::distributions::Distribution;
use rand::prelude::SliceRandom;

#[derive(Clone,Copy,Debug,Default)]
struct NodeInfo {
    degree : Node,
    count : Node,
    weight : f64,
}

pub struct AlgoPolyPa {
    num_total_nodes : Node,
    num_seed_nodes : Node,
    initial_degree: Node,
    without_replacement: bool,
    weight_function : WeightFunction,

    nodes : Vec<NodeInfo>,
    proposal_list : Vec<Node>,
    total_weight : f64,
}

impl Algorithm for AlgoPolyPa {
    fn new(num_seed_nodes: Node, num_rand_nodes: Node, initial_degree: Node, without_replacement: bool, weight_function: WeightFunction) -> Self {
        let num_total_nodes = num_seed_nodes + num_rand_nodes;
        Self {
            num_seed_nodes,
            num_total_nodes,
            initial_degree,
            without_replacement,
            weight_function,

            total_weight: 0.0,
            nodes: vec![Default::default(); num_total_nodes],
            proposal_list: Vec::with_capacity( 3 * num_total_nodes),
        }
    }

    fn set_seed_graph(&mut self, edges: impl Iterator<Item=Edge>) {
        for (u, v) in edges {
            self.increase_degree(u);
            self.increase_degree(v);
        }
    }

    fn run(&mut self, rng : &mut impl Rng, writer: &mut impl EdgeWriter) {
        todo!()
    }
}

impl AlgoPolyPa {
    fn sample_host(&self, rng : &mut impl Rng) -> Node {
        debug_assert!(!self.proposal_list.is_empty());
        loop {
            let proposal = self.proposal_list.as_slice().choose(rng).unwrap();


        }
    }


    fn set_degree(&mut self, node : Node, degree: Node) {
        let info = &mut self.nodes[node];
        info.degree = degree;

        let weight_before = info.weight;
        info.weight = self.weight_function.get(degree);
        self.total_weight += info.weight - weight_before;

        let target_count = ((self.num_total_nodes as f64) * info.weight / self.total_weight).ceil() as usize;

        while info.count < target_count {
            self.proposal_list.push(node);
            info.count += 1;
        }
    }

    fn increase_degree(&mut self, node : Node) {
        self.set_degree(node, self.nodes[node as usize].degree + 1);
    }
}
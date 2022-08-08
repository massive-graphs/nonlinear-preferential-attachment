use super::*;

use rand::distributions::Distribution;

pub struct AlgoDynamicWeightedIndex<R: Rng> {
    rng: R,
    num_seed_nodes: Node,
    num_rand_nodes: Node,
    initial_degree: Node,
    without_replacement: bool,

    degrees: Vec<Node>,
    dyn_index: ::dynamic_weighted_index::DynamicWeightedIndex,

    weight_function: WeightFunction,
}

impl<R: Rng> Algorithm<R> for AlgoDynamicWeightedIndex<R> {
    fn new(
        rng: R,
        num_seed_nodes: Node,
        num_rand_nodes: Node,
        initial_degree: Node,
        without_replacement: bool,
        weight_function: WeightFunction,
    ) -> Self {
        Self {
            rng,
            num_seed_nodes,
            num_rand_nodes,
            initial_degree,
            without_replacement,
            weight_function,

            degrees: vec![0; num_seed_nodes + num_rand_nodes],
            dyn_index: ::dynamic_weighted_index::DynamicWeightedIndex::new(
                num_seed_nodes + num_rand_nodes,
            ),
        }
    }

    fn set_seed_graph_degrees(&mut self, degrees: impl Iterator<Item = Node>) {
        for (u, degree) in degrees.enumerate() {
            self.set_degree(u as Node, degree);
        }
    }

    fn run(&mut self, writer: &mut impl EdgeWriter) {
        let mut hosts = vec![0; self.initial_degree as usize];

        for new_node in self.num_seed_nodes..(self.num_seed_nodes + self.num_rand_nodes) {
            for h in &mut hosts {
                *h = self.dyn_index.sample(&mut self.rng).unwrap();
                if self.without_replacement && self.initial_degree > 1 {
                    self.dyn_index.remove_weight(*h);
                };
            }

            // update neighbors
            for &h in &hosts {
                self.increase_degree(h);
                writer.add_edge(new_node, h);
            }

            self.set_degree(new_node, self.initial_degree);
        }
    }
}

impl<R: Rng> AlgoDynamicWeightedIndex<R> {
    fn set_degree(&mut self, node: Node, degree: Node) {
        self.degrees[node as usize] = degree;
        self.dyn_index
            .set_weight(node as usize, self.weight_function.get(degree));
    }

    fn increase_degree(&mut self, node: Node) {
        self.set_degree(node, self.degrees[node as usize] + 1);
    }
}

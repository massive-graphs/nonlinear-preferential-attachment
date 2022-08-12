use super::*;
use crate::algorithm::algo_parallel_poly_pa::run_length::RunlengthSampler;
use crossbeam::atomic::AtomicCell;

pub(super) struct NodeInfo {
    pub(super) degree: AtomicCell<Node>,
    pub(super) count: AtomicCell<Node>,
    pub(super) weight: AtomicF64,
}

impl Default for NodeInfo {
    fn default() -> Self {
        Self {
            degree: AtomicCell::new(0),
            count: AtomicCell::new(1),
            weight: AtomicF64::new(0.0),
        }
    }
}

pub(super) struct State {
    pub(super) num_total_nodes: Node,
    pub(super) num_seed_nodes: Node,

    pub(super) initial_degree: Node,

    #[allow(dead_code)]
    pub(super) without_replacement: bool,
    pub(super) weight_function: WeightFunction,

    pub(super) nodes: Vec<NodeInfo>,

    pub(super) proposal_list: Arc<ProposalList>,
    pub(super) total_weight: AtomicF64,
    pub(super) runlength_sampler: RunlengthSampler,

    pub(super) wmax: AtomicF64,
    pub(super) max_degree: AtomicCell<usize>,
}

impl State {
    pub(super) fn sequential_set_degree(&self, node: Node, degree: Node) {
        let info = &self.nodes[node];
        let old_degree = info.degree.swap(degree);

        self.max_degree.fetch_max(degree);

        let old_weight = self.weight_function.get(old_degree);
        let new_weight = self.weight_function.get(degree);

        info.weight.fetch_max(new_weight, Ordering::AcqRel);

        self.total_weight
            .fetch_add(new_weight - old_weight, Ordering::AcqRel);
    }

    pub(super) fn sequential_increase_degree(&self, node: Node) {
        self.sequential_set_degree(node, self.nodes[node].degree.load() + 1);
    }

    pub(super) fn sequential_update_node_counts_in_proposal_list(&self, node: Node) {
        let info = &self.nodes[node];
        let target_count = ((self.num_seed_nodes as f64) * info.weight.load(Ordering::Relaxed)
            / self.total_weight.load(Ordering::Acquire))
        .ceil() as usize;

        if info.count.load() < target_count {
            self.proposal_list
                .unbuffered_push(node, target_count - info.count.load());
            info.count.store(target_count);
        }

        let excess = info.weight.load(Ordering::Relaxed) / (info.count.load() as f64);
        self.wmax.fetch_max(excess, Ordering::AcqRel);
    }
}

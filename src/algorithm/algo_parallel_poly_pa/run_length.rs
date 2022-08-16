use super::*;
use crate::weight_function::Regime;
use rand_distr::Geometric;

const BLOCK_LEN: usize = 100;

pub(super) struct RunlengthSampler {
    weight_function: WeightFunction,
    initial_degree: Node,

    total_weight: AtomicF64,
    max_degree: AtomicCell<Node>,

    lower: AtomicCell<Node>,
    upper: AtomicCell<Node>,

    real_lower: AtomicCell<Node>,

    weight_initial_degree: f64,
    weight_max_degree: AtomicF64,
}

impl RunlengthSampler {
    pub(super) fn new(weight_function: WeightFunction, initial_degree: Node) -> Self {
        let weight_initial_degree = weight_function.get(initial_degree);

        Self {
            weight_function,
            initial_degree,
            weight_initial_degree,

            total_weight: Default::default(),
            max_degree: Default::default(),
            lower: Default::default(),
            upper: Default::default(),
            real_lower: Default::default(),
            weight_max_degree: Default::default(),
        }
    }

    pub(super) fn setup_epoch(
        &self,
        lower: Node,
        upper: Node,
        max_degree: Node,
        total_weight: f64,
    ) {
        self.lower.store(lower);
        self.real_lower.store(lower);

        self.upper.store(upper - 1);

        self.total_weight.store(total_weight, Ordering::Release);
        self.max_degree.store(max_degree);
        self.weight_max_degree
            .store(self.weight_function.get(max_degree), Ordering::Release);
    }

    pub(super) fn sample(&self, rng: &mut impl Rng) {
        loop {
            let start_node = self.lower.fetch_add(BLOCK_LEN);
            let upper = self.upper.load();
            if start_node > upper {
                return;
            }

            for node in start_node..upper.min(start_node + BLOCK_LEN) {
                if !self.is_independent_run(rng, node, self.initial_degree) {
                    self.upper.fetch_min(node);
                    return;
                }
            }
        }
    }

    /// In a parallel context, the result is only valid if there's a barrier between sample and result.
    pub(super) fn result(&self) -> Node {
        self.upper.load() + 1
    }

    fn is_independent_run(
        &self,
        rng: &mut impl Rng,
        node: usize,
        sampling_attempts: usize,
    ) -> bool {
        let prob_single_is_independent = self.probability_is_independent(node);
        let prob_all_independent = prob_single_is_independent.powi(sampling_attempts as i32);
        rng.gen_bool(prob_all_independent)
    }

    fn probability_is_independent(&self, node: usize) -> f64 {
        let nodes_in_epoch = node - self.real_lower.load();
        let hosts_in_epoch = nodes_in_epoch * self.initial_degree;

        let total_weight = self.total_weight.load(Ordering::Acquire);

        let upper_bound_weight_increase = match self.weight_function.regime() {
            Regime::Sublinear => {
                self.weight_initial_degree * nodes_in_epoch as f64 + hosts_in_epoch as f64
                // TODO: hosts_in_epoc is crude; may use min-degree
            }
            Regime::Superlinear => {
                let ub_dmax = self.max_degree.load() + nodes_in_epoch;
                let weight_ub_dmax = self.weight_function.get(ub_dmax);

                self.weight_initial_degree * nodes_in_epoch as f64
                    + (weight_ub_dmax - self.weight_max_degree.load(Ordering::Acquire))
                        * self.initial_degree as f64
            }
            Regime::Linear => 2.0 * hosts_in_epoch as f64,
        };

        total_weight / (total_weight + upper_bound_weight_increase)
    }
}

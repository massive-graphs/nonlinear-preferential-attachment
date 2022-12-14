use super::*;
use crate::weight_function::Regime;

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

        let _ = self
            .upper
            .fetch_update(|u| if u <= lower { Some(upper) } else { None });

        self.total_weight.store(total_weight, Ordering::Release);
        self.max_degree.store(max_degree);
        self.weight_max_degree
            .store(self.weight_function.get(max_degree), Ordering::Release);
    }

    #[allow(dead_code)]
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
    pub(super) fn result(&self) -> (Node, f64) {
        let upper_bound = self.upper.load();

        (
            upper_bound,
            self.total_weight_and_upper_bound_for(upper_bound).1,
        )
    }

    pub(super) fn continue_with_node(
        &self,
        rng: &mut impl Rng,
        node: Node,
        sampling_attempts: usize,
    ) -> bool {
        if self.upper.load() <= node {
            return false;
        }

        if self.is_independent_run(rng, node, sampling_attempts) {
            return true;
        }

        self.upper.fetch_min(node);

        false
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

    fn probability_is_independent(&self, node: Node) -> f64 {
        let (total_weight, upper_bound) = self.total_weight_and_upper_bound_for(node);
        total_weight / upper_bound
    }

    pub(super) fn total_weight_and_upper_bound_for(&self, node: Node) -> (f64, f64) {
        let nodes_in_epoch = node - self.real_lower.load();
        let hosts_in_epoch = nodes_in_epoch * self.initial_degree;

        let total_weight = self.total_weight.load(Ordering::Relaxed);

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

        (total_weight, total_weight + upper_bound_weight_increase)
    }
}

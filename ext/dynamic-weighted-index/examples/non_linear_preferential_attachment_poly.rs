use rand::prelude::*;


const NUM_PRECOMPUTED : usize = 100;
struct WeightFunction {
    exponent : f64,
    offset : f64,
    cached : [ f64 ; NUM_PRECOMPUTED],
}

impl WeightFunction {
    pub fn new(exponent : f64, offset : f64) -> Self {
        let mut cached = [0.0 ; NUM_PRECOMPUTED];

        for (degree, weight) in cached.iter_mut().enumerate() {
            *weight = Self::compute(exponent, offset, degree);
        }

        Self {
            exponent,
            offset,
            cached
        }
    }

    pub fn get(&self, degree : usize) -> f64 {
        if NUM_PRECOMPUTED > degree {
            unsafe { *self.cached.get_unchecked(degree) }
        } else {
            Self::compute(self.exponent, self.offset, degree)
        }
    }

    #[inline]
    fn compute(exponent : f64, offset : f64, degree : usize) -> f64 {
        (degree as f64).powf(exponent) + offset
    }
}

#[derive(Default)]
struct NodeInfo {
    degree : usize,
    count : usize,
    weight : f64,
}

struct PolyPA {
    total_weight : f64,
    weight_function : WeightFunction,
    max_weight : f64,
    nodes : Vec<NodeInfo>,
    proposal : Vec<usize>,
}

impl PolyPA {



    fn sample_host(&self, rng : &mut impl Rng) -> usize {
        loop {
            let host = *self.proposal.as_slice().choose(rng).unwrap();
            let node = &self.nodes[host];

            if rng.gen_bool(node.weight / self.max_weight) {
                break host;
            }
        }
    }

    fn set_degree(&mut self, node : usize, degree : usize) {
        let weight = self.weight_function.get(degree);
        if self.max_weight >
    }
}


fn main() {
    println!("Hello, world!");
}
now

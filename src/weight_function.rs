use super::Node;
use std::cmp::Ordering;

const NUM_PRECOMPUTED: usize = 100;

pub struct WeightFunction {
    exponent: f64,
    offset: f64,
    precomputed: [f64; NUM_PRECOMPUTED],
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum Regime {
    Sublinear,
    Linear,
    Superlinear,
}

/// Implements the function `f(d) = d**exponent + offset` with pre-computation of the first few values.
///
/// # Example
/// ```
/// use rust_nlpa::weight_function::WeightFunction;
/// let wf = WeightFunction::new(2.0, 5.0);
///
/// let computed = wf.get(3);
/// let expected = 3.0 * 3.0 + 5.0;
///
/// assert!( (computed - expected).abs() < 1e-6 );
/// ```
impl WeightFunction {
    pub fn new(exponent: f64, offset: f64) -> Self {
        let mut precomputed = [0.0; NUM_PRECOMPUTED];

        for (degree, weight) in precomputed.iter_mut().enumerate() {
            *weight = Self::compute(exponent, offset, degree);
        }

        Self {
            exponent,
            offset,
            precomputed,
        }
    }

    pub fn get(&self, degree: Node) -> f64 {
        if NUM_PRECOMPUTED > degree {
            unsafe { *self.precomputed.get_unchecked(degree as usize) }
        } else {
            Self::compute(self.exponent, self.offset, degree)
        }
    }

    pub fn offset(&self) -> f64 {
        self.offset
    }

    pub fn exponent(&self) -> f64 {
        self.exponent
    }

    pub fn regime(&self) -> Regime {
        match self.exponent.partial_cmp(&1.0).unwrap() {
            Ordering::Less => Regime::Sublinear,
            Ordering::Equal => Regime::Linear,
            Ordering::Greater => Regime::Superlinear,
        }
    }

    #[inline]
    fn compute(exponent: f64, offset: f64, degree: Node) -> f64 {
        (degree as f64).powf(exponent) + offset
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn validate(wf: WeightFunction, reference: impl Fn(Node) -> f64) {
        for d in 0..2 * NUM_PRECOMPUTED {
            let w = wf.get(d);
            let r = reference(d);

            let rel_err = ((w - r) / r).abs();

            assert!(
                w == r || rel_err < 1e-6,
                "wf: {} ref: {} rel_err: {}      exp: {} offset: {} degree: {}",
                w,
                r,
                rel_err,
                wf.exponent,
                wf.offset,
                d
            );
        }
    }

    #[test]
    fn cross_constant() {
        validate(WeightFunction::new(0.0, 0.0), |_| 1.0);
        validate(WeightFunction::new(0.0, 1.0), |_| 2.0);
    }

    #[test]
    fn cross_sqrt() {
        validate(WeightFunction::new(0.5, 0.0), |d| (d as f64).sqrt());
        validate(WeightFunction::new(0.5, 2.0), |d| (d as f64).sqrt() + 2.0);
    }

    #[test]
    fn cross_linear() {
        validate(WeightFunction::new(1.0, 0.0), |d| d as f64);
        validate(WeightFunction::new(1.0, 3.0), |d| d as f64 + 3.0);
    }

    #[test]
    fn cross_sqare() {
        validate(WeightFunction::new(2.0, 0.0), |d| (d * d) as f64);
        validate(WeightFunction::new(2.0, 4.0), |d| (d * d) as f64 + 4.0);
    }
}

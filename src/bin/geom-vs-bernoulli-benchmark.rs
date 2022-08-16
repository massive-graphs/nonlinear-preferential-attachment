use pcg_rand::Pcg64;
use rand::prelude::*;
use rand::SeedableRng;
use rand_distr::num_traits::Pow;
use rand_distr::Geometric;
use std::time::{Duration, Instant};

const NUM_REPEATS: u64 = 10;
const NUM_MICRO_REPEATS: u64 = 100000;
const MAX_EXPECTED_LEN: u64 = 50;

fn benchmark_geom(rng: &mut impl Rng, exp_len: u64) -> Duration {
    let mut len: u64 = 0;
    let start = Instant::now();

    let p = 1.0 / exp_len as f64;
    for _ in 0..NUM_MICRO_REPEATS {
        len += Geometric::new(p).unwrap().sample(rng);
    }

    let elapsed = start.elapsed();

    let expected = exp_len * NUM_MICRO_REPEATS;
    assert!(((expected / 2)..(2 * expected)).contains(&len));

    println!(
        "geom,{},{}",
        exp_len,
        elapsed.as_nanos() * 1000 / NUM_MICRO_REPEATS as u128
    );
    elapsed
}

fn benchmark_pow_bernoulli(rng: &mut impl Rng, exp_len: u64) -> Duration {
    let mut len: u64 = 0;
    let start = Instant::now();

    let p = 1.0 - (1.0 - 1.0 / exp_len as f64).pow(exp_len as f64 / 2.0);

    for _ in 0..NUM_MICRO_REPEATS {
        let p = 1.0 - (1.0 - 1.0 / exp_len as f64).pow(exp_len as f64 / 2.0);
        len += rng.gen_bool(p) as u64;
    }

    let elapsed = start.elapsed();
    let expected = (NUM_MICRO_REPEATS as f64 * p) as u64;
    assert!(((expected / 2)..(2 * expected)).contains(&len));

    println!(
        "bern,{},{}",
        exp_len,
        elapsed.as_nanos() * 1000 / NUM_MICRO_REPEATS as u128
    );
    elapsed
}

fn benchmark_bernoulli(rng: &mut impl Rng, exp_len: u64) -> Duration {
    let mut len: u64 = 0;
    let start = Instant::now();

    let p = 1.0 / exp_len as f64;
    for _ in 0..NUM_MICRO_REPEATS {
        len += rng.gen_bool(p) as u64;
    }

    let elapsed = start.elapsed();
    let expected = (NUM_MICRO_REPEATS as f64 * p) as u64;
    assert!(((expected / 2)..(2 * expected)).contains(&len));

    println!(
        "pbern,{},{}",
        exp_len,
        elapsed.as_nanos() * 1000 / NUM_MICRO_REPEATS as u128
    );
    elapsed
}

fn main() {
    let mut rng = Pcg64::seed_from_u64(123456);

    for _ in 0..NUM_REPEATS {
        for exp_len in 2..MAX_EXPECTED_LEN {
            let geom = benchmark_geom(&mut rng, exp_len);
            let pbern = benchmark_pow_bernoulli(&mut rng, exp_len);
            let bern = benchmark_bernoulli(&mut rng, exp_len);
            println!(
                "fac,{},{}",
                exp_len,
                geom.as_secs_f64() / bern.as_secs_f64()
            );
        }
    }
}

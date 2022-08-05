use std::time::Instant;
use rust_nlpa::parameters::{get_and_check_options, Parameters, SamplingAlgorithm};

use pcg_rand::Pcg64;
use rand::SeedableRng;
use rust_nlpa::algorithm::algo_dynamic_weighted_index::AlgoDynamicWeightedIndex;
use rust_nlpa::algorithm::algo_poly_pa::AlgoPolyPa;
use rust_nlpa::algorithm::Algorithm;
use rust_nlpa::edge_writer::EdgeCounter;

fn execute<T : Algorithm>(opt : &Parameters) {
    let mut rng = if let Some(seed_value) = opt.seed_value {
        Pcg64::seed_from_u64(seed_value)
    } else {
        Pcg64::from_entropy()
    };

    let mut algorithm = T::from_parameters(opt);

    // 1-regular graph
    algorithm.set_seed_graph( (0..opt.seed_nodes.unwrap() / 2).into_iter().map(|u| (2*u, 2*u + 1)));

    let mut writer = EdgeCounter::default();

    let runtime = {
        let start = Instant::now();
        algorithm.run(&mut rng, &mut writer);
        start.elapsed()
    };

    assert_eq!(writer.number_of_edges(), opt.nodes * opt.initial_degree);

    println!("runtime_s:{}", runtime.as_secs_f64());
}


fn main() {
    let opt = get_and_check_options();

    match opt.algorithm {
        SamplingAlgorithm::DynWeightIndex => execute::<AlgoDynamicWeightedIndex>( &opt),
        SamplingAlgorithm::PolyPA => execute::<AlgoPolyPa>(&opt),
    };
}

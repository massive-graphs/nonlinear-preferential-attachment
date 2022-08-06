use rust_nlpa::parameters::{get_and_check_options, Parameters, SamplingAlgorithm};
use std::io::stdout;
use std::time::Instant;

use pcg_rand::Pcg64;
use rand::SeedableRng;
use rust_nlpa::algorithm::algo_dynamic_weighted_index::AlgoDynamicWeightedIndex;
use rust_nlpa::algorithm::algo_poly_pa::AlgoPolyPa;
use rust_nlpa::algorithm::Algorithm;
use rust_nlpa::edge_writer::{DegreeCount, EdgeCounter};

fn execute<T: Algorithm>(opt: &Parameters) {
    let mut rng = if let Some(seed_value) = opt.seed_value {
        Pcg64::seed_from_u64(seed_value)
    } else {
        Pcg64::from_entropy()
    };

    let mut algorithm = T::from_parameters(opt);

    // 1-regular graph
    algorithm.set_seed_graph_degrees((0..opt.seed_nodes.unwrap()).into_iter().map(|_| 1));

    let runtime = if opt.report_degree_distribution {
        let mut writer = DegreeCount::new(opt.seed_nodes.unwrap() + opt.nodes);

        let start = Instant::now();
        algorithm.run(&mut rng, &mut writer);
        assert_eq!(writer.number_of_edges(), opt.nodes * opt.initial_degree);
        let duration = start.elapsed();

        writer.report_distribution(&mut stdout().lock()).unwrap();

        duration
    } else {
        let mut writer = EdgeCounter::default();
        let start = Instant::now();

        algorithm.run(&mut rng, &mut writer);
        assert_eq!(writer.number_of_edges(), opt.nodes * opt.initial_degree);

        start.elapsed()
    };

    println!("runtime_s:{}", runtime.as_secs_f64());
}

fn main() {
    let opt = get_and_check_options();

    match opt.algorithm {
        SamplingAlgorithm::DynWeightIndex => execute::<AlgoDynamicWeightedIndex>(&opt),
        SamplingAlgorithm::PolyPA => execute::<AlgoPolyPa>(&opt),
    };
}

use rust_nlpa::parameters::{get_and_check_options, Parameters, SamplingAlgorithm};
use std::io::stdout;
use std::time::Instant;

use pcg_rand::Pcg64;
use rand::SeedableRng;
use rust_nlpa::algorithm::algo_dynamic_weighted_index::AlgoDynamicWeightedIndex;
use rust_nlpa::algorithm::algo_parallel_poly_pa::AlgoParallelPolyPa;
use rust_nlpa::algorithm::algo_poly_pa::AlgoPolyPa;
use rust_nlpa::algorithm::algo_poly_pa_prefetch::AlgoPolyPaPrefetch;
use rust_nlpa::algorithm::Algorithm;
use rust_nlpa::edge_writer::{degree_distribution, report_distribution, EdgeCounter};

fn execute<R: rand::Rng, T: Algorithm<R>>(rng: R, opt: &Parameters) {
    let mut algorithm = T::from_parameters(rng, opt);

    // 1-regular graph
    algorithm.set_seed_graph_degrees((0..opt.seed_nodes.unwrap()).into_iter().map(|_| 1));

    let runtime = {
        let mut writer = EdgeCounter::default();
        let start = Instant::now();

        algorithm.run(&mut writer);
        let runtime = start.elapsed();

        let degrees = algorithm.degrees();

        if opt.report_degree_distribution {
            let distr = degree_distribution(degrees.iter().copied());
            report_distribution(&distr, &mut stdout().lock()).unwrap();
        }

        /*
                assert_eq!(
                    degrees.iter().copied().sum::<usize>(),
                    opt.seed_nodes.unwrap() + 2 * opt.nodes * opt.initial_degree
                );
        s         */

        runtime
    };

    println!("runtime_s:{}", runtime.as_secs_f64());
}

fn main() {
    let opt = get_and_check_options();

    let rng = if let Some(seed_value) = opt.seed_value {
        Pcg64::seed_from_u64(seed_value)
    } else {
        Pcg64::from_entropy()
    };

    match opt.algorithm {
        SamplingAlgorithm::DynWeightIndex => execute::<_, AlgoDynamicWeightedIndex<_>>(rng, &opt),
        SamplingAlgorithm::PolyPA => execute::<_, AlgoPolyPa<_>>(rng, &opt),
        SamplingAlgorithm::PolyPAPrefetch => execute::<_, AlgoPolyPaPrefetch<_>>(rng, &opt),
        SamplingAlgorithm::ParallelPolyPa => execute::<_, AlgoParallelPolyPa<_>>(rng, &opt),
    };
}

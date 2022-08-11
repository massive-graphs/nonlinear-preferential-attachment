use std::str::FromStr;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "non_linear_preferential_attachment",
    about = "Generates an edge list using non-linear preferential attachment"
)]
pub struct Parameters {
    #[structopt(short = "a", long, default_value = "dyn")]
    pub algorithm: SamplingAlgorithm,

    #[structopt(short = "i", long)]
    pub seed_nodes: Option<usize>,

    #[structopt(short = "s", long)]
    pub seed_value: Option<u64>,

    #[structopt(short = "n", long)]
    pub nodes: usize,

    #[structopt(short = "d", long, default_value = "1")]
    pub initial_degree: usize,

    #[structopt(short = "e", long, default_value = "1.0")]
    pub exponent: f64,

    #[structopt(short = "c", long, default_value = "0.0")]
    pub offset: f64,

    #[structopt(short = "p", long)]
    pub without_replacement: bool,

    #[structopt(short = "l", long)]
    pub resample_previous: bool,

    #[structopt(short = "r", long)]
    pub report_degree_distribution: bool,

    #[structopt(short = "t", long)]
    pub num_threads: Option<usize>,
}

#[derive(Eq, Clone, Copy, PartialEq, Debug)]
pub enum SamplingAlgorithm {
    DynWeightIndex,
    PolyPA,
    PolyPAPrefetch,
    ParallelPolyPa,
}

impl FromStr for SamplingAlgorithm {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "dyn" => Ok(SamplingAlgorithm::DynWeightIndex),
            "polypa" => Ok(SamplingAlgorithm::PolyPA),
            "polypa-prefetch" => Ok(SamplingAlgorithm::PolyPAPrefetch),
            "par-polypa" => Ok(SamplingAlgorithm::ParallelPolyPa),
            _ => Err(format!("Unknown algorithm type: {}", s)),
        }
    }
}

pub fn get_and_check_options() -> Parameters {
    let mut opt = Parameters::from_args();

    assert!(opt.initial_degree >= 1);
    if opt.seed_nodes.is_none() {
        opt.seed_nodes = Some(opt.initial_degree * 10);
    }
    assert!(opt.seed_nodes.unwrap() >= opt.initial_degree);
    assert_eq!(opt.seed_nodes.unwrap() % 2, 0);

    assert!(opt.exponent >= 0.0);
    assert!(opt.offset >= 0.0);

    assert!(opt.num_threads.unwrap_or(1) > 0);

    opt
}

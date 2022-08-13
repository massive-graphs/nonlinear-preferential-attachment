use crossbeam::atomic::AtomicCell;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

trait Master: Sync + Send {
    const LABEL: &'static str;
    fn new(num_threads: usize) -> Self;

    type Local: Waiter + 'static;
    fn waiter(&self) -> Self::Local;
}

trait Waiter: Sync + Send {
    fn bench_wait(&mut self);
}

fn run_multiple_threads<W>(num_threads: usize)
where
    W: Master,
{
    const REPEATS: u32 = 1000;

    let mut handles = Vec::with_capacity(num_threads);

    let wait_master = W::new(num_threads);
    let barrier = Arc::new(Barrier::new(num_threads));

    let runtime_ns = Arc::new(AtomicCell::new(0_u128));

    for _ in 0..num_threads {
        let barrier = barrier.clone();
        let mut waiter = wait_master.waiter();
        let runtime_ns = runtime_ns.clone();

        handles.push(thread::spawn(move || {
            barrier.wait();

            let start = Instant::now();
            for _ in 0..REPEATS {
                waiter.bench_wait();
            }
            let duration = start.elapsed();

            runtime_ns.fetch_add(duration.as_nanos());
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    println!(
        "{:<10},{:>3},{:>6}",
        W::LABEL,
        num_threads,
        runtime_ns.load() / num_threads as u128 / REPEATS as u128
    );
}

struct StdBarrier {
    barrier: Arc<std::sync::Barrier>,
}

impl Master for StdBarrier {
    const LABEL: &'static str = "StdBarrier";
    type Local = Arc<std::sync::Barrier>;

    fn new(num_threads: usize) -> Self {
        Self {
            barrier: Arc::new(std::sync::Barrier::new(num_threads)),
        }
    }

    fn waiter(&self) -> Self::Local {
        self.barrier.clone()
    }
}

impl Waiter for Arc<std::sync::Barrier> {
    fn bench_wait(&mut self) {
        self.wait();
    }
}

struct HurdlesBarrier {
    barrier: hurdles::Barrier,
}

impl Master for HurdlesBarrier {
    const LABEL: &'static str = "Hurdles";

    fn new(num_threads: usize) -> Self {
        Self {
            barrier: hurdles::Barrier::new(num_threads),
        }
    }

    type Local = hurdles::Barrier;

    fn waiter(&self) -> Self::Local {
        self.barrier.clone()
    }
}

impl Waiter for hurdles::Barrier {
    fn bench_wait(&mut self) {
        self.wait();
    }
}

fn main() {
    for _ in 0..10 {
        for t in 1..num_cpus::get() {
            run_multiple_threads::<StdBarrier>(t);
            run_multiple_threads::<HurdlesBarrier>(t);
        }
    }
}

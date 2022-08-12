const UNINITIALIZED: Node = Node::MAX;
const BLOCK_SIZE: usize = 128;

use super::*;
use crossbeam::atomic::AtomicCell;
use itertools::Itertools;
use std::intrinsics::likely;
use std::ops::Range;

struct AtomicBlockInfo {
    begin: AtomicCell<usize>,
    end: AtomicCell<usize>,
}

impl Default for AtomicBlockInfo {
    fn default() -> Self {
        Self {
            begin: AtomicCell::new(0),
            end: AtomicCell::new(0),
        }
    }
}

impl AtomicBlockInfo {
    fn get_range(&self) -> Range<usize> {
        self.begin.load()..self.end.load()
    }
}

pub(super) struct ProposalList {
    proposal_list: Vec<AtomicCell<Node>>,
    begin_of_next_block: AtomicCell<usize>,
    unfinished_blocks: Vec<AtomicBlockInfo>,
    producer_id: AtomicCell<usize>,
}

impl ProposalList {
    pub fn new(size: usize, num_threads: usize) -> Self {
        let n = size + 2 * num_threads * BLOCK_SIZE;

        let mut proposal_list = Vec::with_capacity(n);
        for _ in 0..n {
            proposal_list.push(AtomicCell::new(UNINITIALIZED));
        }

        let unfinished_blocks = (0..num_threads).map(|_| Default::default()).collect();

        Self {
            proposal_list,
            unfinished_blocks,
            begin_of_next_block: AtomicCell::new(0),
            producer_id: AtomicCell::new(0),
        }
    }

    pub fn unbuffered_push(&self, node: Node, mut count: usize) {
        while count > 0 {
            self.proposal_list[self.begin_of_next_block.fetch_add(1)].store(node);
            count -= 1;
        }
    }

    pub fn compact_unfinished_ranges(&self) -> usize {
        // we cannot borrow self as mut (as shared between threads) and work on a (non-atomic) copy
        let mut gap_list = self
            .unfinished_blocks
            .iter()
            .map(|b| b.get_range())
            .filter(|r| !r.is_empty())
            .collect_vec();

        if gap_list.is_empty() {
            return self.begin_of_next_block.load();
        }

        gap_list.sort_unstable_by_key(|r| -(r.start as isize));

        let active_range =
            gap_list.last().unwrap().start.saturating_sub(1)..gap_list.first().unwrap().end;

        let mut set_list = gap_list
            .iter()
            .rev()
            .tuple_windows()
            .map(|(pred, suc)| pred.end..suc.start)
            .collect_vec();

        if gap_list.first().unwrap().end != self.begin_of_next_block.load() {
            // the proposal lists ends with valid data
            set_list.push(gap_list.first().unwrap().end..self.begin_of_next_block.load());
        }

        // uninit_blocks is sorted decreasingly, i.e. the "left-most" block in proposal list is at the back
        // init_blocks is sorted increasingly. We chose this order since we will pop from the back
        let end = self.compact_from_lists(&mut gap_list, &mut set_list);

        // ensure compaction worked: all elements up to end are initilized; all remaining are uninitialized
        debug_assert!(self.proposal_list[active_range.start..end]
            .iter()
            .all(|p| p.load() != UNINITIALIZED));

        debug_assert!(self.proposal_list[end..active_range.end]
            .iter()
            .all(|p| p.load() == UNINITIALIZED));

        self.begin_of_next_block.store(end);

        end
    }

    fn compact_from_lists(
        &self,
        gap_list: &mut Vec<Range<usize>>,
        set_list: &mut Vec<Range<usize>>,
    ) -> usize {
        // ensure all "gaps" are uninitialized and "sets" are initilized
        debug_assert!(gap_list.iter().all(|r| self.proposal_list[r.clone()]
            .iter()
            .all(|p| p.load() == UNINITIALIZED)));

        debug_assert!(set_list.iter().all(|r| self.proposal_list[r.clone()]
            .iter()
            .all(|p| p.load() != UNINITIALIZED)));

        if set_list.is_empty() {
            return gap_list.first().unwrap().start;
        }

        let mut set_range = set_list.pop().unwrap();
        'compacting: loop {
            if gap_list.len() == 1 {
                break gap_list.first().unwrap().start.min(set_range.end);
            }

            let mut gap_range = gap_list.pop().unwrap();

            if set_range.end < gap_range.start {
                break 'compacting set_range.end;
            }

            loop {
                if set_range.is_empty() {
                    match set_list.pop() {
                        Some(b) if b.start >= gap_range.end => set_range = b,
                        _ => break 'compacting gap_range.start,
                    }
                }

                set_range.end -= 1;

                let value = self.proposal_list[set_range.end].load();
                debug_assert!(value != UNINITIALIZED);
                self.proposal_list[gap_range.start].store(value);
                self.proposal_list[set_range.end].store(UNINITIALIZED);

                gap_range.start += 1;

                if gap_range.is_empty() {
                    break;
                }
            }
        }
    }
}

pub(super) struct Sampler {
    proposal_list: Arc<ProposalList>,
    end: usize,
}

impl Sampler {
    pub(super) fn new(proposal_list: Arc<ProposalList>) -> Self {
        let end = proposal_list.begin_of_next_block.load();
        Self { proposal_list, end }
    }

    #[inline]
    pub fn sample(&self, rng: &mut impl Rng) -> Node {
        self.sample_with_explicit_begin(rng, 0)
    }

    pub fn sample_with_explicit_begin(&self, rng: &mut impl Rng, begin: usize) -> Node {
        debug_assert!(begin < self.end);

        loop {
            let index = rng.gen_range(begin..self.end);
            let proposal = unsafe { self.proposal_list.proposal_list.get_unchecked(index) }.load();
            if likely(proposal != UNINITIALIZED) {
                break proposal;
            }
        }
    }

    #[inline]
    pub fn update_end(&mut self) {
        self.end = self.proposal_list.begin_of_next_block.load();
    }

    #[inline]
    #[allow(dead_code)]
    pub fn end(&self) -> usize {
        self.end
    }
}

pub(super) struct Writer {
    proposal_list: Arc<ProposalList>,
    producer_id: usize,
    begin: usize,
    end: usize,
}

impl Writer {
    pub(super) fn new(proposal_list: Arc<ProposalList>) -> Self {
        let producer_id = proposal_list.producer_id.fetch_add(1);
        assert!(producer_id < proposal_list.unfinished_blocks.len());

        Self {
            proposal_list,
            producer_id,
            begin: 0,
            end: 0,
        }
    }

    pub(super) fn push(&mut self, node: Node, mut count: usize) {
        debug_assert!(node != UNINITIALIZED);

        while count > 0 {
            if self.begin == self.end {
                self.fetch_new_range();
            }

            let this_count = count.min(self.end - self.begin);
            for _ in 0..this_count {
                // this access is safe, since we checked the validity in fetch_new_range and
                // the Arc prevents modification of the vector size
                unsafe { self.proposal_list.proposal_list.get_unchecked(self.begin) }.store(node);
                self.begin += 1;
            }

            count -= this_count;
        }
    }

    pub(super) fn free_unfinished_range(&mut self) {
        let info = &self.proposal_list.unfinished_blocks[self.producer_id];
        info.begin.store(self.begin);
        info.end.store(self.end);

        self.begin = self.end;
    }

    fn fetch_new_range(&mut self) {
        self.begin = self.proposal_list.begin_of_next_block.fetch_add(BLOCK_SIZE);
        assert!(self.begin + BLOCK_SIZE < self.proposal_list.proposal_list.len());
        self.end = self.begin + BLOCK_SIZE;
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use pcg_rand::Pcg64;
    use rand::prelude::IteratorRandom;

    fn run_n_threads<F>(size: usize, num_threads: usize, callback: Arc<F>)
    where
        F: Fn(usize, (usize, usize), Arc<ProposalList>, Arc<Barrier>) + Send + Sync + 'static,
    {
        let proposal_list = Arc::new(ProposalList::new(size, num_threads));
        let barrier = Arc::new(Barrier::new(num_threads));

        let mut handles = Vec::with_capacity(num_threads);

        for rank in 0..num_threads {
            let proposal_list = proposal_list.clone();
            let barrier = barrier.clone();
            let callback = callback.clone();
            handles.push(thread::spawn(move || {
                callback(size, (rank, num_threads), proposal_list, barrier)
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    fn run_randomized(size: usize, num_threads: usize) {
        run_n_threads(
            size,
            num_threads,
            Arc::new(
                |size,
                 (rank, num_threads),
                 proposal_list: Arc<ProposalList>,
                 barrier: Arc<Barrier>| {
                    let mut rng =
                        Pcg64::seed_from_u64((rank * 34532 + 12345 + size + num_threads) as u64);

                    let elements_to_push = size / num_threads;
                    let mut inds = (0..elements_to_push).choose_multiple(&mut rng, 10);
                    inds.sort_unstable();

                    inds.push(elements_to_push);

                    let mut i = elements_to_push * rank;

                    let mut producer = Writer::new(proposal_list.clone());

                    for num_to_push in inds.iter().tuple_windows().map(|(a, b)| b - a) {
                        for _ in 0..num_to_push {
                            producer.push(i, 1);
                            i += 1;
                        }
                        producer.free_unfinished_range();

                        barrier.wait();

                        if rank == 0 {
                            proposal_list.compact_unfinished_ranges();
                        }

                        barrier.wait();
                    }
                },
            ),
        );
    }

    const SIZES: [usize; 12] = [10, 20, 30, 40, 50, 100, 200, 300, 400, 500, 1000, 10000];

    #[test]
    fn randomized_seq() {
        for size in SIZES {
            run_randomized(size, 1);
        }
    }

    #[test]
    fn randomized_two_threads() {
        for size in SIZES {
            run_randomized(size, 2);
        }
    }

    #[test]
    fn randomized_many_threads() {
        for threads in 3..16 {
            for size in SIZES {
                run_randomized(size, threads);
            }
        }
    }
}

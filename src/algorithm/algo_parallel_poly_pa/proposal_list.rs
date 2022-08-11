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
        let n = size + num_threads * BLOCK_SIZE;

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

    pub fn compact_unfinished_ranges(&self) {
        // we cannot borrow self as mut (as shared between threads) and work on a (non-atomic) copy
        let mut uninit_blocks = self
            .unfinished_blocks
            .iter()
            .map(|b| b.get_range())
            .filter(|r| !r.is_empty())
            .collect_vec();

        if uninit_blocks.is_empty() {
            return;
        }

        uninit_blocks.sort_unstable_by_key(|r| -(r.start as isize));

        let active_range = uninit_blocks.last().unwrap().start.saturating_sub(1)
            ..uninit_blocks.first().unwrap().end;

        debug_assert!(uninit_blocks.iter().all(|r| self.proposal_list[r.clone()]
            .iter()
            .all(|p| p.load() == UNINITIALIZED)));

        let mut init_blocks = uninit_blocks
            .iter()
            .rev()
            .tuple_windows()
            .map(|(suc, pred)| pred.end..suc.start)
            .collect_vec();

        // uninit_blocks is sorted decreasingly, i.e. the "left-most" block in proposal list is at the back
        // init_blocks is sorted increasingly. We chose this order since we will pop from the back
        let mut init = init_blocks.pop().unwrap();
        let end = 'compacting: loop {
            if uninit_blocks.len() == 1 {
                break uninit_blocks.first().unwrap().start;
            }

            // the last block may remains free
            let mut uninit = uninit_blocks.pop().unwrap();

            while !uninit.is_empty() {
                uninit.end -= 1;
                init.end -= 1;

                self.proposal_list[uninit.end].store(self.proposal_list[init.end].load());
                self.proposal_list[init.end].store(UNINITIALIZED);

                if init.is_empty() {
                    match init_blocks.pop() {
                        Some(b) => init = b,
                        None => {
                            if uninit.is_empty() {
                                continue; // first if of loop will take care
                            } else {
                                break 'compacting uninit.start;
                            }
                        }
                    }
                }
            }
        };

        debug_assert!(self.proposal_list[active_range.start..end]
            .iter()
            .all(|p| p.load() != UNINITIALIZED));

        debug_assert!(self.proposal_list[end..active_range.end]
            .iter()
            .all(|p| p.load() == UNINITIALIZED));

        self.begin_of_next_block.store(end);
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
            let proposal = self.proposal_list.proposal_list[index].load(); // we can make this unchecked
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

pub(super) struct Producer {
    proposal_list: Arc<ProposalList>,
    producer_id: usize,
    begin: usize,
    end: usize,
}

impl Producer {
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
        while count > 0 {
            if self.begin == self.end {
                self.fetch_new_range();
            }

            let this_count = count.min(self.end - self.begin);
            for _ in 0..this_count {
                // this access is safe, since we checked the validity in fetch_new_range and
                // the Arc prevents modification of the vector size
                unsafe { self.proposal_list.proposal_list.get_unchecked(self.begin) }.store(node);
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

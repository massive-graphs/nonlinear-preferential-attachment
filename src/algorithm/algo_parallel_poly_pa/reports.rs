use super::*;
use std::intrinsics::likely;
use std::ops::Range;
use std::time::Instant;

pub(super) struct Reporter {
    start: Instant,
    last_report: Instant,

    num_total_nodes: Node,

    epoch_id: Node,
    epoch_nodes: Range<Node>,

    last_report_ended: Node,
    last_report_epoch_id: Node,
}

impl Reporter {
    pub(super) fn new(num_total_nodes: Node) -> Self {
        let now = Instant::now();
        Self {
            start: now,
            last_report: now,
            num_total_nodes,

            epoch_id: 0,
            epoch_nodes: 0..0,

            last_report_ended: 0,
            last_report_epoch_id: 0,
        }
    }

    pub(super) fn update_epoch(&mut self, epoch_id: Node, epoch_nodes: Range<Node>) {
        self.epoch_id = epoch_id;
        self.epoch_nodes = epoch_nodes;
    }

    pub(super) fn report_progress_sometimes(&mut self) {
        let now = Instant::now();
        let duration = now.duration_since(self.last_report);

        if likely(duration.as_secs_f64() < 0.2) {
            return;
        }

        self.report_progress_now(now);
    }

    pub(super) fn report_progress_forced(&mut self) {
        let now = Instant::now();
        self.report_progress_now(now);
    }

    fn report_progress_now(&mut self, now: Instant) {
        let elasped_ms = now.duration_since(self.start).as_millis();

        let nodes_since_last_report = self.epoch_nodes.end - self.last_report_ended;

        println!(
            "{:>7}ms Epoch {:>6} from {:>9} to {:>9} ({:>5.1} %); len: {:>5} (avg: {:>5.1})",
            elasped_ms,
            self.epoch_id,
            self.epoch_nodes.start,
            self.epoch_nodes.end,
            100.0 * self.epoch_nodes.end as f64 / self.num_total_nodes as f64,
            self.epoch_nodes.len(),
            nodes_since_last_report as f64 / (self.epoch_id - self.last_report_epoch_id) as f64
        );

        self.last_report = now;
        self.last_report_ended = self.epoch_nodes.end;
        self.last_report_epoch_id = self.epoch_id;
    }
}

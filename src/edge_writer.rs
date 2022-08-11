#![allow(dead_code)]

use super::*;
use itertools::Itertools;
use std::io::Write;

pub trait EdgeWriter {
    fn add_edge(&mut self, u: Node, v: Node);
}

#[derive(Default, Clone, Debug)]
pub struct EdgeCounter {
    number_of_edges: usize,
}

impl EdgeWriter for EdgeCounter {
    fn add_edge(&mut self, _u: Node, _v: Node) {
        self.number_of_edges += 1;
    }
}

impl EdgeCounter {
    pub fn number_of_edges(&self) -> usize {
        self.number_of_edges
    }
}

#[derive(Clone, Debug)]
pub struct DegreeCount {
    number_of_edges: usize,
    degrees: Vec<usize>,
}

impl DegreeCount {
    pub fn new(number_of_nodes: usize) -> Self {
        Self {
            number_of_edges: 0,
            degrees: vec![0; number_of_nodes],
        }
    }

    pub fn degrees(&self) -> &[usize] {
        &self.degrees
    }

    pub fn number_of_edges(&self) -> usize {
        self.number_of_edges
    }

    pub fn degree_distribution(&self) -> Vec<(usize, usize)> {
        degree_distribution(self.degrees.iter().copied())
    }

    pub fn report_distribution(&self, writer: &mut impl Write) -> std::io::Result<()> {
        let degree_distr = self.degree_distribution();
        report_distribution(&degree_distr, writer)
    }
}

pub fn degree_distribution(degrees: impl Iterator<Item = Node>) -> Vec<(usize, usize)> {
    let mut counts = degrees.counts().into_iter().collect_vec();
    counts.sort_unstable();
    counts
}

pub fn report_distribution(
    degree_distr: &[(usize, usize)],
    writer: &mut impl Write,
) -> std::io::Result<()> {
    writer.write_all(
        degree_distr
            .iter()
            .map(|&(d, n)| format!("#DD {:>10}, {:>10}\n", d, n))
            .join("")
            .as_bytes(),
    )?;
    Ok(())
}

impl EdgeWriter for DegreeCount {
    fn add_edge(&mut self, u: Node, v: Node) {
        self.number_of_edges += 1;
        self.degrees[u] += 1;
        self.degrees[v] += 1;
    }
}

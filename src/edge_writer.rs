use super::*;

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
}

impl EdgeWriter for DegreeCount {
    fn add_edge(&mut self, u: Node, v: Node) {
        self.number_of_edges += 1;
        self.degrees[u] += 1;
        self.degrees[v] += 1;
    }
}

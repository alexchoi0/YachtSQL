#[derive(Debug, Clone)]
pub struct TDigest {
    values: Vec<f64>,
}

impl TDigest {
    pub fn new() -> Self {
        Self { values: Vec::new() }
    }

    pub fn add(&mut self, value: f64) {
        self.values.push(value);
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn quantile(&mut self, q: f64) -> f64 {
        if self.values.is_empty() {
            return 0.0;
        }

        self.values.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let idx = ((self.values.len() as f64 - 1.0) * q) as usize;
        self.values[idx.min(self.values.len() - 1)]
    }

    pub fn quantiles(&mut self, quantiles: &[f64]) -> Vec<f64> {
        if self.values.is_empty() {
            return vec![0.0; quantiles.len()];
        }

        self.values.sort_by(|a, b| a.partial_cmp(b).unwrap());

        quantiles
            .iter()
            .map(|&q| {
                let idx = ((self.values.len() as f64 - 1.0) * q) as usize;
                self.values[idx.min(self.values.len() - 1)]
            })
            .collect()
    }

    pub fn merge(&mut self, other: &TDigest) {
        self.values.extend_from_slice(&other.values);
    }
}

impl Default for TDigest {
    fn default() -> Self {
        Self::new()
    }
}

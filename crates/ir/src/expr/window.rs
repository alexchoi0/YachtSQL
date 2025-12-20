use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WindowFunction {
    RowNumber,
    Rank,
    DenseRank,
    PercentRank,
    CumeDist,
    Ntile,
    Lead,
    Lag,
    FirstValue,
    LastValue,
    NthValue,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WindowFrame {
    pub unit: WindowFrameUnit,
    pub start: WindowFrameBound,
    pub end: Option<WindowFrameBound>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WindowFrameUnit {
    Rows,
    Range,
    Groups,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WindowFrameBound {
    CurrentRow,
    Preceding(Option<u64>),
    Following(Option<u64>),
}

impl Default for WindowFrame {
    fn default() -> Self {
        Self {
            unit: WindowFrameUnit::Range,
            start: WindowFrameBound::Preceding(None),
            end: Some(WindowFrameBound::CurrentRow),
        }
    }
}

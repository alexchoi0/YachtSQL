use criterion::Criterion;
use pprof::criterion::{Output, PProfProfiler};

pub fn profiled_config() -> Criterion {
    Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)))
}

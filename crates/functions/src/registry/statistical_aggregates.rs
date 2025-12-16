use std::rc::Rc;

use super::FunctionRegistry;
use crate::aggregate::AggregateFunction;
use crate::aggregate::approximate::{
    ApproxCountDistinctFunction, ApproxQuantilesFunction, ApproxTopCountFunction,
    ApproxTopSumFunction,
};
use crate::aggregate::array_agg::{ArrayAggFunction, ArrayConcatAggFunction};
use crate::aggregate::bigquery::{
    AnyValueFunction as BqAnyValueFunction, ArrayAggDistinctFunction,
    ArrayConcatAggFunction as BqArrayConcatAggFunction, CorrFunction as BqCorrFunction,
    HllCountExtractFunction, HllCountInitFunction, HllCountMergeFunction,
    StddevFunction as BqStddevFunction, StringAggDistinctFunction,
    VarianceFunction as BqVarianceFunction,
};
use crate::aggregate::boolean_bitwise::{
    AnyValueFunction, BitAndFunction, BitOrFunction, BitXorFunction, BoolAndFunction,
    BoolOrFunction, EveryFunction, LogicalAndFunction, LogicalOrFunction,
};
use crate::aggregate::conditional::CountIfFunction;
use crate::aggregate::json_agg::{
    JsonAggFunction, JsonObjectAggFunction, JsonbAggFunction, JsonbObjectAggFunction,
};
use crate::aggregate::statistical::{
    AvgFunction, AvgWeightedFunction, CorrFunction, CountFunction, CovarPopFunction,
    CovarSampFunction, KurtPopFunction, KurtSampFunction, MaxFunction, MedianFunction, MinFunction,
    ModeFunction, RegrInterceptFunction, RegrR2Function, RegrSlopeFunction, SkewPopFunction,
    SkewSampFunction, StddevFunction, StddevPopFunction, StddevSampFunction, SumFunction,
    VarPopFunction, VarSampFunction, VarianceFunction,
};
use crate::aggregate::string_agg::{ListAggFunction, StringAggFunction};
use crate::aggregate::window_functions::{
    CumeDistFunction, DenseRankFunction, NtileFunction, PercentRankFunction, RankFunction,
    RowNumberFunction,
};

pub(super) fn register(registry: &mut FunctionRegistry) {
    let basic_functions: Vec<Rc<dyn AggregateFunction>> = vec![
        Rc::new(CountFunction),
        Rc::new(SumFunction),
        Rc::new(AvgFunction),
        Rc::new(MinFunction),
        Rc::new(MaxFunction),
    ];

    for func in basic_functions {
        registry.register_aggregate(func.name().to_string(), func);
    }

    let statistical_functions: Vec<Rc<dyn AggregateFunction>> = vec![
        Rc::new(StddevPopFunction),
        Rc::new(StddevSampFunction),
        Rc::new(StddevFunction),
        Rc::new(VarPopFunction),
        Rc::new(VarSampFunction),
        Rc::new(VarianceFunction),
        Rc::new(MedianFunction),
        Rc::new(ModeFunction),
        Rc::new(CorrFunction),
        Rc::new(CovarPopFunction),
        Rc::new(CovarSampFunction),
        Rc::new(RegrSlopeFunction),
        Rc::new(RegrInterceptFunction),
        Rc::new(RegrR2Function),
        Rc::new(SkewPopFunction),
        Rc::new(SkewSampFunction),
        Rc::new(KurtPopFunction),
        Rc::new(KurtSampFunction),
        Rc::new(AvgWeightedFunction),
    ];

    for func in statistical_functions {
        registry.register_aggregate(func.name().to_string(), func);
    }

    registry.register_aggregate("STDDEVPOP".to_string(), Rc::new(StddevPopFunction));
    registry.register_aggregate("STDDEVSAMP".to_string(), Rc::new(StddevSampFunction));
    registry.register_aggregate("VARPOP".to_string(), Rc::new(VarPopFunction));
    registry.register_aggregate("VARSAMP".to_string(), Rc::new(VarSampFunction));
    registry.register_aggregate("COVARPOP".to_string(), Rc::new(CovarPopFunction));
    registry.register_aggregate("COVARSAMP".to_string(), Rc::new(CovarSampFunction));
    registry.register_aggregate("SKEWPOP".to_string(), Rc::new(SkewPopFunction));
    registry.register_aggregate("SKEWSAMP".to_string(), Rc::new(SkewSampFunction));
    registry.register_aggregate("KURTPOP".to_string(), Rc::new(KurtPopFunction));
    registry.register_aggregate("KURTSAMP".to_string(), Rc::new(KurtSampFunction));
    registry.register_aggregate("AVGWEIGHTED".to_string(), Rc::new(AvgWeightedFunction));

    registry.register_aggregate("LISTAGG".to_string(), Rc::new(ListAggFunction::new()));
    registry.register_aggregate(
        "STRING_AGG".to_string(),
        Rc::new(StringAggFunction::default()),
    );

    registry.register_aggregate(
        "ARRAY_AGG".to_string(),
        Rc::new(ArrayAggFunction::new(false)),
    );

    registry.register_aggregate("COUNTIF".to_string(), Rc::new(CountIfFunction));

    let boolean_functions: Vec<Rc<dyn AggregateFunction>> = vec![
        Rc::new(BoolAndFunction),
        Rc::new(BoolOrFunction),
        Rc::new(EveryFunction),
    ];

    for func in boolean_functions {
        registry.register_aggregate(func.name().to_string(), func);
    }

    let bitwise_functions: Vec<Rc<dyn AggregateFunction>> = vec![
        Rc::new(BitAndFunction),
        Rc::new(BitOrFunction),
        Rc::new(BitXorFunction),
    ];

    for func in bitwise_functions {
        registry.register_aggregate(func.name().to_string(), func);
    }

    let json_functions: Vec<Rc<dyn AggregateFunction>> = vec![
        Rc::new(JsonAggFunction),
        Rc::new(JsonbAggFunction),
        Rc::new(JsonObjectAggFunction),
        Rc::new(JsonbObjectAggFunction),
    ];

    for func in json_functions {
        registry.register_aggregate(func.name().to_string(), func);
    }

    let bigquery_functions: Vec<Rc<dyn AggregateFunction>> = vec![
        Rc::new(LogicalAndFunction),
        Rc::new(LogicalOrFunction),
        Rc::new(AnyValueFunction),
        Rc::new(ArrayConcatAggFunction),
    ];

    for func in bigquery_functions {
        registry.register_aggregate(func.name().to_string(), func);
    }

    let approximate_functions: Vec<Rc<dyn AggregateFunction>> = vec![
        Rc::new(ApproxCountDistinctFunction),
        Rc::new(ApproxQuantilesFunction::default()),
        Rc::new(ApproxTopCountFunction::default()),
        Rc::new(ApproxTopSumFunction::default()),
    ];

    for func in approximate_functions {
        registry.register_aggregate(func.name().to_string(), func);
    }

    let bigquery_functions: Vec<Rc<dyn AggregateFunction>> = vec![
        Rc::new(HllCountInitFunction::default()),
        Rc::new(HllCountMergeFunction),
        Rc::new(HllCountExtractFunction),
        Rc::new(BqCorrFunction),
        Rc::new(BqStddevFunction),
        Rc::new(BqVarianceFunction),
        Rc::new(BqAnyValueFunction),
        Rc::new(ArrayAggDistinctFunction::default()),
        Rc::new(BqArrayConcatAggFunction),
        Rc::new(StringAggDistinctFunction::default()),
    ];

    for func in bigquery_functions {
        registry.register_aggregate(func.name().to_string(), func);
    }

    registry.register_aggregate(
        "HLL_COUNT.INIT".to_string(),
        Rc::new(HllCountInitFunction::default()),
    );
    registry.register_aggregate(
        "HLL_COUNT.MERGE".to_string(),
        Rc::new(HllCountMergeFunction),
    );
    registry.register_aggregate(
        "HLL_COUNT.MERGE_PARTIAL".to_string(),
        Rc::new(HllCountMergeFunction),
    );
    registry.register_aggregate(
        "HLL_COUNT_MERGE_PARTIAL".to_string(),
        Rc::new(HllCountMergeFunction),
    );
    registry.register_aggregate(
        "HLL_COUNT.EXTRACT".to_string(),
        Rc::new(HllCountExtractFunction),
    );

    let window_functions: Vec<Rc<dyn AggregateFunction>> = vec![
        Rc::new(RowNumberFunction),
        Rc::new(RankFunction),
        Rc::new(DenseRankFunction),
        Rc::new(NtileFunction::default()),
        Rc::new(PercentRankFunction),
        Rc::new(CumeDistFunction),
    ];

    for func in window_functions {
        registry.register_aggregate(func.name().to_string(), func);
    }
}

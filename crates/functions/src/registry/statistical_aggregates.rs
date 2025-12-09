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
use crate::aggregate::clickhouse::{
    AnyFunction, AnyHeavyFunction, AnyLastFunction, ArgMaxFunction, ArgMinFunction,
    ArrayFilterFunction, ArrayFlattenFunction, ArrayMapFunction, ArrayReduceFunction,
    AvgArrayFunction, AvgIfFunction, BitmapAndCardinalityFunction, BitmapCardinalityFunction,
    BitmapOrCardinalityFunction, BoundingRatioFunction, CategoricalInformationValueFunction,
    ContingencyFunction, CountEqualFunction, CramersVFunction, DeltaSumFunction,
    DeltaSumTimestampFunction, EntropyFunction, ExponentialMovingAverageFunction,
    GroupArrayFunction, GroupArrayInsertAtFunction, GroupArrayMovingAvgFunction,
    GroupArrayMovingSumFunction, GroupArraySampleFunction, GroupBitAndFunction, GroupBitOrFunction,
    GroupBitXorFunction, GroupBitmapAndFunction, GroupBitmapFunction, GroupBitmapOrFunction,
    GroupBitmapXorFunction, GroupConcatFunction, GroupUniqArrayFunction, IntervalLengthSumFunction,
    MannWhitneyUTestFunction, MaxArrayFunction, MaxIfFunction, MaxMapFunction, MinArrayFunction,
    MinIfFunction, MinMapFunction, QuantileBFloat16Function, QuantileDeterministicFunction,
    QuantileExactFunction, QuantileExactWeightedFunction, QuantileFunction,
    QuantileTDigestFunction, QuantileTDigestWeightedFunction, QuantileTimingFunction,
    QuantileTimingWeightedFunction, QuantilesExactFunction, QuantilesFunction,
    QuantilesTDigestFunction, QuantilesTimingFunction, RankCorrFunction, RetentionFunction,
    SequenceCountFunction, SequenceMatchFunction, SimpleLinearRegressionFunction,
    StudentTTestFunction, SumArrayFunction, SumIfFunction, SumMapFunction, SumWithOverflowFunction,
    TheilUFunction, TopKFunction, TopKWeightedFunction, UniqCombined64Function,
    UniqCombinedFunction, UniqExactFunction, UniqFunction, UniqHll12Function,
    UniqThetaSketchFunction, UniqUpToFunction, WelchTTestFunction, WindowFunnelFunction,
};
use crate::aggregate::conditional::CountIfFunction;
use crate::aggregate::json_agg::{
    JsonAggFunction, JsonObjectAggFunction, JsonbAggFunction, JsonbObjectAggFunction,
};
use crate::aggregate::postgresql::{
    FirstValueFunction, LagFunction, LastValueFunction, LeadFunction,
    ModeFunction as PgModeFunction, NthValueFunction, PercentileContFunction,
    PercentileDiscFunction, RegrAvgXFunction, RegrAvgYFunction, RegrCountFunction, RegrSxxFunction,
    RegrSxyFunction, RegrSyyFunction,
};
use crate::aggregate::statistical::{
    AvgFunction, CorrFunction, CountFunction, CovarPopFunction, CovarSampFunction, MaxFunction,
    MedianFunction, MinFunction, ModeFunction, RegrInterceptFunction, RegrR2Function,
    RegrSlopeFunction, StddevFunction, StddevPopFunction, StddevSampFunction, SumFunction,
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

    let clickhouse_functions: Vec<Rc<dyn AggregateFunction>> = vec![
        Rc::new(UniqFunction),
        Rc::new(UniqExactFunction),
        Rc::new(UniqHll12Function),
        Rc::new(UniqCombinedFunction),
        Rc::new(UniqCombined64Function),
        Rc::new(UniqThetaSketchFunction),
        Rc::new(UniqUpToFunction::default()),
        Rc::new(TopKFunction::default()),
        Rc::new(TopKWeightedFunction::default()),
        Rc::new(QuantileFunction::default()),
        Rc::new(QuantileExactFunction::default()),
        Rc::new(QuantileExactWeightedFunction::default()),
        Rc::new(QuantileTimingFunction::default()),
        Rc::new(QuantileTimingWeightedFunction::default()),
        Rc::new(QuantileTDigestFunction::default()),
        Rc::new(QuantileTDigestWeightedFunction::default()),
        Rc::new(QuantilesFunction::default()),
        Rc::new(QuantilesExactFunction::default()),
        Rc::new(QuantilesTimingFunction::default()),
        Rc::new(QuantilesTDigestFunction::default()),
        Rc::new(ArgMinFunction),
        Rc::new(ArgMaxFunction),
        Rc::new(GroupArrayFunction),
        Rc::new(GroupUniqArrayFunction),
        Rc::new(GroupArrayInsertAtFunction),
        Rc::new(GroupArrayMovingAvgFunction::default()),
        Rc::new(GroupArrayMovingSumFunction::default()),
        Rc::new(GroupArraySampleFunction::default()),
        Rc::new(GroupBitAndFunction),
        Rc::new(GroupBitOrFunction),
        Rc::new(GroupBitXorFunction),
        Rc::new(GroupBitmapFunction),
        Rc::new(GroupBitmapAndFunction),
        Rc::new(GroupBitmapOrFunction),
        Rc::new(GroupBitmapXorFunction),
        Rc::new(SumMapFunction),
        Rc::new(MinMapFunction),
        Rc::new(MaxMapFunction),
        Rc::new(SumWithOverflowFunction),
        Rc::new(RankCorrFunction),
        Rc::new(ExponentialMovingAverageFunction::default()),
        Rc::new(IntervalLengthSumFunction),
        Rc::new(RetentionFunction),
        Rc::new(WindowFunnelFunction::default()),
        Rc::new(SequenceMatchFunction::default()),
        Rc::new(SequenceCountFunction::default()),
        Rc::new(SumIfFunction),
        Rc::new(AvgIfFunction),
        Rc::new(MinIfFunction),
        Rc::new(MaxIfFunction),
        Rc::new(CountEqualFunction::default()),
        Rc::new(BoundingRatioFunction),
        Rc::new(SimpleLinearRegressionFunction),
        Rc::new(ContingencyFunction),
        Rc::new(CramersVFunction),
        Rc::new(GroupConcatFunction::default()),
        Rc::new(EntropyFunction),
        Rc::new(TheilUFunction),
        Rc::new(CategoricalInformationValueFunction),
        Rc::new(DeltaSumFunction),
        Rc::new(DeltaSumTimestampFunction),
        Rc::new(ArrayFlattenFunction),
        Rc::new(ArrayReduceFunction::default()),
        Rc::new(ArrayMapFunction::default()),
        Rc::new(ArrayFilterFunction::default()),
        Rc::new(SumArrayFunction),
        Rc::new(AvgArrayFunction),
        Rc::new(MinArrayFunction),
        Rc::new(MaxArrayFunction),
        Rc::new(BitmapCardinalityFunction),
        Rc::new(BitmapAndCardinalityFunction),
        Rc::new(BitmapOrCardinalityFunction),
        Rc::new(QuantileDeterministicFunction::default()),
        Rc::new(QuantileBFloat16Function::default()),
        Rc::new(MannWhitneyUTestFunction),
        Rc::new(StudentTTestFunction),
        Rc::new(WelchTTestFunction),
        Rc::new(AnyFunction),
        Rc::new(AnyLastFunction),
        Rc::new(AnyHeavyFunction),
    ];

    for func in clickhouse_functions {
        registry.register_aggregate(func.name().to_string(), func);
    }

    let postgresql_functions: Vec<Rc<dyn AggregateFunction>> = vec![
        Rc::new(RegrAvgXFunction),
        Rc::new(RegrAvgYFunction),
        Rc::new(RegrCountFunction),
        Rc::new(RegrSxxFunction),
        Rc::new(RegrSyyFunction),
        Rc::new(RegrSxyFunction),
        Rc::new(PercentileContFunction::default()),
        Rc::new(PercentileDiscFunction::default()),
        Rc::new(FirstValueFunction),
        Rc::new(LastValueFunction),
        Rc::new(NthValueFunction::default()),
        Rc::new(LagFunction::default()),
        Rc::new(LeadFunction::default()),
        Rc::new(PgModeFunction),
    ];

    for func in postgresql_functions {
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

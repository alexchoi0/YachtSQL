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
    AvgArrayFunction, AvgDistinctFunction, AvgIfFunction, AvgMapFunction, AvgMergeFunction,
    AvgStateFunction, BitmapAndCardinalityFunction, BitmapCardinalityFunction,
    BitmapOrCardinalityFunction, BoundingRatioFunction, CategoricalInformationValueFunction,
    ContingencyFunction, CountEqualFunction, CountMergeFunction, CountStateFunction,
    CramersVFunction, DeltaSumFunction, DeltaSumTimestampFunction, EntropyFunction,
    ExponentialMovingAverageFunction, GroupArrayFunction, GroupArrayInsertAtFunction,
    GroupArrayIntersectFunction, GroupArrayLastFunction, GroupArrayMovingAvgFunction,
    GroupArrayMovingSumFunction, GroupArraySampleFunction, GroupArraySortedFunction,
    GroupBitAndFunction, GroupBitOrFunction, GroupBitXorFunction, GroupBitmapAndFunction,
    GroupBitmapFunction, GroupBitmapOrFunction, GroupBitmapXorFunction, GroupConcatFunction,
    GroupUniqArrayFunction, HistogramFunction, IntervalLengthSumFunction, MannWhitneyUTestFunction,
    MaxArrayFunction, MaxIfFunction, MaxMapFunction, MaxMergeFunction, MaxStateFunction,
    MinArrayFunction, MinIfFunction, MinMapFunction, MinMergeFunction, MinStateFunction,
    QuantileBFloat16Function, QuantileBFloat16WeightedFunction, QuantileDDFunction,
    QuantileDeterministicFunction, QuantileExactFunction, QuantileExactHighFunction,
    QuantileExactLowFunction, QuantileExactWeightedFunction, QuantileFunction, QuantileGKFunction,
    QuantileInterpolatedWeightedFunction, QuantileTDigestFunction, QuantileTDigestWeightedFunction,
    QuantileTimingFunction, QuantileTimingWeightedFunction, QuantilesExactFunction,
    QuantilesFunction, QuantilesTDigestFunction, QuantilesTimingFunction, RankCorrFunction,
    RetentionFunction, SequenceCountFunction, SequenceMatchFunction,
    SimpleLinearRegressionFunction, StudentTTestFunction, SumArrayFunction, SumDistinctFunction,
    SumIfFunction, SumMapFunction, SumMergeFunction, SumStateFunction, SumWithOverflowFunction,
    TheilUFunction, TopKFunction, TopKWeightedFunction, UniqCombined64Function,
    UniqCombinedFunction, UniqExactFunction, UniqFunction, UniqHll12Function, UniqMergeFunction,
    UniqStateFunction, UniqThetaSketchFunction, UniqUpToFunction, WelchTTestFunction,
    WindowFunnelFunction,
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

    registry.register_aggregate(
        "GROUPARRAYSAMPLE".to_string(),
        Rc::new(GroupArraySampleFunction::default()),
    );
    registry.register_aggregate(
        "GROUPARRAYSORTED".to_string(),
        Rc::new(GroupArraySortedFunction::default()),
    );
    registry.register_aggregate(
        "GROUPARRAYINSERTAT".to_string(),
        Rc::new(GroupArrayInsertAtFunction),
    );
    registry.register_aggregate(
        "GROUPARRAYMOVINGAVG".to_string(),
        Rc::new(GroupArrayMovingAvgFunction::default()),
    );
    registry.register_aggregate(
        "GROUPARRAYMOVINGSUM".to_string(),
        Rc::new(GroupArrayMovingSumFunction::default()),
    );
    registry.register_aggregate(
        "GROUPUNIQARRAY".to_string(),
        Rc::new(GroupUniqArrayFunction),
    );
    registry.register_aggregate(
        "GROUPARRAYLAST".to_string(),
        Rc::new(GroupArrayLastFunction::default()),
    );
    registry.register_aggregate("GROUPBITAND".to_string(), Rc::new(GroupBitAndFunction));
    registry.register_aggregate("GROUPBITOR".to_string(), Rc::new(GroupBitOrFunction));
    registry.register_aggregate("GROUPBITXOR".to_string(), Rc::new(GroupBitXorFunction));
    registry.register_aggregate("SUMMAP".to_string(), Rc::new(SumMapFunction));
    registry.register_aggregate("MINMAP".to_string(), Rc::new(MinMapFunction));
    registry.register_aggregate("MAXMAP".to_string(), Rc::new(MaxMapFunction));
    registry.register_aggregate("AVGMAP".to_string(), Rc::new(AvgMapFunction));
    registry.register_aggregate(
        "GROUPARRAYINTERSECT".to_string(),
        Rc::new(GroupArrayIntersectFunction),
    );
    registry.register_aggregate(
        "GROUPCONCAT".to_string(),
        Rc::new(GroupConcatFunction::default()),
    );
    registry.register_aggregate(
        "QUANTILEEXACTLOW".to_string(),
        Rc::new(QuantileExactLowFunction::default()),
    );
    registry.register_aggregate(
        "QUANTILEEXACTHIGH".to_string(),
        Rc::new(QuantileExactHighFunction::default()),
    );
    registry.register_aggregate(
        "QUANTILEDD".to_string(),
        Rc::new(QuantileDDFunction::default()),
    );
    registry.register_aggregate(
        "QUANTILEGK".to_string(),
        Rc::new(QuantileGKFunction::default()),
    );
    registry.register_aggregate(
        "QUANTILEINTERPOLATEDWEIGHTED".to_string(),
        Rc::new(QuantileInterpolatedWeightedFunction::default()),
    );
    registry.register_aggregate(
        "QUANTILEBFLOAT16WEIGHTED".to_string(),
        Rc::new(QuantileBFloat16WeightedFunction::default()),
    );
    registry.register_aggregate(
        "QUANTILEEXACTWEIGHTED".to_string(),
        Rc::new(QuantileExactWeightedFunction::default()),
    );
    registry.register_aggregate(
        "QUANTILETIMINGWEIGHTED".to_string(),
        Rc::new(QuantileTimingWeightedFunction::default()),
    );
    registry.register_aggregate(
        "QUANTILETDIGESTWEIGHTED".to_string(),
        Rc::new(QuantileTDigestWeightedFunction::default()),
    );
    registry.register_aggregate(
        "QUANTILEDETERMINISTIC".to_string(),
        Rc::new(QuantileDeterministicFunction::default()),
    );
    registry.register_aggregate(
        "QUANTILEBFLOAT16".to_string(),
        Rc::new(QuantileBFloat16Function::default()),
    );
    registry.register_aggregate("UNIQUPTO".to_string(), Rc::new(UniqUpToFunction::default()));
    registry.register_aggregate(
        "TOPKWEIGHTED".to_string(),
        Rc::new(TopKWeightedFunction::default()),
    );
    registry.register_aggregate("UNIQTHETA".to_string(), Rc::new(UniqThetaSketchFunction));
    registry.register_aggregate(
        "HISTOGRAM".to_string(),
        Rc::new(HistogramFunction::default()),
    );
    registry.register_aggregate("SUMDISTINCT".to_string(), Rc::new(SumDistinctFunction));
    registry.register_aggregate("AVGDISTINCT".to_string(), Rc::new(AvgDistinctFunction));

    registry.register_aggregate("SUMIF".to_string(), Rc::new(SumIfFunction));
    registry.register_aggregate("AVGIF".to_string(), Rc::new(AvgIfFunction));
    registry.register_aggregate("MINIF".to_string(), Rc::new(MinIfFunction));
    registry.register_aggregate("MAXIF".to_string(), Rc::new(MaxIfFunction));
    registry.register_aggregate(
        "DELTASUMTIMESTAMP".to_string(),
        Rc::new(DeltaSumTimestampFunction),
    );
    registry.register_aggregate("DELTASUM".to_string(), Rc::new(DeltaSumFunction));
    registry.register_aggregate(
        "INTERVALLENGTHSUM".to_string(),
        Rc::new(IntervalLengthSumFunction),
    );
    registry.register_aggregate(
        "WINDOWFUNNEL".to_string(),
        Rc::new(WindowFunnelFunction::default()),
    );
    registry.register_aggregate(
        "SEQUENCEMATCH".to_string(),
        Rc::new(SequenceMatchFunction::default()),
    );
    registry.register_aggregate(
        "SEQUENCECOUNT".to_string(),
        Rc::new(SequenceCountFunction::default()),
    );
    registry.register_aggregate("ARGMIN".to_string(), Rc::new(ArgMinFunction));
    registry.register_aggregate("ARGMAX".to_string(), Rc::new(ArgMaxFunction));
    registry.register_aggregate("RANKCORR".to_string(), Rc::new(RankCorrFunction));
    registry.register_aggregate("ANYHEAVY".to_string(), Rc::new(AnyHeavyFunction));
    registry.register_aggregate("ANYLAST".to_string(), Rc::new(AnyLastFunction));
    registry.register_aggregate("RETENTION".to_string(), Rc::new(RetentionFunction));
    registry.register_aggregate(
        "EXPONENTIALMOVINGAVERAGE".to_string(),
        Rc::new(ExponentialMovingAverageFunction::default()),
    );
    registry.register_aggregate(
        "SIMPLELINEARREGRESSION".to_string(),
        Rc::new(SimpleLinearRegressionFunction),
    );
    registry.register_aggregate(
        "MANNWHITNEYUTEST".to_string(),
        Rc::new(MannWhitneyUTestFunction),
    );
    registry.register_aggregate("STUDENTTTEST".to_string(), Rc::new(StudentTTestFunction));
    registry.register_aggregate("WELCHTTEST".to_string(), Rc::new(WelchTTestFunction));
    registry.register_aggregate("CRAMERSV".to_string(), Rc::new(CramersVFunction));
    registry.register_aggregate("CONTINGENCY".to_string(), Rc::new(ContingencyFunction));
    registry.register_aggregate("THEILSU".to_string(), Rc::new(TheilUFunction));
    registry.register_aggregate(
        "CATEGORICALINFORMATIONVALUE".to_string(),
        Rc::new(CategoricalInformationValueFunction),
    );
    registry.register_aggregate(
        "SUMWITHOVERFLOW".to_string(),
        Rc::new(SumWithOverflowFunction),
    );
    registry.register_aggregate("SUMARRAY".to_string(), Rc::new(SumArrayFunction));
    registry.register_aggregate("AVGARRAY".to_string(), Rc::new(AvgArrayFunction));
    registry.register_aggregate("MINARRAY".to_string(), Rc::new(MinArrayFunction));
    registry.register_aggregate("MAXARRAY".to_string(), Rc::new(MaxArrayFunction));
    registry.register_aggregate("UNIQHLL12".to_string(), Rc::new(UniqHll12Function));
    registry.register_aggregate("UNIQCOMBINED".to_string(), Rc::new(UniqCombinedFunction));
    registry.register_aggregate(
        "UNIQCOMBINED64".to_string(),
        Rc::new(UniqCombined64Function),
    );
    registry.register_aggregate("SUMSTATE".to_string(), Rc::new(SumStateFunction));
    registry.register_aggregate("SUMMERGE".to_string(), Rc::new(SumMergeFunction));
    registry.register_aggregate("AVGSTATE".to_string(), Rc::new(AvgStateFunction));
    registry.register_aggregate("AVGMERGE".to_string(), Rc::new(AvgMergeFunction));
    registry.register_aggregate("COUNTSTATE".to_string(), Rc::new(CountStateFunction));
    registry.register_aggregate("COUNTMERGE".to_string(), Rc::new(CountMergeFunction));
    registry.register_aggregate("MINSTATE".to_string(), Rc::new(MinStateFunction));
    registry.register_aggregate("MINMERGE".to_string(), Rc::new(MinMergeFunction));
    registry.register_aggregate("MAXSTATE".to_string(), Rc::new(MaxStateFunction));
    registry.register_aggregate("MAXMERGE".to_string(), Rc::new(MaxMergeFunction));
    registry.register_aggregate("UNIQSTATE".to_string(), Rc::new(UniqStateFunction));
    registry.register_aggregate("UNIQMERGE".to_string(), Rc::new(UniqMergeFunction));
}

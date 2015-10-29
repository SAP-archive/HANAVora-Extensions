package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.analysis.EliminateSubQueries

object ExtendedOptimizer extends Optimizer {
  private val MAX_ITERATIONS = 100

  override val batches =
  // SubQueries are only needed for analysis and can be removed before execution.
    Batch("Remove SubQueries", FixedPoint(MAX_ITERATIONS),
      EliminateSubQueries) ::
      Batch("Operator Reordering", FixedPoint(MAX_ITERATIONS),
        UnionPushdown,
        CombineFilters,
        PushPredicateThroughProject,
        PushPredicateThroughJoin,
        PushPredicateThroughGenerate,
        ColumnPruning,
        ProjectCollapsing,
        CombineLimits,
        ExtraBooleanSimplification
      ) ::
      Batch("ConstantFolding", FixedPoint(MAX_ITERATIONS),
        NullPropagation,
        OptimizeIn,
        ConstantFolding,
        LikeSimplification,
        BooleanSimplification,
        SimplifyFilters,
        SimplifyCasts,
        SimplifyCaseConversionExpressions) ::
      Batch("Decimal Optimizations", FixedPoint(MAX_ITERATIONS),
        DecimalAggregates) ::
      Batch("LocalRelation", FixedPoint(MAX_ITERATIONS),
        ConvertToLocalRelation) :: Nil
}

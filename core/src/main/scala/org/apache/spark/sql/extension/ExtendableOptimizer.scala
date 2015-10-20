package org.apache.spark.sql.extension

import org.apache.spark.sql.catalyst.optimizer.{DefaultOptimizer, Optimizer}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * [[Optimizer]] that can be extended with more rules.
 *
 * @param extendedRules
 */
private[extension] class ExtendableOptimizer(extendedRules: Seq[Rule[LogicalPlan]])
  extends Optimizer {

  private val extendedOptimizerRules = extendedRules
  private val MAX_ITERATIONS = 100

  /* TODO: This might be gone in Spark 1.6+
   * See: https://issues.apache.org/jira/browse/SPARK-7727
   */
  // scalastyle:off structural.type
  private def transformBatchType(b: DefaultOptimizer.Batch): Batch = {
    val strategy = b.strategy.maxIterations match {
      case 1 => Once
      case n => FixedPoint(n)
    }
    Batch(b.name, strategy, b.rules: _*)
  }

  // scalastyle:on structural.type

  private val baseBatches = DefaultOptimizer.batches.map(transformBatchType)

  override protected val batches: Seq[Batch] = if (extendedOptimizerRules.isEmpty) {
    baseBatches
  } else {
    baseBatches :+
      Batch("Extended Optimization Rules", FixedPoint(MAX_ITERATIONS), extendedOptimizerRules: _*)
  }

}

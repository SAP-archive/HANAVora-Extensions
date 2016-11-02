package org.apache.spark.sql.extension

import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule


/**
  * A factory for extended [[Optimizer]] objects.
  *
  * This object serves as a compatibility layer between spark versions 1.6.1 and 1.6.2 by choosing
  * the appropriate [[Optimizer]] class based on [[org.apache.spark.SPARK_VERSION]].
  *
  */
private[sql] object OptimizerFactory {
  case class ExtendableOptimizerBatch(name: String, iterations: Int, rules: Seq[Rule[LogicalPlan]])

  /**
    * Produce an [[Optimizer]] with optional additional rules.
    *
    * @param earlyBatches [[Optimizer.Batch]]es to be prepended to the optimizer
    * @param mainBatchRules rules to enforce in the main optimizer batch (e.g. constant folding)
    * @param postBatches [[Optimizer.Batch]]es to be appended to the optimizer
    * @return
    */
  def produce(earlyBatches: Seq[ExtendableOptimizerBatch] = Nil,
              mainBatchRules: Seq[Rule[LogicalPlan]] = Nil,
              postBatches: Seq[ExtendableOptimizerBatch] = Nil): Optimizer = {
    if (org.apache.spark.SPARK_VERSION.contains("1.6.2")) {
      new ExtendableOptimizer162(earlyBatches, mainBatchRules, postBatches)
    } else {
      new ExtendableOptimizer161(earlyBatches, mainBatchRules, postBatches)
    }
  }
}

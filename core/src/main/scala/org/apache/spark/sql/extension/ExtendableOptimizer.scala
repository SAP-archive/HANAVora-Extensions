package org.apache.spark.sql.extension

import org.apache.spark.sql.catalyst.optimizer.{DefaultOptimizer, Optimizer}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/**
  * [[Optimizer]] that can be extended with more rules.
  *
  * @param earlyBatches Batches to be prepended to the optimizer.
  * @param mainBatchRules Rules to include in the main optimizer batch (e.g. constant folding)
  */
private[sql] class ExtendableOptimizer(earlyBatches: Seq[ExtendableOptimizerBatch] = Nil,
                                       mainBatchRules: Seq[Rule[LogicalPlan]] = Nil)
  extends Optimizer {

  private val baseBatches = DefaultOptimizer.batches.map(transformBatchType)

  private val preMainBatches = earlyBatches.map {
    b => b.iterations match {
      case 1 => Batch(b.name, Once, b.rules: _*)
      case _ => Batch(b.name, FixedPoint(b.iterations), b.rules: _*)
    }
  }

  private val mainOptimizationsBatchName = org.apache.spark.SPARK_VERSION match {
    case v if v startsWith "1.4." => "ConstantFolding"
    case v => sys.error(s"Unsupported Spark version: $v")
  }

  override protected val batches: Seq[Batch] =
    baseBatches match {
      case removeSubQueriesBatch :: otherBatches =>
        removeSubQueriesBatch ::
          preMainBatches.toList ++
            appendToBatch(mainOptimizationsBatchName, otherBatches, mainBatchRules)
      case otherBatches => sys.error("Impossible to add the extended optimizer rules")
    }

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

  private def appendToBatch(
      name: String,
      batches: Seq[Batch],
      rulesToAppend: Seq[Rule[LogicalPlan]]): Seq[Batch] = {
    if (!batches.exists(_.name == name)) {
      sys.error(s"Could not find $name batch")
    }
    batches.map({
      case Batch(`name`, strategy, rules @ _*) =>
        Batch(name, strategy, rules ++ rulesToAppend: _*)
      case batch =>
        batch
    })
  }

}

private[sql] case class ExtendableOptimizerBatch(name: String,
                                                 iterations: Int,
                                                 rules: Seq[Rule[LogicalPlan]])

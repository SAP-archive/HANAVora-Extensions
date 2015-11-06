package org.apache.spark.sql.extension

import org.apache.spark.sql.catalyst.optimizer.{DefaultOptimizer, Optimizer}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * [[Optimizer]] that can be extended with more rules.
 *
 * @param earlyRules Extended rules to be executed early.
 * @param lateRules Extended rules to be executed late.
 */
private[sql] class ExtendableOptimizer(
    earlyRules: Seq[Rule[LogicalPlan]] = Nil,
    lateRules: Seq[Rule[LogicalPlan]] = Nil)
  extends Optimizer {

  private val MAX_ITERATIONS = 100

  private val baseBatches = DefaultOptimizer.batches.map(transformBatchType)

  private val earlyBatch = optionalBatch("Early extended optimizations", earlyRules)

  private val constantFoldingBatchName = org.apache.spark.SPARK_VERSION match {
    case v if v startsWith "1.4." => "ConstantFolding"
    case v => sys.error(s"Unsupported Spark version: $v")
  }

  override protected val batches: Seq[Batch] =
    baseBatches match {
      case removeSubQueriesBatch :: otherBatches =>
        removeSubQueriesBatch ::
          earlyBatch.toList ++
            appendToBatch(constantFoldingBatchName, otherBatches, lateRules)
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

  private def optionalBatch(name: String, rules: Seq[Rule[LogicalPlan]]): Option[Batch] =
    rules match {
      case Nil => None
      case _ =>
        Some(Batch(name, FixedPoint(MAX_ITERATIONS), rules: _*))
    }

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

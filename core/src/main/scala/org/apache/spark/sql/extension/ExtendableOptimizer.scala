package org.apache.spark.sql.extension

import org.apache.spark.sql.catalyst.optimizer.{DefaultOptimizer, Optimizer}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/**
  * [[Optimizer]] that can be extended with more rules.
  *
  * @param earlyBatches Batches to be prepended to the optimizer.
  * @param mainBatchRules Rules to include in the main optimizer batch (e.g. constant folding).
  */
private[sql] class ExtendableOptimizer(earlyBatches: Seq[ExtendableOptimizerBatch] = Nil,
                                       mainBatchRules: Seq[Rule[LogicalPlan]] = Nil)
  extends Optimizer {

  /** Batches from [[DefaultOptimizer]] (Spark defaults). */
  private val baseBatches = DefaultOptimizer.batches.map(defaultOptimizerBatchToInternalBatch)

  /** Batches to be preprended based on [[earlyBatches]]. */
  private val preMainBatches = earlyBatches map extendedBatchToInternalBatch

  /** Name of the batch to be considered the main one. This varies with the Spark version. */
  private val mainOptimizationsBatchName = org.apache.spark.SPARK_VERSION match {
    case v if v startsWith "1.4." => "ConstantFolding"
    case v => sys.error(s"Unsupported Spark version: $v")
  }

  /** These are the final batches to be used by this optimizer. */
  override protected val batches: Seq[Batch] =
    baseBatches match {
      case removeSubQueriesBatch :: otherBatches =>
        removeSubQueriesBatch ::
          preMainBatches.toList ++
            appendToBatch(mainOptimizationsBatchName, otherBatches, mainBatchRules)
      case otherBatches =>
        sys.error("Impossible to add the extended optimizer rules")
    }

  /**
    * Appends rules to a batch.
    *
    * @param name Name of the batch to append to.
    * @param batches Sequence of batches.
    * @param rulesToAppend Sequence of rules to be appended.
    * @return A copy of the input batches with rules to appended to the given batch.
    */
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

  //
  // SPARK-7727: Batch is defined as a structural type, and is a different type for
  //             every Optimizer instance. So we cannot use them interchangeably and
  //             we need to use the following functions to be able to transform different
  //             kinds of Batch to the internal Batch type for ExtendableOptimizer.
  //

  /** Transforms an [[ExtendableOptimizerBatch]] to [[Batch]]. See SPARK-7727. */
  private def extendedBatchToInternalBatch(b: ExtendableOptimizerBatch): this.Batch =
    b.iterations match {
      case 1 => Batch(b.name, Once, b.rules: _*)
      case _ => Batch(b.name, FixedPoint(b.iterations), b.rules: _*)
    }

  /** Transforms a [[DefaultOptimizer.Batch]] into a [[Batch]]. See SPARK-7727. */
  // scalastyle:off structural.type
  private def defaultOptimizerBatchToInternalBatch(b: DefaultOptimizer.Batch): Batch = {
    val strategy = b.strategy.maxIterations match {
      case 1 => Once
      case n => FixedPoint(n)
    }
    Batch(b.name, strategy, b.rules: _*)
  }
  // scalastyle:on structural.type



}

/** Represents an [[Optimizer#Batch]]. See See SPARK-7727. */
private[sql] case class ExtendableOptimizerBatch(name: String,
                                                 iterations: Int,
                                                 rules: Seq[Rule[LogicalPlan]])

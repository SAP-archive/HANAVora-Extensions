package org.apache.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.analysis.VelocityCheckAnalysis
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.execution.{AddDefaultExchange, SparkPlan}
import org.apache.spark.sql.sources.PushDownAggregatesStrategy

/**
 * This context provides extended SQLContext functionality such as hierarchies, enhaced data
 * sources API with support for aggregates pushdown, etc.
 */
class VelocitySQLContext(@transient override val sparkContext: SparkContext)
  extends ExtendableSQLContext(sparkContext, Seq(SQLExtensions, HierarchySQLContextExtension)) {
  self =>
  
  /**
   * Prepares a planned SparkPlan for execution by inserting shuffle operations as needed.
   *
   * We override this to solve:
   * https://issues.apache.org/jira/browse/SPARK-6321
   */
  @transient
  override protected[sql] val prepareForExecution = new RuleExecutor[SparkPlan] {
    val batches =
      Batch("Add exchange", Once, new AddDefaultExchange(self)) :: Nil
  }

  override lazy val checkAnalysis = new VelocityCheckAnalysis {
    override val extendedCheckRules = Seq(
      sources.PreWriteCheck(catalog)
    )
  }

}

private[sql] object SQLExtensions extends SQLContextExtension {

  override def strategies(planner: ExtendedPlanner) : Seq[Strategy] =
    Seq(PushDownAggregatesStrategy)

}

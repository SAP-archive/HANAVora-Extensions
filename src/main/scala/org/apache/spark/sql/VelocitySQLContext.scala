package org.apache.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.analysis.VelocityCheckAnalysis
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.execution.{AddDefaultExchange, SparkPlan}
import org.apache.spark.sql.sources.{PushDownFunctionsStrategy, CatalystSourceStrategy, PushDownAggregatesStrategy}

/**
 * This context provides extended [[SQLContext]] functionality such as hierarchies, enhanced data
 * sources API with support for aggregates pushdown, etc.
 */
class VelocitySQLContext(@transient override val sparkContext: SparkContext)
  extends ExtendableSQLContext(sparkContext)
  with WithVeloctyFixes
  with PushDownFunctionsSQLContextExtension
  with PushDownAggregatesSQLContextExtension
  with HierarchiesSQLContextExtension
  with CatalystSourceSQLContextExtension
  with VelocityCommandsSQLContextExtension

/**
 * Convenience trait to include miscelaneous general fixes for [[SQLContext]].
 */
private[sql] trait WithVeloctyFixes extends WithDefaultExchangeFix with WithVelocityCheckAnalysis {
  self: ExtendableSQLContext =>
}

private[sql] trait WithDefaultExchangeFix {
  self: ExtendableSQLContext =>

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
}

private[sql] trait WithVelocityCheckAnalysis {
  self: ExtendableSQLContext =>

  override lazy val checkAnalysis = new VelocityCheckAnalysis {
    override val extendedCheckRules = Seq(
      sources.PreWriteCheck(catalog)
    )
  }

}

private[sql] trait CatalystSourceSQLContextExtension extends PlannerSQLContextExtension {

  override def strategies(planner: ExtendedPlanner): List[Strategy] =
    CatalystSourceStrategy :: super.strategies(planner)

}

private[sql] trait PushDownAggregatesSQLContextExtension extends PlannerSQLContextExtension {

  override def strategies(planner: ExtendedPlanner): List[Strategy] =
    PushDownAggregatesStrategy :: super.strategies(planner)

}

private[sql] trait PushDownFunctionsSQLContextExtension extends PlannerSQLContextExtension {

  override def strategies(planner: ExtendedPlanner): List[Strategy] =
    PushDownFunctionsStrategy :: super.strategies(planner)

}

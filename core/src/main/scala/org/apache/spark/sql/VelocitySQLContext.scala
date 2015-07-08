package org.apache.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.execution.{EnsureRequirements, SparkPlan}
import org.apache.spark.sql.sources.{PushDownFunctionsStrategy, CatalystSourceStrategy, PushDownAggregatesStrategy}

/**
 * This context provides extended [[SQLContext]] functionality such as hierarchies, enhanced data
 * sources API with support for aggregates pushdown, etc.
 */
class VelocitySQLContext(@transient override val sparkContext: SparkContext)
  extends ExtendableSQLContext(sparkContext)
  with PushDownFunctionsSQLContextExtension
  with PushDownAggregatesSQLContextExtension
  with HierarchiesSQLContextExtension
  with CatalystSourceSQLContextExtension
  with VelocityCommandsSQLContextExtension

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

package org.apache.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.sources.{CatalystSourceStrategy, CreatePersistentTableStrategy}

/**
 * This context provides extended [[SQLContext]] functionality such as hierarchies, enhanced data
 * sources API with support for aggregates pushdown, etc.
 */
class SapSQLContext(@transient override val sparkContext: SparkContext)
  extends ExtendableSQLContext(sparkContext)
  with HierarchiesSQLContextExtension
  with CatalystSourceSQLContextExtension
  with SapCommandsSQLContextExtension
  with NonTemporaryTableSQLContextExtension

object SapSQLContext {
  val PROPERTY_IGNORE_USE_STATEMENTS = "spark.vora.ignore_use_statements"
}

private[sql] trait CatalystSourceSQLContextExtension extends PlannerSQLContextExtension {

  override def strategies(planner: ExtendedPlanner): List[Strategy] =
    CatalystSourceStrategy :: super.strategies(planner)

}

private[sql] trait NonTemporaryTableSQLContextExtension extends PlannerSQLContextExtension {
  override def strategies(planner: ExtendedPlanner): List[Strategy] =
    CreatePersistentTableStrategy :: super.strategies(planner)
}

// class VelocitySQLContext is kept for a short time in order to avoid
// build problems with datasource package
@deprecated
class VelocitySQLContext(@transient override val sparkContext: SparkContext)
 extends SapSQLContext(sparkContext){
}

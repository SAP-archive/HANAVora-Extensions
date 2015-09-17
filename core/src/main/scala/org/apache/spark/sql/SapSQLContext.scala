package org.apache.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.sources.{CatalystSourceStrategy, CreatePersistentTableStrategy}
import org.apache.spark.sql.sources._

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
{
  // check if we have to automatically register tables
  val sparkConf = sparkContext.getConf
  sparkConf.getOption(SapSQLContext.PROPERTY_AUTO_REGISTER_TABLES) match {
    case None => // do nothing
    case conf: Some[String] => {
      conf.get.split(",").foreach(ds => {
        logInfo("Auto-Registering tables from Datasource '" + ds + "'")
        SapSQLContext.registerTablesFromDs(ds, this, Map.empty[String,String],
          ignoreConflicts = true)
      })
    }
  }
}

object SapSQLContext {
  val PROPERTY_IGNORE_USE_STATEMENTS = "spark.vora.ignore_use_statements"
  val PROPERTY_AUTO_REGISTER_TABLES = "spark.vora.autoregister"

  private def registerTablesFromDs(provider: String, sqlc: SapSQLContext,
                                   options: Map[String,String], ignoreConflicts: Boolean): Unit = {
    DataFrame(sqlc, new RegisterAllTablesUsing(provider, options, ignoreConflicts))
  }
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

package org.apache.spark.sql

import java.io.{IOException, FileInputStream, InputStream}
import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.sources.{CatalystSourceStrategy, CreatePersistentTableStrategy}
import org.apache.spark.sql.sources._

/**
 * [[SapSQLContext]] is the main entry point for SAP Spark extensions.
 * Users of this class should check Apache Spark SQL official documentation.
 *
 * Extensions are composed through [[ExtendableSQLContext]], which allows
 * to define modular extensions.
 *
 * The following extensions are applied to [[SapSQLContext]]:
 *
 *  - [[CatalystSourceSQLContextExtension]]: Provides a new data source API that
 *    can be used to push arbitrary queries down to the data source.
 *  - [[HierarchiesSQLContextExtension]]: Adds support for a new SQL extension for
 *    hierarchy support.
 *  - [[SapCommandsSQLContextExtension]]: Enables support for commands (e.g. REGISTER TABLE).
 *  - [[NonTemporaryTableSQLContextExtension]]: Adds support for both temporary and
 *    non-temporary tables.
 *
 */
class SapSQLContext(@transient override val sparkContext: SparkContext)
  extends ExtendableSQLContext(sparkContext)
  with HierarchiesSQLContextExtension
  with CatalystSourceSQLContextExtension
  with SapCommandsSQLContextExtension
  with NonTemporaryTableSQLContextExtension
{
  logProjectVersion()
  // check if we have to automatically register tables
  sparkContext.getConf.getOption(SapSQLContext.PROPERTY_AUTO_REGISTER_TABLES) match {
    case None => // do nothing
    case conf: Some[String] => {
      conf.get.split(",").foreach(ds => {
        logInfo("Auto-Registering tables from Datasource '" + ds + "'")
        SapSQLContext.registerTablesFromDs(ds, this, Map.empty[String,String],
          ignoreConflicts = true)
      })
    }
  }

  def logProjectVersion(): Unit = {
    val prop = new Properties()
    var input: InputStream = null
    try {
      input = getClass.getResourceAsStream("/project.properties")
      prop.load(input)
      logInfo(s"SapSQLContext [version: ${prop.getProperty("datasourcedist.version")}] created")
    }
    catch {
      case e: Exception => logDebug("project.properties file does not exist")
    }
    if(input != null) {
      try {
        input.close()
      }
      catch {
        case e: Exception =>
      }
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

/**
 * Provides a new data source API that can be used to push arbitrary queries down to the
 * data source.
 *
 * In order to leverage this API, data sources must implement the [[CatalystSource]] trait.
 *
 * [[CatalystSourceStrategy]] is added to the planner to deal with this kind of data source.
 */
@DeveloperApi
trait CatalystSourceSQLContextExtension extends PlannerSQLContextExtension {

  override def strategies(planner: ExtendedPlanner): List[Strategy] =
    CatalystSourceStrategy :: super.strategies(planner)

}

@DeveloperApi
trait NonTemporaryTableSQLContextExtension extends PlannerSQLContextExtension {
  override def strategies(planner: ExtendedPlanner): List[Strategy] =
    CreatePersistentTableStrategy :: super.strategies(planner)
}

package org.apache.spark.sql

import java.io.InputStream
import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.sql.extension.{ExtendableSQLContext, SapSQLContextExtension}
import org.apache.spark.sql.sources._

/**
 * [[SapSQLContext]] is the main entry point for SAP Spark extensions.
 * Users of this class should check Apache Spark SQL official documentation.
 *
 * This context provides:
 *
 *  - A new data source API that can be used to push arbitrary queries down to the data source.
 *    See [[CatalystSource]].
 *  - Support for a new SQL extension for hierarchy queries.
 *  - New DDL commands (e.g. REGISTER TABLE).
 *  - Support for both temporary and non-temporary tables.
 */
class SapSQLContext(@transient override val sparkContext: SparkContext)
  extends ExtendableSQLContext(sparkContext)
  with SapSQLContextExtension {

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

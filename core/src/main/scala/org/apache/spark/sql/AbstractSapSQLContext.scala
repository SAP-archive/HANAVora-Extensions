package org.apache.spark.sql

import java.io.InputStream
import java.util.Properties

import org.apache.spark.sql.extension._
import org.apache.spark.sql.sources.commands.RegisterAllTablesUsing

private[sql] trait AbstractSapSQLContext
  extends SapSQLContextExtension {
  self: SQLContext =>

  logProjectVersion()
  // check if we have to automatically register tables
  sparkContext.getConf.getOption(AbstractSapSQLContext.PROPERTY_AUTO_REGISTER_TABLES) match {
    case None => // do nothing
    case Some(conf) =>
      conf.split(",").foreach(ds => {
        logInfo("Auto-Registering tables from Datasource '" + ds + "'")
        AbstractSapSQLContext
          .registerTablesFromDs(ds, this, Map.empty[String, String], ignoreConflicts = true)
      })
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

private[sql] object AbstractSapSQLContext {
  val PROPERTY_IGNORE_USE_STATEMENTS = "spark.vora.ignore_use_statements"
  val PROPERTY_AUTO_REGISTER_TABLES = "spark.vora.autoregister"

  private def registerTablesFromDs(provider: String, sqlc: SQLContext,
                                   options: Map[String,String], ignoreConflicts: Boolean): Unit = {
    DataFrame(sqlc, new RegisterAllTablesUsing(provider, options, ignoreConflicts))
  }
}

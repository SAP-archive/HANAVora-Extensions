package org.apache.spark.sql

import java.io.InputStream
import java.util.Properties

import org.apache.spark.sql.extension._
import org.apache.spark.sql.sources.commands.RegisterAllTablesUsing

/**
  * Mixin for [[SQLContext]] derivatives providing functionality specific to SAP Spark extensions.
  * This trait is used both by [[SapSQLContext]] and [[org.apache.spark.sql.hive.SapHiveContext]]
  * to share functionality.
  */
private[sql] trait CommonSapSQLContext
  extends SapSQLContextExtension {
  self: SQLContext =>

  checkSparkVersion(List("1.5.0", "1.5.1", "1.5.2"))
  logProjectVersion()
  // check if we have to automatically register tables
  sparkContext.getConf.getOption(CommonSapSQLContext.PROPERTY_AUTO_REGISTER_TABLES) match {
    case None => // do nothing
    case Some(conf) =>
      conf.split(",").foreach(ds => {
        logInfo("Auto-Registering tables from Datasource '" + ds + "'")
        CommonSapSQLContext
          .registerTablesFromDs(ds, this, Map.empty[String, String], ignoreConflicts = true)
      })
  }

  def checkSparkVersion(supportedVersions:List[String]): Unit = {
     if (!supportedVersions.contains(org.apache.spark.SPARK_VERSION)){
       logError(s"Spark Version mismatch: Supported: ${supportedVersions.mkString(",")}, " +
                s"Runtime is: ${org.apache.spark.SPARK_VERSION}")
       throw new RuntimeException ("Termination due to Spark version mismatch")
     }
  }

  private[this] def logProjectVersion(): Unit = {
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

private[sql] object CommonSapSQLContext {
  val PROPERTY_IGNORE_USE_STATEMENTS = "spark.vora.ignore_use_statements"
  val PROPERTY_AUTO_REGISTER_TABLES = "spark.vora.autoregister"

  private def registerTablesFromDs(provider: String, sqlc: SQLContext,
                                   options: Map[String,String], ignoreConflicts: Boolean): Unit = {
    DataFrame(sqlc, new RegisterAllTablesUsing(provider, options, ignoreConflicts))
  }
}

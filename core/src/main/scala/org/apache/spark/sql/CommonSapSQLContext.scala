package org.apache.spark.sql

import java.util.Properties

import org.apache.spark.sql.execution.datasources.RegisterAllTablesCommand
import org.apache.spark.sql.extension._

import scala.util.Try

/**
  * Mixin for [[SQLContext]] derivatives providing functionality specific to SAP Spark extensions.
  * This trait is used both by [[SapSQLContext]] and [[org.apache.spark.sql.hive.SapHiveContext]]
  * to share functionality.
  */
private[sql] trait CommonSapSQLContext
  extends SapSQLContextExtension {
  self: SQLContext =>

  val supportedVersions: List[String] = List("1.6.0","1.6.1", "1.6.2")

  checkSparkVersion(supportedVersions)

  lazy private val projectProperties: Option[Properties] = Try {
    val props = new Properties()
    val stream = getClass.getResourceAsStream("/project.properties")
    try {
      props.load(stream)
    } finally {
      stream.close()
    }
    props
  }.toOption

  val EXTENSIONS_VERSION: Option[String] =
    projectProperties.flatMap(props => Option(props.get("sapextensions.version").toString))
  val DATASOURCES_VERSION: Option[String] =
    projectProperties.flatMap(props => Option(props.get("datasourcedist.version").toString))

  logProjectVersion()
  // check if we have to automatically register tables
  sparkContext.getConf.getOption(CommonSapSQLContext.PROPERTY_AUTO_REGISTER_TABLES) match {
    case None => // do nothing
    case Some(conf) =>
      conf.split(",").foreach(ds => {
        logInfo("Auto-Registering tables from Datasource '" + ds + "'")
        CommonSapSQLContext.registerTablesFromDs(
          ds,
          this,
          Map.empty[String, String],
          ignoreConflicts = true,
          allowExisting = true)
      })
  }

  private[sql] def getCurrentSparkVersion(): String = org.apache.spark.SPARK_VERSION

  def checkSparkVersion(supportedVersions:List[String]): Unit = {
    // only one of the versions have to match
     if (!supportedVersions.exists(supportedVersion =>
            getCurrentSparkVersion().contains(supportedVersion))){
       logError(s"Spark Version mismatch: Supported: ${supportedVersions.mkString(",")}, " +
                s"Runtime is: ${org.apache.spark.SPARK_VERSION}")
       throw new RuntimeException ("Termination due to Spark version mismatch")
     }
  }

  private[this] def logProjectVersion(): Unit = {
    DATASOURCES_VERSION match {
      case None => {
        logDebug("hanalite-spark-datasources version unknown")
      }
      case Some(version) => logInfo(s"SapSQLContext [version: $version] created")
    }
  }
}

private[sql] object CommonSapSQLContext {
  val PROPERTY_IGNORE_USE_STATEMENTS = "spark.vora.ignore_use_statements"
  val PROPERTY_AUTO_REGISTER_TABLES = "spark.sap.autoregister"

  private def registerTablesFromDs(provider: String,
                                   sqlc: SQLContext,
                                   options: Map[String, String],
                                   ignoreConflicts: Boolean,
                                   allowExisting: Boolean): Unit = {
    DataFrame(sqlc, RegisterAllTablesCommand(provider, options, ignoreConflicts, allowExisting))
  }
}

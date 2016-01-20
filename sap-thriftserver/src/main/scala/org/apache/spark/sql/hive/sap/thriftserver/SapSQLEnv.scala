package org.apache.spark.sql.hive.sap.thriftserver

import java.io.PrintStream

import org.apache.spark.scheduler.StatsReportListener
import org.apache.spark.sql.hive.{HiveContext, SapHiveContext}
import org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver
import org.apache.spark.sql.hive.thriftserver.SparkSQLEnv._
import org.apache.spark.util.Utils
import org.apache.spark.{Logging, SparkConf, SparkContext}

import scala.collection.JavaConversions._

/** A singleton object for the master program. The slaves should not access this. */
object SapSQLEnv extends Logging {

  def init() {
    logDebug("Initializing SapSQLEnv")
    if (hiveContext == null) {
      logInfo("Creating SapSQLContext")
      val sparkConf = new SparkConf(loadDefaults = true)
      val maybeSerializer = sparkConf.getOption("spark.serializer")
      val maybeKryoReferenceTracking = sparkConf.getOption("spark.kryo.referenceTracking")
      // If user doesn't specify the appName, we want to get [SparkSQL::localHostName] instead of
      // the default appName [SparkSQLCLIDriver] in cli or beeline.
      val maybeAppName = sparkConf
        .getOption("spark.app.name")
        .filterNot(_ == classOf[SparkSQLCLIDriver].getName)

      sparkConf
        .setAppName(maybeAppName.getOrElse(s"SparkSQL::${Utils.localHostName()}"))
        .set("spark.serializer",
          maybeSerializer.getOrElse("org.apache.spark.serializer.KryoSerializer"))
        .set("spark.kryo.referenceTracking",
          maybeKryoReferenceTracking.getOrElse("false"))

      sparkContext = new SparkContext(sparkConf)
      sparkContext.addSparkListener(new StatsReportListener())
      hiveContext = new SapHiveContext(sparkContext)

      hiveContext.metadataHive.setOut(new PrintStream(System.out, true, "UTF-8"))
      hiveContext.metadataHive.setInfo(new PrintStream(System.err, true, "UTF-8"))
      hiveContext.metadataHive.setError(new PrintStream(System.err, true, "UTF-8"))

      hiveContext.setConf("spark.sql.hive.version", HiveContext.hiveExecutionVersion)

      if (log.isDebugEnabled) {
        hiveContext.hiveconf.getAllProperties.toSeq.sorted.foreach { case (k, v) =>
          logDebug(s"HiveConf var: $k=$v")
        }
      }
    }
  }
}

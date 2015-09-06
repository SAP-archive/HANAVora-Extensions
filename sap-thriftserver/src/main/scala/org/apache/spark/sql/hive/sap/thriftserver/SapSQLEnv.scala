package org.apache.spark.sql.hive.sap.thriftserver

import java.io.PrintStream

import org.apache.spark.scheduler.StatsReportListener
import org.apache.spark.sql.SapSQLContext
import org.apache.spark.sql.hive.HiveShim
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

      sparkConf
        .setAppName(s"SparkSQL::${Utils.localHostName()}")
        .set("spark.serializer",
          maybeSerializer.getOrElse("org.apache.spark.serializer.KryoSerializer"))
        .set("spark.kryo.referenceTracking",
          maybeKryoReferenceTracking.getOrElse("false"))

      sparkContext = new SparkContext(sparkConf)
      sparkContext.addSparkListener(new StatsReportListener())
      hiveContext = new SapSQLContext(sparkContext)

      hiveContext.metadataHive.setOut(new PrintStream(System.out, true, "UTF-8"))
      hiveContext.metadataHive.setInfo(new PrintStream(System.err, true, "UTF-8"))
      hiveContext.metadataHive.setError(new PrintStream(System.err, true, "UTF-8"))

      hiveContext.setConf("spark.sql.hive.version", HiveShim.version)

      if (log.isDebugEnabled) {
        hiveContext.hiveconf.getAllProperties.toSeq.sorted.foreach { case (k, v) =>
          logDebug(s"HiveConf var: $k=$v")
        }
      }
    }
  }
}

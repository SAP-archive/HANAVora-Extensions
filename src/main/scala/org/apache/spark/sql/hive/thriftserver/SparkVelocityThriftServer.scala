package org.apache.spark.sql.hive.thriftserver

import org.apache.commons.logging.LogFactory
import org.apache.hive.service.server.ServerOptionsProcessor
import org.apache.spark.Logging
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2._
import org.apache.spark.sql.hive.thriftserver.ui.ThriftServerTab
import org.apache.spark.sql.hive.velocity.thriftserver.VelocitySQLEnv
import org.apache.spark.util.Utils

object SparkVelocityThriftServer extends Logging {
  var LOG = LogFactory.getLog(classOf[SparkVelocityThriftServer])

  def main(args: Array[String]) {
    val optionsProcessor = new ServerOptionsProcessor("SparkVelocityThriftServer")
    if (!optionsProcessor.process(args)) {
      System.exit(-1)
    }

    logInfo("Starting SparkContext")
    VelocitySQLEnv.init()

    Utils.addShutdownHook { () =>
      SparkSQLEnv.stop()
      uiTab.foreach(_.detach())
    }

    try {
      val server = new HiveThriftServer2(SparkSQLEnv.hiveContext)
      server.init(SparkSQLEnv.hiveContext.hiveconf)
      server.start()
      logInfo("SparkVelocityThriftServer started")
      listener = new HiveThriftServer2Listener(server, SparkSQLEnv.hiveContext.conf)
      SparkSQLEnv.sparkContext.addSparkListener(listener)
      uiTab = if (SparkSQLEnv.sparkContext.getConf.getBoolean("spark.ui.enabled", true)) {
        Some(new ThriftServerTab(SparkSQLEnv.sparkContext))
      } else {
        None
      }
    } catch {
      case e: Exception =>
        logError("Error starting SparkVelocityThriftServer", e)
        System.exit(-1)
    }
  }
}

private[hive] class SparkVelocityThriftServer(val hiveContext: HiveContext) extends Logging{

  def start: Unit = {
    logInfo("ThriftServer with VelocitySQLContext")
    logInfo("Starting SparkContext")
    HiveThriftServer2.startWithContext(hiveContext)
  }
}

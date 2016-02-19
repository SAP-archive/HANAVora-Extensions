package org.apache.spark.sql.hive.thriftserver

import org.apache.commons.logging.LogFactory
import org.apache.spark.Logging
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.sap.thriftserver.SapSQLEnv
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2._
import org.apache.spark.sql.hive.thriftserver.ui.ThriftServerTab
import org.apache.hive.service.server.HiveServerServerOptionsProcessor

object SapThriftServer extends Logging {
  var LOG = LogFactory.getLog(classOf[SapThriftServer])


  def main(args: Array[String]) {
    val optionsProcessor = new HiveServerServerOptionsProcessor("SapThriftServer")
    if (!optionsProcessor.process(args)) {
      System.exit(-1)
    }

    logInfo("Starting SparkContext")
    SapSQLEnv.init()

    org.apache.spark.util.ShutdownHookManager.addShutdownHook { () =>
      SparkSQLEnv.stop()
      uiTab.foreach(_.detach())
    }

    try {
      val server = new HiveThriftServer2(SparkSQLEnv.hiveContext)
      server.init(SparkSQLEnv.hiveContext.hiveconf)
      server.start()
      logInfo("SapThriftServer started")
      listener = new HiveThriftServer2Listener(server, SparkSQLEnv.hiveContext.conf)
      SparkSQLEnv.sparkContext.addSparkListener(listener)
      uiTab = if (SparkSQLEnv.sparkContext.getConf.getBoolean("spark.ui.enabled", true)) {
        Some(new ThriftServerTab(SparkSQLEnv.sparkContext))
      } else {
        None
      }
    } catch {
      case e: Exception =>
        logError("Error starting SapThriftServer", e)
        System.exit(-1)
    }
  }
}

private[hive] class SapThriftServer(val hiveContext: HiveContext) extends Logging{

  def start: Unit = {
    logInfo("ThriftServer with SapSQLContext")
    logInfo("Starting SparkContext")
    HiveThriftServer2.startWithContext(hiveContext)
  }
}

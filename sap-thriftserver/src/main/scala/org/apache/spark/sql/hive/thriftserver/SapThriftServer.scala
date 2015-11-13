package org.apache.spark.sql.hive.thriftserver

import org.apache.commons.logging.LogFactory
import org.apache.spark.Logging
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.sap.thriftserver.SapSQLEnv
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2._
import org.apache.spark.sql.hive.thriftserver.ui.ThriftServerTab

object SapThriftServer extends Logging {
  var LOG = LogFactory.getLog(classOf[SapThriftServer])

  private def processOptions(name: String, args: Array[String]): Boolean = {
    val optionsProcessorClass = org.apache.spark.SPARK_VERSION match {
      case v if v startsWith "1.4" =>
        "org.apache.hive.service.server.ServerOptionsProcessor"
      case v if v startsWith "1.5" =>
        "org.apache.hive.service.server.HiveServerServerOptionsProcessor"
      case other =>
        sys.error(s"SPARK_VERSION $other is not supported")
    }
    val clazz = Class.forName(optionsProcessorClass)
    val optionsProcessor =
      clazz
        .getConstructor(classOf[String])
        .newInstance(name)
    clazz
      .getMethod("process", classOf[Array[String]])
      .invoke(optionsProcessor, args)
      .asInstanceOf[Boolean]
  }

  private def addShutdownHook(block: () => Unit): Unit = {
    val utilsClass = org.apache.spark.SPARK_VERSION match {
      case v if v startsWith "1.4" =>
        "org.apache.spark.util.Utils$"
      case v if v startsWith "1.5" =>
        "org.apache.spark.util.ShutdownHookManager$"
      case other =>
        sys.error(s"SPARK_VERSION $other is not supported")
    }
    val c = Class.forName(utilsClass)
    val obj = c.getField("MODULE$").get(c)
    c.getDeclaredMethod("addShutdownHook", classOf[() => Unit])
      .invoke(obj, block)
  }

  def main(args: Array[String]) {
    if (!processOptions("SapThriftServer", args)) {
      System.exit(-1)
    }

    logInfo("Starting SparkContext")
    SapSQLEnv.init()

    addShutdownHook { () =>
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

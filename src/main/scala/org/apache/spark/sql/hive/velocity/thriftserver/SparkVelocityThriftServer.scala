package org.apache.spark.sql.hive.velocity.thriftserver

import org.apache.commons.logging.LogFactory
import org.apache.hive.service.server.ServerOptionsProcessor
import org.apache.spark.Logging
import org.apache.spark.sql.VelocitySQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.thriftserver.SparkSQLEnv._
import org.apache.spark.sql.hive.thriftserver.{HiveThriftServer2, SparkSQLEnv, ReflectedCompositeService}
import org.apache.spark.sql.hive.velocity.thriftserver.SparkVelocityThriftServer._


object SparkVelocityThriftServer  {
  var LOG = LogFactory.getLog(classOf[SparkVelocityThriftServer])

  def main(args: Array[String]) {
    val optionsProcessor = new ServerOptionsProcessor("HiveThriftServer2")
    if (!optionsProcessor.process(args)) {
      System.exit(-1)
    }
    val thriftServer=new SparkVelocityThriftServer
    thriftServer.start


  }


}

private[hive] class SparkVelocityThriftServer extends Logging{

  def start: Unit = {
    logInfo("ThriftServer with VelocitySQLContext")
    logInfo("Starting SparkContext")
    init()
    val hContext: HiveContext = new VelocitySQLContext(sparkContext).asInstanceOf[HiveContext]
    startWithContext(hContext)
  }

  def startWithContext(hContext: HiveContext): Unit = {
    HiveThriftServer2.startWithContext(hContext)
  }
}


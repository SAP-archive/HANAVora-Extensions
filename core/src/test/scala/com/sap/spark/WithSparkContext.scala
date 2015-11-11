package com.sap.spark

import com.sap.spark.util.TestUtils._
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait WithSparkContext extends BeforeAndAfterAll {
  self: Suite =>

  override def beforeAll(): Unit = {
    try {
      super.beforeAll()
      setUpSparkContext()
    } catch {
      case ex: Throwable =>
        tearDownSparkContext()
        throw ex
    }
  }

  override def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      tearDownSparkContext()
    }
  }

  /**
   * evaluate the Spark master URL, given as property or as environment variable
     to support different spark deployment options.
   */
  protected def sparkMaster: String =
    getSetting("spark.master", s"local[$numberOfSparkWorkers]")

  /**
   * Number of workers to use (default: 3).
   */
  protected lazy val numberOfSparkWorkers: Int =
    getSetting("spark.workers", "3").toInt

  def sparkConf: SparkConf = {
    val conf = new SparkConf(loadDefaults = false)
    conf.set("spark.driver.allowMultipleContexts", "true")

    /* XXX: Prevent 200 partitions on shuffle */
    conf.set("spark.sql.shuffle.partitions", "4")
    /* XXX: Disable join broadcast */
    conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    conf.set("spark.broadcast.factory", "org.apache.spark.broadcast.HttpBroadcastFactory")
    conf.set("spark.shuffle.spill", "false")
    conf.set("spark.shuffle.compress", "false")
    conf.set("spark.ui.enabled", "false")
    conf.set("spark.ui.showConsoleProgress", "false")
  }

  def sc: SparkContext

  protected def setUpSparkContext(): Unit

  protected def tearDownSparkContext(): Unit

}

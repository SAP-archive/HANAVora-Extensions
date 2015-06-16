package org.apache.spark

import corp.sap.spark.{GlobalSparkContext, WithSparkContext}
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfterEach, BeforeAndAfter, Suite}

/**
 * Provides a simple mocked SparkContext.
 */
trait MockitoSparkContext extends WithSparkContext {
  self: Suite =>

  private var _sparkConf : SparkConf = null
  private var _sc : SparkContext = null

  override def sc: SparkContext = _sc

  override protected def setUpSparkContext(): Unit = {
    _sc = mock[SparkContext](classOf[SparkContext])
    _sparkConf = new SparkConf(loadDefaults = false)
    when(_sc.conf).thenReturn(_sparkConf)
    when(_sc.getConf).thenReturn(_sparkConf)
  }

  override protected def tearDownSparkContext(): Unit = {
    _sc = null
  }

}


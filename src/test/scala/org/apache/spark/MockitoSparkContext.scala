package org.apache.spark

import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfterEach, BeforeAndAfter, Suite}

/**
 * Provides a simple mocked SparkContext.
 */
trait MockitoSparkContext extends BeforeAndAfterEach { self: Suite =>

  def sc : SparkContext = _sc

  private var _sparkConf : SparkConf = null
  private var _sc : SparkContext = null

  override def beforeEach() {
    super.beforeEach()
    _sc = mock[SparkContext](classOf[SparkContext])
    _sparkConf = new SparkConf(loadDefaults = false)
    when(_sc.conf).thenReturn(_sparkConf)
    when(_sc.getConf).thenReturn(_sparkConf)
  }

}


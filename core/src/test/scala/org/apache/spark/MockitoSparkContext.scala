package org.apache.spark

import com.sap.spark.WithSparkContext
import org.mockito.Mockito._
import org.scalatest.Suite
import org.scalatest.mock.MockitoSugar

/**
 * Provides a simple mocked SparkContext.
 */
trait MockitoSparkContext extends WithSparkContext with MockitoSugar {
  self: Suite =>

  private var _sparkConf: SparkConf = _
  private var _sc: SparkContext = _

  override def sc: SparkContext = _sc

  protected def mockSparkConf: SparkConf = _sparkConf

  override protected def setUpSparkContext(): Unit = {
    _sparkConf = sparkConf
    _sc = mock[SparkContext](withSettings().stubOnly())
    when(_sc.conf).thenReturn(_sparkConf)
    when(_sc.getConf).thenReturn(_sparkConf)
    when(_sc.ui).thenReturn(None)
  }

  override protected def tearDownSparkContext(): Unit = {
    _sc.stop()
    _sc = null
  }

}

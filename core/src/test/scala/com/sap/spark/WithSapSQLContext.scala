package com.sap.spark

import org.apache.spark.sql.SapSQLContext
import org.scalatest.Suite

/**
 * Trait to be used for every class/trait that uses a new SapSQLContext for every test.
 */
trait WithSapSQLContext extends WithSQLContext {
  self: Suite with WithSparkContext =>

  override protected def setUpSQLContext(): Unit =
    _sqlContext = new SapSQLContext(sc)

}

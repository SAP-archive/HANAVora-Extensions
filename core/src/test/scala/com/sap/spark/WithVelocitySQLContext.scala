package com.sap.spark

import org.apache.spark.sql.VelocitySQLContext
import org.scalatest.Suite

/**
 * Trait to be used for every class/trait that uses a new VelocitySQLContext for every test.
 */
trait WithVelocitySQLContext extends WithSQLContext {
  self: Suite with WithSparkContext =>

  override protected def setUpSQLContext(): Unit =
    _sqlContext = new VelocitySQLContext(sc)

}

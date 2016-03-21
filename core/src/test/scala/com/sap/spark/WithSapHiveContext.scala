package com.sap.spark

import org.apache.spark.sql.hive.SapHiveContext
import org.scalatest.Suite

/**
 * Trait to be used for every class/trait that uses a new HiveContext for every test.
 */
trait WithSapHiveContext extends WithSQLContext {
  self: Suite with WithSparkContext =>

  override protected def setUpSQLContext(): Unit =
    _sqlContext = new SapHiveContext(sc)

}

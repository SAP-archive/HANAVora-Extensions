package com.sap.spark

import org.apache.spark.sql.SapSQLContext
import org.scalatest.Suite


// TODO   the following trait is kept in until version 0.0.9
//        in order to avoid build problems in datasource package
@deprecated
trait WithVelocitySQLContext extends WithSQLContext {
  self: Suite with WithSparkContext =>

  override protected def setUpSQLContext(): Unit =
    _sqlContext = new SapSQLContext(sc)

}

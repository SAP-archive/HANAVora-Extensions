package org.apache.spark.sql

import com.sap.spark.{GlobalSparkContext, WithSQLContext}
import org.apache.spark.SparkContext
import org.scalatest.Suite

trait GlobalSapSQLContext extends GlobalSparkContext with WithSQLContext {
  self: Suite =>

  override def sqlContext: SQLContext = GlobalSapSQLContext._sqlc

  override protected def setUpSQLContext(): Unit =
    GlobalSapSQLContext.init(sc)

  override protected def tearDownSQLContext(): Unit =
    GlobalSapSQLContext.reset()

}



object GlobalSapSQLContext {

  private var _sqlc: SapSQLContext = _

  private def init(sc: SparkContext): Unit = {
    if (_sqlc == null) {
      _sqlc = new SapSQLContext(sc)
    }
  }

  private def reset(): Unit = {
    _sqlc.catalog.unregisterAllTables()
  }

}


trait GlobalVelocitySQLContext extends GlobalSparkContext with WithSQLContext {
  self: Suite =>
  override def sqlContext: SQLContext = GlobalVelocitySQLContext._sqlc

  override protected def setUpSQLContext(): Unit =
    GlobalVelocitySQLContext.init(sc)

  override protected def tearDownSQLContext(): Unit =
    GlobalVelocitySQLContext.reset()
}

object GlobalVelocitySQLContext {

  private var _sqlc: SapSQLContext = _

  private def init(sc: SparkContext): Unit = {
    if (_sqlc == null) {
      _sqlc = new SapSQLContext(sc)
    }
  }

  private def reset(): Unit = {
    _sqlc.catalog.unregisterAllTables()
  }

}





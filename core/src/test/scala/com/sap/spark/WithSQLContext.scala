package com.sap.spark

import java.util.Locale

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.scalatest.{BeforeAndAfterEach, Suite}

trait WithSQLContext extends BeforeAndAfterEach {
  self: Suite with WithSparkContext =>

  override def beforeEach(): Unit = {
    try {
      super.beforeEach()
      setUpSQLContext()
    } catch {
      case ex: Throwable =>
        tearDownSQLContext()
        throw ex
    }
  }

  override def afterEach(): Unit = {
    try {
      super.afterEach()
    } finally {
      tearDownSQLContext()
    }
  }

  implicit def sqlContext: SQLContext = _sqlContext
  def sqlc: SQLContext = sqlContext

  var _sqlContext: SQLContext = _

  protected def setUpSQLContext(): Unit =
    _sqlContext = SQLContext.getOrCreate(sc).newSession()


  protected def tearDownSQLContext(): Unit =
    _sqlContext = null

  protected def tableName(name: String): String =
    sqlc match {
      /* Hive tables are all lower case */
      case _: HiveContext => name.toLowerCase(Locale.ENGLISH)
      case _ => name
    }

}



package org.apache.spark.sql

import org.apache.spark.MockitoSparkContext
import org.scalatest.FunSuite


class AutoRegisterSuite extends FunSuite with MockitoSparkContext {

  test("Auto-registering Feature ON") {
    val relationName = "TestRelation"
    com.sap.spark.dstest.DefaultSource.addRelation(relationName)
    mockSparkConf.set(SapSQLContext.PROPERTY_AUTO_REGISTER_TABLES, "com.sap.spark.dstest")
    val sapSQLContext = new SapSQLContext(sc)
    val tables = sapSQLContext.tableNames()
    assert(tables.contains(relationName))
  }

  test("Auto-registering Feature OFF") {
    mockSparkConf.remove(SapSQLContext.PROPERTY_AUTO_REGISTER_TABLES)
    val sapSQLContext = new SapSQLContext(sc)
    val tables = sapSQLContext.tableNames()
    assert(tables.length == 0)
  }

}


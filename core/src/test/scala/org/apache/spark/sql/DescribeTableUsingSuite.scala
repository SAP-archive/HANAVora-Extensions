package org.apache.spark.sql

import org.scalatest.FunSuite

/**
  * Defines a set of tests for
  * [[org.apache.spark.sql.execution.datasources.DescribeTableUsingRunnableCommand]]
  */
class DescribeTableUsingSuite extends FunSuite
  with GlobalSapSQLContext {

  test("DESCRIBE existing table") {
    // simulate existing relation in the data source's catalog.
    com.sap.spark.dstest.DefaultSource.addRelation("t1")
    val actual = sqlContext.sql(s"DESCRIBE TABLE t1 USING com.sap.spark.dstest").collect
    assertResult(Seq(Row("t1", "<DDL statement>")))(actual)
  }

  test("DESCRIBE non-existing table") {
    val actual = sqlContext.sql(s"DESCRIBE TABLE nonExisting USING com.sap.spark.dstest").collect
    assertResult(Seq(Row("", "")))(actual)
  }

}

package org.apache.spark.sql

import org.scalatest.FunSuite

class DropCommandSuite extends FunSuite with GlobalSapSQLContext {

  lazy val existingTable1 = tableName("existingTable1")
  lazy val existingTable2 = tableName("existingTable2")
  lazy val willBeDeleted = tableName("willBeDeleted")
  lazy val someTable = tableName("someTable")
  lazy val someTable2 = tableName("someTable2")
  lazy val someView = tableName("someView")

  test("Drop a Spark table referencing Vora tables removes it from Spark catalog."){
    sqlContext.sql(s"""CREATE TABLE $existingTable1 (id string)
                      |USING com.sap.spark.dstest
                      |OPTIONS ()""".stripMargin)

    sqlContext.sql(s"""CREATE TEMPORARY TABLE $existingTable2 (id string)
                      |USING com.sap.spark.dstest
                      |OPTIONS ()""".stripMargin)

    sqlContext.sql(s"""CREATE TABLE $willBeDeleted
                      |USING com.sap.spark.dstest
                      |OPTIONS ()""".stripMargin)

    val result = sqlContext.tables().collect()
    assert(result.contains(Row(existingTable1, false)))
    assert(result.contains(Row(existingTable2, true)))
    assert(result.contains(Row(willBeDeleted, false)))
    assert(result.length == 3)

    sqlContext.sql(s"DROP TABLE $willBeDeleted")

    val result2 = sqlContext.tables().collect()
    assert(result2.length == 2)
    assert(result2.contains(Row(existingTable1, false)))
    assert(result2.contains(Row(existingTable2, true)))
    assert(!result2.contains(Row(willBeDeleted, false)))
  }

  test("Drop Spark table succeeds if it does not exist but if exists flag is provided") {
    sqlContext
      .sql(s"DROP TABLE IF EXISTS $someTable")
  }

  test("Drop Spark table succeeds if it does exist and if exists flag is provided") {
    sqlContext.sql(s"""CREATE TABLE $existingTable1 (id string)
                      |USING com.sap.spark.dstest
                      |OPTIONS ()""".stripMargin)

    sqlContext.sql(s"""CREATE TEMPORARY TABLE $existingTable2 (id string)
                      |USING com.sap.spark.dstest
                      |OPTIONS ()""".stripMargin)

    sqlContext.sql(s"""CREATE TABLE $willBeDeleted
                      |USING com.sap.spark.dstest
                      |OPTIONS ()""".stripMargin)

    val result = sqlContext.tables().collect()
    assert(result.contains(Row(existingTable1, false)))
    assert(result.contains(Row(existingTable2, true)))
    assert(result.contains(Row(willBeDeleted, false)))
    assert(result.length == 3)

    sqlContext.sql(s"DROP TABLE IF EXISTS $willBeDeleted")

    val result2 = sqlContext.tables().collect()
    assert(result2.length == 2)
    assert(result2.contains(Row(existingTable1, false)))
    assert(result2.contains(Row(existingTable2, true)))
    assert(!result2.contains(Row(willBeDeleted, false)))
  }

  test("Drop Spark table fails if it is referenced more than once in catalog."){
    val ex = intercept[RuntimeException] {
      sqlContext.sql(s"""CREATE TABLE $someTable (id string)
                        |USING com.sap.spark.dstest
                        |OPTIONS ()""".stripMargin)

      sqlContext.sql(s"""CREATE TEMPORARY TABLE $someTable2 (id string)
                        |USING com.sap.spark.dstest
                        |OPTIONS ()""".stripMargin)

      sqlContext.sql(s"""CREATE VIEW $someView
                        |AS SELECT * FROM someTable""".stripMargin)

      val result = sqlContext.tables().collect()
      assert(result.contains(Row(someTable, false)))
      assert(result.contains(Row(someTable2, true)))
      assert(result.contains(Row(someView, false)))
      assert(result.length == 3)

      sqlContext.sql(s"DROP TABLE $someTable")
    }

    assert(ex.getMessage.contains("Can not drop because more than one relation has " +
      "references to the target relation"))
  }

  test("Drop Spark table succeeds if it referenced more than once in catalog" +
    "with cascading") {
    sqlContext.sql(s"""CREATE TABLE $someTable (id string)
                      |USING com.sap.spark.dstest
                      |OPTIONS ()""".stripMargin)

    sqlContext.sql(s"""CREATE TEMPORARY TABLE $someTable2 (id string)
                      |USING com.sap.spark.dstest
                      |OPTIONS ()""".stripMargin)

    sqlContext.sql(s"""CREATE VIEW someView
                      |AS SELECT * FROM $someTable""".stripMargin)

    val result = sqlContext.tables().collect()
    assert(result.contains(Row(someTable, false)))
    assert(result.contains(Row(someTable2, true)))
    assert(result.contains(Row(someView, false)))
    assert(result.length == 3)

    sqlContext.sql(s"DROP TABLE $someTable CASCADE")

    val result2 = sqlContext.tables().collect()
    assert(result2.length == 1)
    assert(result2.contains(Row(someTable2, true)))
  }
}

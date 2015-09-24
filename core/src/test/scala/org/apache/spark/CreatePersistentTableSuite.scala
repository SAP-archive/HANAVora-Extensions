package org.apache.spark

import com.sap.spark.util.TestUtils._
import org.apache.spark.sql.{GlobalVelocitySQLContext, Row}
import org.scalatest.FunSuite

/**
 * Tests the additional possibility of creating temporary / persistent tables
 */
class CreatePersistentTableSuite extends FunSuite with GlobalVelocitySQLContext {

  test("Create non-temporary table") {
    sqlContext.sql( s"""CREATE TABLE tableNotTemp (field string)
                       |USING com.sap.spark.dstest
                       |OPTIONS ()""".stripMargin)


    sqlContext.sql( s"""CREATE TEMPORARY TABLE tableTemp (field string)
                       |USING com.sap.spark.dstest
                       |OPTIONS ()""".stripMargin)

    // test with no schema
    sqlContext.sql( s"""CREATE TABLE testTableNoSchema
                       |USING com.sap.spark.dstest
                       |OPTIONS ()""".stripMargin)

    val result = sqlContext.tables().collect()

    assert(result.contains(Row("tableTemp", true)))
    assert(result.contains(Row("tableNotTemp", false)))
    assert(result.contains(Row("testTableNoSchema", false)))
    assert(result.length == 3)
  }


  test("I can't register a persistent table using a datasource that doesn't extend " +
    "TemporaryAndPersistentNature") {

    val path = getFileFromClassPath("/simple.csv")
    val tableName = "tableTestPersistent"

    val ex = intercept[RuntimeException] {
      sqlc.sql(
        s"""
           |CREATE TABLE $tableName (name varchar(200), age integer)
                                     |USING org.apache.spark.sql.json
                                     |OPTIONS (
                                     |path "$path"
                                                   |)""".stripMargin)
    }
    assert(ex.getMessage.
      equals("Tables created with SQLContext must be TEMPORARY. Use a HiveContext instead."))
  }

  test("I can register a temporary table using a datasource that doesn't extend " +
    "TemporaryAndPersistentNature") {

    val path = getFileFromClassPath("/simple.csv")
    val tableName = "tableTestTemporary"

    sqlc.sql(
      s"""
         |CREATE TEMPORARY TABLE $tableName (name varchar(200), age integer)
         |USING org.apache.spark.sql.json
         |OPTIONS (
         |path "$path"
         |)""".stripMargin)

    val result = sqlContext.tables().collect()

    assert(result.length == 1)
    assert(result.contains(Row(tableName, true)))
  }
}

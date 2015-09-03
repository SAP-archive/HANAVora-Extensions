package org.apache.spark

import org.apache.spark.sql.{Row, GlobalVelocitySQLContext}
import org.scalatest.FunSuite

/**
 * Tests the additional possibility of creating temporary / persistent tables
 */
class CreatePersistentTableSuite extends FunSuite with GlobalVelocitySQLContext{

  test("Create non-temporary table"){
    sqlContext.sql(s"""CREATE TABLE tableNotTemp (field string)
                      |USING com.sap.spark.dstest
                      |OPTIONS ()""".stripMargin)


    sqlContext.sql(s"""CREATE TEMPORARY TABLE tableTemp (field string)
                      |USING com.sap.spark.dstest
                      |OPTIONS ()""".stripMargin)

    // test with no schema
    sqlContext.sql(s"""CREATE TABLE testTableNoSchema
                      |USING com.sap.spark.dstest
                      |OPTIONS ()""".stripMargin)

    val result = sqlContext.tables().collect()

    assert(result.contains(Row("tableTemp", true)))
    assert(result.contains(Row("tableNotTemp", false)))
    assert(result.contains(Row("testTableNoSchema", false)))
    assert(result.length == 3)
  }
}

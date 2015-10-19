package org.apache.spark.sql

import org.scalatest.FunSuite

class DropCommandSuite extends FunSuite with GlobalVelocitySQLContext{

  test("Drop a Spark table referencing Velocity tables removes it from Spark catalog."){
    sqlContext.sql(s"""CREATE TABLE existingTable1 (id string)
                      |USING com.sap.spark.dstest
                      |OPTIONS ()""".stripMargin)

    sqlContext.sql(s"""CREATE TEMPORARY TABLE existingTable2 (id string)
                      |USING com.sap.spark.dstest
                      |OPTIONS ()""".stripMargin)

    sqlContext.sql(s"""CREATE TABLE willBeDeleted
                      |USING com.sap.spark.dstest
                      |OPTIONS ()""".stripMargin)

    val result = sqlContext.tables().collect()
    assert(result.contains(Row("existingTable1", false)))
    assert(result.contains(Row("existingTable2", true)))
    assert(result.contains(Row("willBeDeleted", false)))
    assert(result.length == 3)

    sqlContext.sql("DROP TABLE willBeDeleted")

    val result2 = sqlContext.tables().collect()
    assert(result2.length == 2)
    assert(result2.contains(Row("existingTable1", false)))
    assert(result2.contains(Row("existingTable2", true)))
    assert(!result2.contains(Row("willBeDeleted", false)))
  }

  test("Drop Spark table fails if it is referenced more than once in catalog."){
    val ex = intercept[RuntimeException] {
      sqlContext.sql(s"""CREATE TABLE someTable (id string)
                        |USING com.sap.spark.dstest
                        |OPTIONS ()""".stripMargin)

      sqlContext.sql(s"""CREATE TEMPORARY TABLE someTable2 (id string)
                        |USING com.sap.spark.dstest
                        |OPTIONS ()""".stripMargin)

      sqlContext.sql(s"""CREATE VIEW someView
                        |AS SELECT * FROM someTable""".stripMargin)

      val result = sqlContext.tables().collect()
      assert(result.contains(Row("someTable", false)))
      assert(result.contains(Row("someTable2", true)))
      assert(result.contains(Row("someView", false)))
      assert(result.length == 3)

      sqlContext.sql("DROP TABLE someTable")
    }

    assert(ex.getMessage.contains("Can not drop because more than one relation has " +
      "references to the target relation"))
  }

  test("Drop Spark table succeeds if it referenced more than once in catalog" +
    "with cascading") {
    sqlContext.sql(s"""CREATE TABLE someTable (id string)
                      |USING com.sap.spark.dstest
                      |OPTIONS ()""".stripMargin)

    sqlContext.sql(s"""CREATE TEMPORARY TABLE someTable2 (id string)
                      |USING com.sap.spark.dstest
                      |OPTIONS ()""".stripMargin)

    sqlContext.sql(s"""CREATE VIEW someView
                      |AS SELECT * FROM someTable""".stripMargin)

    val result = sqlContext.tables().collect()
    assert(result.contains(Row("someTable", false)))
    assert(result.contains(Row("someTable2", true)))
    assert(result.contains(Row("someView", false)))
    assert(result.length == 3)

    sqlContext.sql("DROP TABLE someTable CASCADE")

    val result2 = sqlContext.tables().collect()
    assert(result2.length == 1)
    assert(result2.contains(Row("someTable2", true)))
  }
}

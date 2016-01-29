package com.sap.spark

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import com.sap.spark.catalystSourceTest.{CatalystSourceTestRDD, CataystSourceTestRDDPartition}
import org.apache.spark.sql.{GlobalSapSQLContext, Row, SQLContext}
import org.scalatest.{BeforeAndAfterAll, ConfigMap, FunSuite}

/**
  * This uses the com.sap.spark.catalystSourceTest Default source to test the pushdown of plans
  */
// scalastyle:off magic.number
class CatalystSourceAndDatasourceTestSuite extends FunSuite with GlobalSapSQLContext {

  val testTableNameNameAge = "testTableNameAge"
  val testTableFourIntColumns = "testTableFourIntCols"

  private def createTestTableNameAge(sqlc: SQLContext) = {
    sqlc.sql(
      s"""CREATE TABLE $testTableNameNameAge (name string, age integer)
          |USING com.sap.spark.catalystSourceTest
          |OPTIONS ()""".stripMargin)
  }

  private def createTestTableFourIntColumns(sqlc: SQLContext) = {
    sqlc.sql(
      s"""CREATE TABLE $testTableFourIntColumns (a integer, b integer, c integer,
          |d integer, e integer)
          |USING com.sap.spark.catalystSourceTest
          |OPTIONS ()""".stripMargin)
  }

  test("Average pushdown"){

    createTestTableNameAge(sqlContext)

    CatalystSourceTestRDD.partitions =
      Seq(CataystSourceTestRDDPartition(0),CataystSourceTestRDDPartition(1))
    CatalystSourceTestRDD.rowData =
      Map(CataystSourceTestRDDPartition(0) -> Seq(Row("name1", 20.0, 10L), Row("name2", 10.0, 10L)),
        CataystSourceTestRDDPartition(1) -> Seq(Row("name1", 10.0, 10L), Row("name2", 20.0, 10L)))

    val result =
      sqlContext.sql(s"SELECT name, avg(age) FROM $testTableNameNameAge GROUP BY name").collect()

    assert(result.size == 2)
    assert(result.contains(Row("name1", 1.5)))
    assert(result.contains(Row("name2", 1.5)))
 }

  test("Nested query") {
    createTestTableFourIntColumns(sqlContext)

    CatalystSourceTestRDD.partitions =
      Seq(CataystSourceTestRDDPartition(0),CataystSourceTestRDDPartition(1))
    CatalystSourceTestRDD.rowData =
      Map(CataystSourceTestRDDPartition(0) -> Seq(Row(5), Row(5)),
        CataystSourceTestRDDPartition(1) -> Seq(Row(5), Row(1)))

    val result = sqlContext.sql(s"SELECT COUNT(*) FROM (SELECT e,sum(d) " +
      s"FROM ${testTableFourIntColumns} GROUP BY e) as A").collect()

    assert(result.size == 1)
    assert(result.contains(Row(2)))
  }
}

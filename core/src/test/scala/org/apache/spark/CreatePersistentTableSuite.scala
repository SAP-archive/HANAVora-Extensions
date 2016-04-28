package org.apache.spark

import com.sap.spark.dstest.{DefaultSource, DummyRelationWithTempFlag}
import com.sap.spark.util.TestUtils._
import org.apache.spark.sql.catalyst.plans.logical.Subquery
import org.apache.spark.sql.execution.datasources.{IsLogicalRelation, LogicalRelation, ResolvedDataSource}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{CatalogAccessor, GlobalSapSQLContext, Row}
import org.scalatest.{BeforeAndAfterEach, FunSuite, Inside}

/**
 * Tests the additional possibility of creating temporary / persistent tables
 */
class CreatePersistentTableSuite
  extends FunSuite
  with GlobalSapSQLContext
  with BeforeAndAfterEach
  with Inside {

  lazy val tableNotTemp = tableName("tableNotTemp")
  lazy val tableTemp = tableName("tableTemp")
  lazy val testTableNoSchema = tableName("testTableNoSchema")
  lazy val notExistingYet = tableName("notExistingYet")
  lazy val twiceTest = tableName("twiceTest")


  test("Create table keeps the table identifier") {
    sqlContext.sql(
      s"""CREATE TABLE $tableNotTemp (field string)
         |USING com.sap.spark.dstest
       """.stripMargin)

    val plan = CatalogAccessor(sqlContext).lookupRelation(Seq(tableNotTemp))
    inside(plan) {
      case Subquery(_,
        IsLogicalRelation(DummyRelationWithTempFlag(_, tableName, _, _))) =>
        assert(tableName == Seq(tableNotTemp))
    }
  }

  test("Create temporary table keeps the table identifier") {
    sqlContext.sql(
      s"""CREATE TEMPORARY TABLE $tableTemp (field string)
          |USING com.sap.spark.dstest
       """.stripMargin)

    val plan = CatalogAccessor(sqlContext).lookupRelation(Seq(tableTemp))
    inside(plan) {
      case Subquery(_,
      IsLogicalRelation(DummyRelationWithTempFlag(_, tableName, _, _))) =>
        assert(tableName == Seq(tableTemp))
    }
  }

  test("Create non-temporary table") {
    sqlContext.sql(s"""CREATE TABLE $tableNotTemp (field string)
                       |USING com.sap.spark.dstest
                       |OPTIONS ()""".stripMargin)


    sqlContext.sql(s"""CREATE TEMPORARY TABLE $tableTemp (field string)
                       |USING com.sap.spark.dstest
                       |OPTIONS ()""".stripMargin)

    // test with no schema
    sqlContext.sql(s"""CREATE TABLE $testTableNoSchema
                       |USING com.sap.spark.dstest
                       |OPTIONS ()""".stripMargin)

    val result = sqlContext.tables().collect()

    assert(result.contains(Row(tableTemp, true)))
    assert(result.contains(Row(tableNotTemp, false)))
    assert(result.contains(Row(testTableNoSchema, false)))
    assert(result.length == 3)
  }

  test("Create non existing table with if not exists flag") {
    sqlContext.sql(s"""CREATE TABLE IF NOT EXISTS $notExistingYet
                      |USING com.sap.spark.dstest
                      |OPTIONS ()""".stripMargin)

    val result = sqlContext.tables().collect()

    assert(result.contains(Row(notExistingYet, false)))
    assert(result.length == 1)
  }

  test("Create a table twice with if not exists flag") {
    sqlContext.sql(s"""CREATE TABLE IF NOT EXISTS $twiceTest
                      |USING com.sap.spark.dstest
                      |OPTIONS ()""".stripMargin)

    sqlContext.sql(s"""CREATE TABLE IF NOT EXISTS $twiceTest
                      |USING com.sap.spark.dstest
                      |OPTIONS ()""".stripMargin)

    val result = sqlContext.tables().collect()

    assert(result.contains(Row(twiceTest, false)))
  }

  test("Create a table twice without if not exists flag -- should fail") {
    sqlContext.sql(s"""CREATE TABLE $twiceTest
                       |USING com.sap.spark.dstest
                       |OPTIONS ()""".stripMargin)

    intercept[RuntimeException] {
      sqlContext.sql(s"""CREATE TABLE $twiceTest
                         |USING com.sap.spark.dstest
                         |OPTIONS ()""".stripMargin)
    }

    val result = sqlContext.tables().collect()

    assert(result.contains(Row(twiceTest, false)))
  }

  test(
    "Cannot create persistent table using a datasource that is not a TemporaryAndPersistentNature"
  ) {
    if (sqlc.isInstanceOf[HiveContext]) pending

    val path = getFileFromClassPath("/json")
    val tableName = this.tableName("tableTestPersistent")

    val ex = intercept[RuntimeException] {
      sqlc.sql(
        s"""
           |CREATE TABLE $tableName (name varchar(200), age integer)
           |USING org.apache.spark.sql.json
           |OPTIONS (
           |path "$path"
           |)""".stripMargin)
    }
    assert(ex.getMessage ==
      "Tables created with SQLContext must be TEMPORARY. Use a HiveContext instead.")
  }

  ignore(
    "Can create persistent table using a data source that is not a TemporaryAndPersistentNature") {
    if (!sqlc.isInstanceOf[HiveContext]) pending

    val path = getFileFromClassPath("/json")
    val tableName = this.tableName("tableTestPersistent")

    sqlc.sql(s"""
                |CREATE TABLE $tableName (name varchar(200), age integer)
                |USING org.apache.spark.sql.json
                |OPTIONS (
                |path "$path"
                |)""".stripMargin)
    assert(sqlc.tableNames().contains(tableName))
  }

  test("Can create temporary table using a data source that is not TemporaryAndPersistentNature") {

    val path = getFileFromClassPath("/json")
    val tableName = this.tableName("tableTestPersistent")

    sqlc.sql(
      s"""
         |CREATE TEMPORARY TABLE $tableName (name varchar(200), age integer)
         |USING org.apache.spark.sql.json
         |OPTIONS (
         |path "$path"
         |)""".stripMargin)
    assert(sqlc.tableNames().contains(tableName))
  }
}

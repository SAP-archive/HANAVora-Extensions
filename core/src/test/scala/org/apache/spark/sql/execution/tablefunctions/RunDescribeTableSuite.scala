package org.apache.spark.sql.execution.tablefunctions

import org.apache.spark.sql.execution.datasources.alterByCatalystSettings
import org.apache.spark.sql.hierarchy.HierarchyTestUtils
import org.apache.spark.sql.{GlobalSapSQLContext, Row}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class RunDescribeTableSuite
  extends FunSuite
  with GlobalSapSQLContext
  with HierarchyTestUtils
  with BeforeAndAfterEach {

  // scalastyle:off magic.number
  private val persons = Person("Jeff", 30) :: Person("Horst", 50) :: Nil
  // scalastyle:on magic.number
  private val pets = Pet(owner = "Jeff", "Doge") :: Nil

  override def beforeEach(): Unit = {
    super.beforeEach()
    sqlc.createDataFrame(persons).registerTempTable("persons")
    sqlc.createDataFrame(pets).registerTempTable("pets")
  }

  val numericInt = new {
    val precision = 32
    val radix = 2
    val scale = 0
  }

  val expectedDescribePersonsOutput =
    Set(
      List("", "persons", "name", "persons", "name",
        1, true, "VARCHAR(*)", null, null, null, null, null),
      List("", "persons", "age", "persons", "age",
        2, false, "INTEGER",
        numericInt.precision, numericInt.radix, numericInt.scale, null, null))

  test("Run describe table on in memory data") {
    val result = sqlc.sql("SELECT * FROM describe_table(SELECT * FROM persons)").collect()

    val values = result.map(_.toSeq.toList).toSet

    assert(values == expectedDescribePersonsOutput)
  }

  test("Run describe table on select with join") {

    val result = sqlc.sql("SELECT COLUMN_NAME, TABLE_NAME FROM describe_table(" +
      "SELECT persons.name, pets.petName FROM " +
      "persons INNER JOIN pets on persons.name=pets.owner)").collect()

    val values = result.map(_.toSeq.toList).toList
    val expected =
      ("name" :: "persons" :: Nil) ::
      ("petName" :: "pets" :: Nil) :: Nil
    assert(values == expected)
  }

  test("describe view on UNION works correctly") {
    testUnion()
  }

  test("describe view on UNION ALL works correctly") {
    testUnion(unique = false)
  }

  def testUnion(unique: Boolean = true): Unit = {
    sqlc.sql("CREATE VIEW v AS " +
      "SELECT name AS leftOwner@(foo = 'bar') FROM persons " +
      s"UNION ${if (!unique) "ALL" else ""} " +
      "SELECT name AS rightOwner@(baz = 'qux') FROM persons")

    val result = sqlc.sql("SELECT * FROM DESCRIBE_TABLE(SELECT * FROM v)").collect()
    val actual = result.map(_.toSeq.toList).toSet

    val expected =
      Set(List("", "v", "leftOwner", "persons", "name", 1,
        true, "VARCHAR(*)", null, null, null, "foo", "bar"))

    assertResult(expected)(actual)
  }

  test("describe_table should work on aggregates (Bug 110908)") {
    sqlc.sql(
      """CREATE TABLE sales (CUSTOMER_ID int, YEAH int, REVENUE int)
        |USING com.sap.spark.dstest""".stripMargin)

    sqlc.sql(
      """CREATE VIEW V1 AS SELECT YEAH @(Semantics.type = 'date'),
        |SUM(REVENUE), CUSTOMER_ID
        |FROM sales
        |GROUP BY CUSTOMER_ID
      """.stripMargin)

    val actual = sqlc.sql("SELECT * FROM describe_table(SELECT * FROM V1)").collect()

    // scalastyle:off magic.number
    val expected =
      Set(
        List("", alterByCatalystSettings(sqlc.catalog, "V1"), "YEAH",
          alterByCatalystSettings(sqlc.catalog, "sales"), "YEAH", 1, true,
          "INTEGER", 32, 2, 0, "Semantics.type", "date"),
        List("", alterByCatalystSettings(sqlc.catalog, "V1"), "_c1",
          alterByCatalystSettings(sqlc.catalog, "sales"), "REVENUE", 2, true,
          "BIGINT", 64, 2, 0, null, null),
        List("", alterByCatalystSettings(sqlc.catalog, "V1"), "CUSTOMER_ID",
          alterByCatalystSettings(sqlc.catalog, "sales"), "CUSTOMER_ID", 3, true,
          "INTEGER", 32, 2, 0, null, null))
    // scalastyle:on magic.number

    assertResult(expected)(actual.map(_.toSeq.toList).toSet)
  }

  test("describe hierarchy works correctly") {
    createAnimalsTable(sqlc)
    sqlc.sql(s"CREATE VIEW hv AS ${adjacencyListHierarchySQL(animalsTable, "name, node")}")

    val result = sqlc.sql("SELECT * FROM DESCRIBE_TABLE(SELECT name, node FROM hv)").collect()
    val actual = result.map(_.toSeq.toList).toSet

    val expected =
      Set(
        List("", "hv", "name", alterByCatalystSettings(sqlc.catalog, "animalsTbl"), "name",
          1, true, "VARCHAR(*)", null, null, null, null, null),
        List("", "hv", "node", "H", "node", 2, false, "<INTERNAL>", null, null, null, null, null))

    assertResult(expected)(actual)
  }

  test("Run describe table if exists on non existent table returns empty result") {
    val result = sqlc.sql("SELECT * FROM describe_table_if_exists(" +
      "SELECT * FROM nonexistent)").collect()

    assert(result.isEmpty)
  }

  test("Describe table if exists returns the same results as the regular function") {
    val (result1 :: result2 :: Nil) =
      "describe_table" :: "describe_table_if_exists" :: Nil map { functionName =>
        sqlc.sql(s"SELECT COLUMN_NAME, TABLE_NAME FROM $functionName(" +
          "SELECT persons.name, pets.petName FROM " +
          "persons INNER JOIN pets on persons.name=pets.owner)").collect().toSet
      }

    assert(result1 == result2)
  }

  test("Describe table if exists produced the correct output") {
    val result = sqlc.sql("SELECT * FROM describe_table_if_exists(SELECT * FROM persons)").collect()

    val values = result.map(_.toSeq.toList).toSet

    assert(values == expectedDescribePersonsOutput)
  }

  test("Column name is correctly backtracked (Bug 116133)") {
    val tpch = TPCHTables(sqlc)
    Seq(
      tpch.customerTable,
      tpch.nationTable,
      tpch.lineItemTable,
      tpch.ordersTable).foreach { table =>
      sqlc.baseRelationToDataFrame(table).registerTempTable(table.tableName)
    }

    sqlc.sql(
      """CREATE VIEW RASH_REVENUE_1995_2005 AS
       |SELECT C_NAME AS CustomerName@(EndUserText.heading = 'Customer Name'),
       |N_NAME AS NationName@(EndUserText.heading = 'Nation Name'), SS.NewDate , SS.L_SHIPDATE
       |FROM CUSTOMER
       |INNER JOIN NATION
       |ON CUSTOMER.C_NATIONKEY = NATION.N_NATIONKEY
       |INNER JOIN (
       |SELECT O_CUSTKEY, L_SHIPDATE, (LINEITEM.L_EXTENDEDPRICE * (1 - LINEITEM.L_DISCOUNT))
       |AS VOLUME, (add_years(LINEITEM.L_SHIPDATE,10)) AS NewDate
       |FROM ORDERS
       |INNER JOIN LINEITEM
       |ON ORDERS.O_ORDERKEY = LINEITEM.L_ORDERKEY) SS
       |ON CUSTOMER.C_CUSTKEY = SS.O_CUSTKEY
       |GROUP BY C_NAME, N_NAME, SS.NewDate, SS.L_SHIPDATE
       |HAVING SS.NewDate >= '2002-01-08'
       |ORDER BY SS.NewDate""".stripMargin)

    val values =
      sqlc.sql(
        """SELECT TABLE_NAME, COLUMN_NAME, ORIGINAL_TABLE_NAME, ORIGINAL_COLUMN_NAME
          |FROM describe_table(SELECT * FROM RASH_REVENUE_1995_2005)""".stripMargin)
        .collect()
        .toSet

    assertResult(
      Set(
        Row(alterByCatalystSettings(sqlc.catalog, "RASH_REVENUE_1995_2005"), "CustomerName",
          "CUSTOMER", "C_NAME"),
        Row(alterByCatalystSettings(sqlc.catalog, "RASH_REVENUE_1995_2005"), "NationName",
          "NATION", "N_NAME"),
        Row(alterByCatalystSettings(sqlc.catalog, "RASH_REVENUE_1995_2005"), "NewDate",
          "LINEITEM", "L_SHIPDATE"),
        Row(alterByCatalystSettings(sqlc.catalog, "RASH_REVENUE_1995_2005"), "L_SHIPDATE",
          "LINEITEM", "L_SHIPDATE")))(values)
  }
}

private case class Pet(owner: String, petName: String)
private case class Person(name: String, age: Int)

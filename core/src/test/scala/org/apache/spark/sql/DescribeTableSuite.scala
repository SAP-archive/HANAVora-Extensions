package org.apache.spark.sql

import org.scalatest.FunSuite

// scalastyle:off magic.number
class DescribeTableSuite extends FunSuite
  with GlobalSapSQLContext {

  private val expected = Seq(
    Row("A", 1, "INTEGER", "foo", "bar"),
    Row("B", 2, "INTEGER", "foo", "bar"),
    Row("B", 2, "INTEGER", "baz", "bla"),
    Row("C", 3, "INTEGER", "foo", "[bar,baz]"),
    Row("D", 4, "INTEGER", "foo", "?"),
    Row("G", 5, "INTEGER", "foo", "123"),
    Row("H", 6, "INTEGER", "foo", "1.23"))

  private val annotatedTableQuery = """CREATE TABLE testTable(
      A @ (foo = 'bar') int,
      B @ (foo = 'bar', baz = 'bla') int,
      C @ (foo = ('bar', 'baz')) int,
      D @ (foo = '?') int,
      G @ (foo = 123) int,
      H @ (foo = 1.23) int)
      USING com.sap.spark.dstest
      OPTIONS ()"""

  private val annotatedTable2Query = """CREATE TABLE testTable2(
      A @ (foo = 'bar') int,
      Z @ (bla = 'blabla') int)
      USING com.sap.spark.dstest
      OPTIONS ()"""

  private val tableQuery = """CREATE TABLE testTable2
      A INT, B INT
      USING com.sap.spark.dstest
      OPTIONS()"""

  def describe(query: String): Array[Row] =
    sqlContext.sql(
      s"""SELECT COLUMN_NAME, ORDINAL_POSITION,
          |DATA_TYPE, ANNOTATION_KEY, ANNOTATION_VALUE
          |FROM describe_table($query)""".stripMargin).collect()

  test("get table annotations via table name") {
    sqlContext.sql(annotatedTableQuery)
    val actual = describe("SELECT * FROM testTable")
    assertResult(expected.toSet) (actual.toSet)
  }

  test("get table annotations via query") {
    sqlContext.sql(annotatedTableQuery)
    val actual = describe("SELECT * @(*=?) FROM testTable")
    assertResult(expected.toSet)(actual.toSet)
  }

  // scalastyle:off magic.number
  test("get subset of annotated attributes") {
    sqlContext.sql(annotatedTableQuery)
    val actual = describe("SELECT A@(*=?), B@(*=?), C@(*=?) FROM testTable")
    assertResult(expected.take(4).toSet)(actual.toSet)
  }

  test("get annotation via aliases") {
    sqlContext.sql(annotatedTableQuery)
    val actual = describe("SELECT A AS AL @(foo='override') FROM testTable")
    assertResult(Set(Row("AL", 1, "INTEGER", "foo", "override")))(actual.toSet)
  }

  test("get meta data without annotations") {
    sqlContext.sql(annotatedTableQuery)
    val actual = describe("SELECT A, B AS BA FROM testTable")
    assertResult(Set(
      Row("A", 1, "INTEGER", "foo", "bar"),
      Row("BA", 2, "INTEGER", "foo", "bar"),
      Row("BA", 2, "INTEGER", "baz", "bla")))(actual.toSet)
  }

  test("get all meta data without annotations") {
    sqlContext.sql(annotatedTableQuery)
    val actual = describe("SELECT * FROM testTable")
    assertResult(expected.toSet)(actual.toSet)
  }

  test("get annotations of a complex query") {
    sqlContext.sql(annotatedTableQuery)
    sqlContext.sql(annotatedTable2Query)
    val actual = describe("SELECT X.AA @(foo=?), X.BB @(baz=?), Y.Z @(*=?)" +
      "FROM (SELECT A AS AA, B AS BB FROM testTable) X, testTable2 Y")
    assertResult(Set(
      Row("AA", 1, "INTEGER", "foo", "bar"),
      Row("BB", 2, "INTEGER", "baz", "bla"),
      Row("Z", 3, "INTEGER", "bla", "blabla")))(actual.toSet)
  }
}

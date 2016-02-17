package org.apache.spark.sql.catalyst.expressions.tablefunctions

import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, GlobalSapSQLContext, SapSqlParser}
import org.scalatest.FunSuite

class RunDescribeTableSuite
  extends FunSuite
  with GlobalSapSQLContext {

  // scalastyle:ignore magic.number
  private val persons = Person("Jeff", 30) :: Person("Horst", 50) :: Nil
  private val pets = Pet(owner = "Jeff", "Doge") :: Nil

  test("Run describe table on in memory data") {
    sqlc.createDataFrame(persons).registerTempTable("persons")

    val result = sqlc.sql("SELECT * FROM describe_table(SELECT * FROM persons)").collect()

    val values = result.map(_.toSeq.toList).toList
    val expected =
      List(
        List(
          "", "root\n |-- name: string (nullable = true)\n |-- age: integer (nullable = false)\n",
          "persons", "name", 0, "", "true", "string", 0, 0, 0, 0, 0, 0, "", "",
          "", "", "", ""),
        List(
          "", "root\n |-- name: string (nullable = true)\n |-- age: integer (nullable = false)\n",
          "persons", "age", 0, "", "false", "integer", 0, 0, 0, 0, 0, 0, "", "",
          "", "", "", ""))

    assert(values == expected)
  }

  test("Run describe table on select with join") {
    sqlc.createDataFrame(persons).registerTempTable("persons")
    sqlc.createDataFrame(pets).registerTempTable("pets")

    val result = sqlc.sql("SELECT COLUMN_NAME, TABLE_NAME FROM describe_table(" +
      "SELECT persons.name, pets.petName FROM " +
      "persons INNER JOIN pets on persons.name=pets.owner)").collect()

    val values = result.map(_.toSeq.toList).toList
    val expected =
      ("name" :: "Inner JOIN ON persons, pets" :: Nil) ::
      ("petName" :: "Inner JOIN ON persons, pets" :: Nil) :: Nil
    assert(values == expected)
  }
}

private case class Pet(owner: String, petName: String)
private case class Person(name: String, age: Int)

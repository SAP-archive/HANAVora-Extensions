package org.apache.spark.sql.execution.tablefunctions

import org.apache.spark.sql.GlobalSapSQLContext
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class RunDescribeTableSuite
  extends FunSuite
  with GlobalSapSQLContext
  with BeforeAndAfterEach {

  // scalastyle:ignore magic.number
  private val persons = Person("Jeff", 30) :: Person("Horst", 50) :: Nil
  private val pets = Pet(owner = "Jeff", "Doge") :: Nil

  override def beforeEach(): Unit = {
    super.beforeEach()
    sqlc.createDataFrame(persons).registerTempTable("persons")
    sqlc.createDataFrame(pets).registerTempTable("pets")
  }

  test("Run describe table on in memory data") {
    val result = sqlc.sql("SELECT * FROM describe_table(SELECT * FROM persons)").collect()

    val values = result.map(_.toSeq.toList).toSet
    val expected =
      Set(
        List("", "persons", "name", 0, true, "VARCHAR(*)", null, null, "", ""),
        List("", "persons", "age", 1, false, "INTEGER", null, null, "", ""))

    assert(values == expected)
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
}

private case class Pet(owner: String, petName: String)
private case class Person(name: String, age: Int)

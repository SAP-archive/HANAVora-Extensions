package org.apache.spark.sql.execution.tablefunctions

import org.apache.spark.sql.GlobalSapSQLContext
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class RunDescribeTableSuite
  extends FunSuite
  with GlobalSapSQLContext
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
      List("", "persons", "name", 1, true, "VARCHAR(*)", null, null, null, null, null),
      List("", "persons", "age", 2, false, "INTEGER",
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
}

private case class Pet(owner: String, petName: String)
private case class Person(name: String, age: Int)

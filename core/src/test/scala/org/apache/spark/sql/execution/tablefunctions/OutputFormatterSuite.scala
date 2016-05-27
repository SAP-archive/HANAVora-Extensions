package org.apache.spark.sql.execution.tablefunctions

import org.scalatest.FunSuite

class OutputFormatterSuite extends FunSuite {
  // scalastyle:off magic.number
  test("Formatting a value will yield a sequence with a sequence with the single value") {
    val formatter = new OutputFormatter(1)
    assertResult(Seq(Seq(1)))(formatter.format())
  }

  test("Formatting with two values will yield a sequence with a sequence of the two values") {
    val formatter = new OutputFormatter(1, 2)
    assertResult(Seq(Seq(1, 2)))(formatter.format())
  }

  test("Formatting with a list and a single value will append the single value to all rows") {
    val formatter = new OutputFormatter(Seq(1, 2), 3)
    assertResult(Seq(Seq(1, 3), Seq(2, 3)))(formatter.format())
  }

  test("Formatting with a list and another list will 'multiply' the lists") {
    val formatter = new OutputFormatter(Seq(1, 2), Seq(2, 3))
    assertResult(Seq(Seq(1, 2), Seq(2, 2), Seq(1, 3), Seq(2, 3)))(formatter.format())
  }

  test("Formatting a list will output a sequence with sequences with the individual values") {
    val formatter = new OutputFormatter(Seq(1, 2, 3))
    assertResult(Seq(Seq(1), Seq(2), Seq(3)))(formatter.format())
  }

  test("Formatting a single map will yield rows with the key-value pairs") {
    val formatter = new OutputFormatter(Map(1 -> 2, 2 -> 3))
    assertResult(Seq(Seq(1, 2), Seq(2, 3)))(formatter.format())
  }

  test("Formatting with a list and a map will 'multiply' the rows with the key-value-pairs") {
    val formatter = new OutputFormatter(Seq(1, 2), Map(1 -> 2, 3 -> 4))
    assertResult(Seq(Seq(1, 1, 2), Seq(2, 1, 2), Seq(1, 3, 4), Seq(2, 3, 4)))(formatter.format())
  }
  // scalastyle:on magic.number
}

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.unsafe.types.compat._
import org.apache.spark.sql.types.compat._
import org.scalatest.FunSuite

class FixedIfSuite extends FunSuite with Logging {

  test("Check IF statement") {
    val a = 'a.string.at(0)
    val ifExp = FixedIf(EqualTo(a,
      Literal("STRING")), Literal("EQUALS"), Literal("DIFFERENT"))

    assert(ifExp.dataType == StringType)
    assertResult("DIFFERENT")(ifExp.eval(Row(UTF8String.fromString("R"))))
    assertResult("EQUALS")(ifExp.eval(Row(UTF8String.fromString("STRING"))))
  }

  test("Check IF statement with Null values") {
    val a = 'a.string.at(0)
    val ifExp = FixedIf(EqualTo(a,
      Literal("STRING")), Literal("EQUALS"), Literal(null))

    assert(ifExp.dataType == StringType)
    val falseResult = ifExp.eval(Row("R"))
    val trueResult = ifExp.eval(Row(UTF8String.fromString("STRING")))

    assertResult(null)(falseResult)
    assertResult("EQUALS")(trueResult)
  }

  test("Check IF statement with different value types") {
    val a = 'a.string.at(0)
    val ifExp = FixedIf(EqualTo(a,
      Literal("STRING")), Literal("EQUALS"), Literal(1))

    intercept[Exception](ifExp.dataType)
  }

}

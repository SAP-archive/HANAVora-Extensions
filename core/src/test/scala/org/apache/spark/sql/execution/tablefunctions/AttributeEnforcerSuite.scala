package org.apache.spark.sql.execution.tablefunctions

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types.{StringType, BooleanType, IntegerType, DataType}
import org.scalatest.FunSuite

class AttributeEnforcerSuite extends FunSuite {
  def testEnforce(dataType: DataType, nullable: Boolean, value: Any)
                 (out: => Any = None): Unit = {
    val attribute = AttributeReference("foo", dataType, nullable = nullable)()
    val enforcer = new AttributeEnforcer(attribute)

    assert(enforcer.enforce(value) == out)
  }

  def testEnforceNonNullable(dataType: DataType, value: Any)(out: => Any = ()): Unit = {
    testEnforce(dataType, nullable = false, value)(out)
  }

  test("enforce int types") {
    testEnforceNonNullable(IntegerType, 1)(1)
    intercept[AssertionError](testEnforceNonNullable(IntegerType, "test")())
  }

  test("enforce bool types") {
    testEnforceNonNullable(BooleanType, true)(true)
    intercept[AssertionError](testEnforceNonNullable(BooleanType, "test")())
  }

  test("enforce string types") {
    testEnforceNonNullable(StringType, "test")("test")
    testEnforceNonNullable(StringType, 1)("1")
  }

  test("nullable enforce") {
    intercept[AssertionError](testEnforceNonNullable(IntegerType, null)())
    intercept[AssertionError](testEnforceNonNullable(IntegerType, None)())
    testEnforce(IntegerType, nullable = true, null)(null)
    testEnforce(IntegerType, nullable = true, null)(null)
  }
}

package org.apache.spark.sql.execution.tablefunctions

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types.IntegerType
import org.scalatest.FunSuite

class SchemaEnforcerSuite extends FunSuite {
  val a1 = AttributeReference("foo", IntegerType, nullable = false)()
  val a2 = AttributeReference("bar", IntegerType, nullable = true)()
  val enforcer = new SchemaEnforcer(Seq(a1, a2))

  test("successful schema enforce") {
    assert(enforcer.enforce(Seq(Seq(1, 2))) == Seq(Seq(1, 2)))
    assert(enforcer.enforce(Seq(Seq(1, null))) == Seq(Seq(1, null)))
  }

  test("schema enforce fail") {
    intercept[AssertionError](enforcer.enforce(Seq(Seq(null, 1))))
    intercept[AssertionError](enforcer.enforce(Seq(Seq(null, 1))))
  }
}

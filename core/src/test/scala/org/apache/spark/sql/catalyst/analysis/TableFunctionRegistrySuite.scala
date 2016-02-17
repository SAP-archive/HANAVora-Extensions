package org.apache.spark.sql.catalyst.analysis

import org.mockito.Mockito
import org.scalatest.FunSuite

class TableFunctionRegistrySuite extends FunSuite {
  val tf1 = Mockito.mock(classOf[TableFunction])
  val tf2 = Mockito.mock(classOf[TableFunction])

  test("lookup of table functions") {
    val registry = new SimpleTableFunctionRegistry
    registry.registerFunction("test", tf1)

    assert(registry.lookupFunction("test") == Some(tf1))
    assert(registry.lookupFunction("Test") == Some(tf1))
    assert(registry.lookupFunction("TEST") == Some(tf1))
    assert(registry.lookupFunction("notExisting").isEmpty)
  }

  test("overriding of table functions") {
    val registry = new SimpleTableFunctionRegistry
    registry.registerFunction("test", tf1)
    assert(registry.lookupFunction("test") == Some(tf1))
    registry.registerFunction("test", tf2)

    assert(registry.lookupFunction("test") == Some(tf2))
  }
}

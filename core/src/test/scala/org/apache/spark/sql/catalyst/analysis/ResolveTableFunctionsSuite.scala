package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.tablefunctions.UnresolvedTableFunction
import org.mockito.Mockito._
import org.scalatest.FunSuite

class ResolveTableFunctionsSuite extends FunSuite {

  test("resolution of previously registered function") {
    val tf = mock(classOf[TableFunction])
    val analyzer = mock(classOf[Analyzer])
    val registry = new SimpleTableFunctionRegistry()
    val strategy = ResolveTableFunctions(analyzer, registry)
    registry.registerFunction("foo", tf)
    val unresolved = UnresolvedTableFunction("foo", Seq.empty)
    when(tf.analyze(analyzer, Seq.empty)) thenReturn Seq.empty

    val resolved = strategy.apply(unresolved)

    assert(resolved == ResolvedTableFunction(tf, Seq.empty))
    verify(tf).analyze(analyzer, Seq.empty)
  }

  test("fail on unregistered functions") {
    val analyzer = mock(classOf[Analyzer])
    val strategy = ResolveTableFunctions(analyzer)
    val unresolved = UnresolvedTableFunction("foo", Seq.empty)

    intercept[AnalysisException] {
      strategy(unresolved)
    }
  }
}

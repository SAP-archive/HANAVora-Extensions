package org.apache.spark.sql.execution.tablefunctions

import org.apache.spark.sql.catalyst.analysis.{ResolvedTableFunction, TableFunction}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.extension.ExtendedPlanner
import org.mockito.Mockito._
import org.scalatest.FunSuite

class TableFunctionsStrategySuite extends FunSuite {
  val planner = mock(classOf[ExtendedPlanner])
  val strategy = TableFunctionsStrategy(planner)

  test("Execution of the table function strategy") {
    val arguments = mock(classOf[LogicalPlan])
    val tableFunction = mock(classOf[TableFunction])
    val resolved = ResolvedTableFunction(tableFunction, Seq(arguments))
    when(tableFunction(planner)(Seq(arguments))) thenReturn Seq.empty

    assert(strategy.apply(resolved) == Seq.empty)

    verify(tableFunction)(planner)(Seq(arguments))
  }

  test("strategy ignores other plans") {
    assert(strategy.apply(mock(classOf[LogicalPlan])) == Nil)
  }
}

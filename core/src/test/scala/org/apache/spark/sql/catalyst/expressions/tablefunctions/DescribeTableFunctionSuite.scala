package org.apache.spark.sql.catalyst.expressions.tablefunctions

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.tablefunctions.RunDescribeTable
import org.apache.spark.sql.extension.ExtendedPlanner
import org.scalatest.FunSuite
import org.mockito.Mockito._

class DescribeTableFunctionSuite extends FunSuite {
  val f = new DescribeTableFunction
  val planner = mock(classOf[ExtendedPlanner])
  val bound = f(planner) _
  val plan = mock(classOf[LogicalPlan])

  test("correct number of arguments given") {
    assert(bound(plan :: Nil) == RunDescribeTable(plan) :: Nil)
  }

  test("wrong number of arguments given") {
    intercept[IllegalArgumentException] {
      bound(plan :: plan :: Nil)
    }
  }
}

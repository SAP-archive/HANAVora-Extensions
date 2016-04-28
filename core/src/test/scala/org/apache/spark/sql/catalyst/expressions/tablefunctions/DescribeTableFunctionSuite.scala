package org.apache.spark.sql.catalyst.expressions.tablefunctions

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.extension.ExtendedPlanner
import org.mockito.Mockito._
import org.scalatest.FunSuite

class DescribeTableFunctionSuite extends FunSuite {
  val f = new DescribeTableFunction
  val planner = mock(classOf[ExtendedPlanner])
  val bound = f(planner) _
  val plan = mock(classOf[LogicalPlan])

  test("wrong number of arguments given") {
    intercept[IllegalArgumentException] {
      bound(plan :: plan :: Nil)
    }
  }
}

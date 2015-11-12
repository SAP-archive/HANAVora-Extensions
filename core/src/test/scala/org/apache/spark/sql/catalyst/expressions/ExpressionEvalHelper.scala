package org.apache.spark.sql.catalyst.expressions

//
// Partially backported from Spark 1.5.2.
//

import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.compat.InternalRow
import org.apache.spark.sql.catalyst.optimizer.DefaultOptimizer
import org.apache.spark.sql.catalyst.plans.logical.{OneRowRelation, Project}
import org.scalactic.TripleEqualsSupport.Spread
import org.scalatest.FunSuite
import org.scalatest.prop.GeneratorDrivenPropertyChecks

// scalastyle:off

/**
  * A few helper functions for expression evaluation testing. Mixin this trait to use them.
  */
trait ExpressionEvalHelper extends GeneratorDrivenPropertyChecks {
  self: FunSuite =>

  protected def create_row(values: Any*): InternalRow = {
    InternalRow.fromSeq(values.map(CatalystTypeConverters.convertToCatalyst))
  }

  protected def checkEvaluation(
                                 expression: => Expression, expected: Any, inputRow: InternalRow = EmptyRow): Unit = {
    val catalystValue = CatalystTypeConverters.convertToCatalyst(expected)
    checkEvaluationWithoutCodegen(expression, catalystValue, inputRow)
    checkEvaluationWithOptimization(expression, catalystValue, inputRow)
  }

  /**
    * Check the equality between result of expression and expected value, it will handle
    * Array[Byte] and Spread[Double].
    */
  protected def checkResult(result: Any, expected: Any): Boolean = {
    (result, expected) match {
      case (result: Array[Byte], expected: Array[Byte]) =>
        java.util.Arrays.equals(result, expected)
      case (result: Double, expected: Spread[Double]) =>
        expected.isWithin(result)
      case _ => result == expected
    }
  }

  protected def evaluate(expression: Expression, inputRow: InternalRow = EmptyRow): Any = {
    expression.foreach {
      /* XXX: case n: Nondeterministic => n.setInitialValues() */
      case _ =>
    }
    expression.eval(inputRow)
  }

  protected def generateProject(
                                 generator: => Projection,
                                 expression: Expression): Projection = {
    try {
      generator
    } catch {
      case e: Throwable =>
        fail(
          s"""
             |Code generation of $expression failed:
             |$e
             |${e.getStackTraceString}
          """.stripMargin)
    }
  }

  protected def checkEvaluationWithoutCodegen(
                                               expression: Expression,
                                               expected: Any,
                                               inputRow: InternalRow = EmptyRow): Unit = {

    val actual = try evaluate(expression, inputRow) catch {
      case e: Exception => fail(s"Exception evaluating $expression", e)
    }
    if (!checkResult(actual, expected)) {
      val input = if (inputRow == EmptyRow) "" else s", input: $inputRow"
      fail(s"Incorrect evaluation (codegen off): $expression, " +
        s"actual: $actual, " +
        s"expected: $expected$input")
    }
  }

  protected def checkEvaluationWithOptimization(
                                                 expression: Expression,
                                                 expected: Any,
                                                 inputRow: InternalRow = EmptyRow): Unit = {
    val plan = Project(Alias(expression, s"Optimized($expression)")() :: Nil, OneRowRelation)
    val optimizedPlan = DefaultOptimizer.execute(plan)
    checkEvaluationWithoutCodegen(optimizedPlan.expressions.head, expected, inputRow)
  }

}

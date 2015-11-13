package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.analysis.compat._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.compat._
import org.apache.spark.sql.types.compat._

/**
 * Register custom functions in a function registry.
 */
object RegisterCustomFunctions {

  def apply(registry: FunctionRegistry): Unit = {
    registry.registerExpression[Remainder]("remainder")
    registry.registerExpression[Remainder]("mod")

    registry.registerExpression[AddYears]("add_years")
    registry.registerExpression[DateAdd]("add_days")
    registry.registerExpression[Replace]("replace")
    registry.registerExpression[Log]("ln")
    registry.registerFunction("to_double", toDoubleBuilder)
    registry.registerFunction("to_integer", toIntegerBuilder)
    registry.registerFunction("to_varchar", toVarcharBuilder)
    registry.registerFunction("rand", randBuilder)
    registry.registerFunction("days_between", daysBetweenBuilder)
  }

  private def toDoubleBuilder(expressions: Seq[Expression]): Expression =
    expressions match {
      case Seq(exp) => Cast(exp, DoubleType)
      case _ =>
        throw new AnalysisException("Input argument to TO_DOUBLE must be a single expression")
    }

  private def toIntegerBuilder(expressions: Seq[Expression]): Expression =
    expressions match {
      case Seq(exp) => Cast(exp, IntegerType)
      case _ =>
        throw new AnalysisException("Input argument to TO_INTEGER must be a single expression")
    }

  private def toVarcharBuilder(expressions: Seq[Expression]): Expression =
    expressions match {
      case Seq(exp) => Cast(exp, StringType)
      case _ =>
        throw new AnalysisException("Input argument to TO_VARCHAR must be a single expression")
    }

  private def randBuilder(expressions: Seq[Expression]): Expression =
    expressions match {
      case Nil => new Rand()
      case Seq(IntegerLiteral(n)) => new Rand(n)
      case _ => throw new AnalysisException("Input argument to RAND must be an integer literal.")
    }

  private def daysBetweenBuilder(expressions: Seq[Expression]): Expression =
    expressions match {
      case Seq(exp1, exp2) => Abs(DateDiff(exp1, exp2))
      case _ => throw new AnalysisException("Input argument to DAYS_BETWEEN must two expressions.")
    }

}

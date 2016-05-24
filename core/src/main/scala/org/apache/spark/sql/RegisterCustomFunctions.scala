package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.currency.CurrencyConversionFunction
import org.apache.spark.sql.types._
import scala.reflect.ClassTag

/**
 * Register custom functions in a function registry.
 */
object RegisterCustomFunctions {

  /**
   * Utility methods that registers an expression in the given registry by extracting its
   * builder from the registry and use it for registering the expression.
   *
   * @param registry The registry.
   * @param name The name of the expression.
   * @param tag The class tag of the expression.
   * @tparam T The Ttype of the expression.
   */
  // TODO move this to an implicit function in the registry.
  private[this] def registerExpression[T <: Expression](registry: FunctionRegistry, name: String)
                                         (implicit tag: ClassTag[T]): Unit = {
    val (_, (_, builder)) = expression[T](name)
    registry.registerFunction(name, builder)
  }

  def apply(registry: FunctionRegistry): Unit = {
    registerExpression[Remainder](registry, "remainder")
    registerExpression[Remainder](registry, "mod")
    registerExpression[AddYears](registry, "add_years")
    registerExpression[AddSeconds](registry, "add_seconds")
    registerExpression[DateAdd](registry, "add_days")
    registerExpression[Replace](registry, "replace")
    registerExpression[Log](registry, "ln")
    registry.registerFunction("to_double", toDoubleBuilder)
    registry.registerFunction("to_integer", toIntegerBuilder)
    registry.registerFunction("to_varchar", toVarcharBuilder)
    registry.registerFunction("rand", randBuilder)
    registry.registerFunction("days_between", daysBetweenBuilder)

    // register all currency conversions
    CurrencyConversionFunction.functions.foreach {
      case (name, impl) => registry.registerFunction(name, impl.getExpression)

    }
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

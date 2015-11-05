package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions.{IntegerLiteral, Rand, Expression}

/**
 * Register custom functions in a function registry.
 */
object RegisterCustomFunctions {
  def apply(registry: FunctionRegistry): Unit = {
    registry.registerFunction("rand", (expressions: Seq[Expression]) => expressions match {
      case Nil => new Rand()
      case Seq(IntegerLiteral(n)) => new Rand(n)
      case _ => throw new AnalysisException("Input argument to rand must be an integer literal.")
    })
  }
}

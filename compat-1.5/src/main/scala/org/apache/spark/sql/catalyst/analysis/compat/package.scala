package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.CatalystConf
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.expression
import org.apache.spark.sql.catalyst.expressions.{NamedExpression, Expression, ExpressionInfo}

import scala.reflect.ClassTag

package object compat {
  val BackportedUnresolvedFunction = org.apache.spark.sql.catalyst.analysis.UnresolvedFunction

  def unresolvedAliases(children: Expression*): Seq[NamedExpression] =
    children.map({
      case child => UnresolvedAlias(child)
    })

  def newSimpleFunctionRegistry(conf: CatalystConf): SimpleFunctionRegistry =
    new SimpleFunctionRegistry

  implicit class FunctionRegistryCompatOps(registry: FunctionRegistry) {

    def registerBuiltins(): Unit =
      expressions.foreach {
        case (name, (info, builder)) => registry.registerFunction(name, builder)
      }

    def registerExpression[T <: Expression](name: String)
                                           (implicit tag: ClassTag[T]): Unit = {
      val (_, (_, builder)) = expression[T](name)
      registry.registerFunction(name, builder)
    }
  }

  type FunctionBuilder = Seq[Expression] => Expression

  private val expressions: Map[String, (ExpressionInfo, FunctionBuilder)] =
    FunctionRegistry.expressions
}

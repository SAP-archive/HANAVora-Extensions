package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.{Expression, BinaryExpression, UnaryExpression}

import scala.reflect.ClassTag

object FunctionBuilders {

  type ExpressionBuilder = Seq[Expression] => Expression

  def expression[T <: Expression](arity: Int)(implicit tag: ClassTag[T]): ExpressionBuilder = {
    val argTypes = (1 to arity).map(x => classOf[Expression])
    val constructor = tag.runtimeClass.getDeclaredConstructor(argTypes: _*)
    (expressions: Seq[Expression]) => {
      if (expressions.size != arity) {
        throw new IllegalArgumentException(
          s"Invalid number of arguments: ${expressions.size} (must be equal to $arity)"
        )
      }
      constructor.newInstance(expressions: _*).asInstanceOf[Expression]
    }
  }

  def unaryExpression[T <: UnaryExpression](implicit tag: ClassTag[T]): ExpressionBuilder =
    expression[T](1)

  def binaryExpression[T <: BinaryExpression](implicit tag: ClassTag[T]): ExpressionBuilder =
    expression[T](2)

  def reverse(expressionBuilder: ExpressionBuilder): ExpressionBuilder =
    (expressions: Seq[Expression]) => {
      expressionBuilder(expressions.reverse)
    }

}

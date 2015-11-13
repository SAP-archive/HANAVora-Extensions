package org.apache.spark.sql.catalyst.expressions.compat

import org.apache.spark.sql.catalyst.compat.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.compat._

//
// Traits to backport expressions from Spark 1.5.x to 1.4.x
//



private[expressions] trait BackportedExpectsInputTypes extends ExpectsInputTypes {

  def inputTypes: Seq[AbstractDataType]

  override lazy val expectedChildTypes: Seq[DataType] = inputTypes.map(_.defaultConcreteType)
}

private[expressions] trait ImplicitCastInputTypes extends BackportedExpectsInputTypes

private[expressions] trait BackportedExpression extends Expression {
  self: Product =>

  override type EvaluatedType = Any

  def prettyName: String = getClass.getSimpleName.toLowerCase

}

private[expressions] trait BackportedLeafExpression
  extends LeafExpression
  with BackportedExpression {
  self: Product =>

}

private[expressions] trait BackportedUnaryExpression
  extends UnaryExpression
  with BackportedExpression {
  self: Product =>

  override def nullable: Boolean = child.nullable

  override def eval(input: InternalRow): Any = {
    val value = child.eval(input)
    if (value == null) {
      null
    } else {
      nullSafeEval(value)
    }
  }

  protected def nullSafeEval(input: Any): Any =
    sys.error(s"UnaryExpressions must override either eval or nullSafeEval")

}

private[expressions] trait BackportedBinaryExpression
  extends BinaryExpression
  with BackportedExpression {
  self: Product =>

  override def nullable: Boolean = children.exists(_.nullable)

  def symbol: String = prettyName

  override def eval(input: InternalRow): Any = {
    val value1 = left.eval(input)
    if (value1 == null) {
      null
    } else {
      val value2 = right.eval(input)
      if (value2 == null) {
        null
      } else {
        nullSafeEval(value1, value2)
      }
    }
  }

  protected def nullSafeEval(input1: Any, input2: Any): Any =
    sys.error(s"BinaryExpressions must override either eval or nullSafeEval")

}

private[expressions] trait BackportedTernaryExpression
  extends Expression
  with BackportedExpression {
  self: Product =>

  override def nullable: Boolean = children.exists(_.nullable)

  def symbol: String = prettyName

  override def eval(input: InternalRow): Any = {
    val exprs = children
    val value1 = exprs(0).eval(input)
    if (value1 != null) {
      val value2 = exprs(1).eval(input)
      if (value2 != null) {
        val value3 = exprs(2).eval(input)
        if (value3 != null) {
          return nullSafeEval(value1, value2, value3) // scalastyle:ignore
        }
      }
    }
    null
  }

  protected def nullSafeEval(input1: Any, input2: Any, input3: Any): Any =
    sys.error(s"BinaryExpressions must override either eval or nullSafeEval")

}

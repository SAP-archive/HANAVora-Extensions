package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.types.compat._

/* TODO remove this expression when this error is closed:
https://issues.apache.org/jira/browse/SPARK-10485 */
/**
 * Fixed "If" expression that handles nulls correctly
 *
 * @param predicate
 * @param trueValue
 * @param falseValue
 */
case class FixedIf(predicate: Expression, trueValue: Expression, falseValue: Expression)
  extends Expression {

  override def children: Seq[Expression] = predicate :: trueValue :: falseValue :: Nil

  override def nullable: Boolean = trueValue.nullable || falseValue.nullable

  // scalastyle:off cyclomatic.complexity
  override def dataType: DataType = {
    (trueValue.dataType, falseValue.dataType) match {
      case (t, f) if t == NullType && f == NullType => NullType
      case (t, f) if t == NullType => f
      case (t, f) if f == NullType => t
      case (t, f) if t == f => t
      case (t, f) => throw new TreeNodeException(this,
        s"Types do not match $t != $f")
    }
  }

  type EvaluatedType = Any

  override def eval(input: Row): Any = {
    if (true == predicate.eval(input)) {
      trueValue.eval(input)
    } else {
      falseValue.eval(input)
    }
  }

  override def toString(): String = s"if ($predicate) $trueValue else $falseValue"
}


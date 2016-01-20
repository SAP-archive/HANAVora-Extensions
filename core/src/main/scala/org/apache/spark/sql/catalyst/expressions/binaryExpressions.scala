package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.types._

import scala.annotation.tailrec

/**
  * Matches a [[BinaryComparison]] between an [[AttributeReference]]
  * and a numeric [[Literal]].
  *
  * @see [[NumericLiteral]]
  */
private[sql] object BinaryComparisonWithNumericLiteral {
  type ReturnType = (AttributeReference, BigDecimal)

  def unapply(exp: BinaryComparison): Option[ReturnType] = (exp.left, exp.right) match {
    case (MaybeCast(attr: AttributeReference), MaybeCast(NumericLiteral(value))) =>
      Some(attr, value)
    case (MaybeCast(NumericLiteral(value)), MaybeCast(attr: AttributeReference)) =>
      Some(attr, value)
    case _ => None
  }
}

/**
  * Matches an expression, ignoring any wrapping [[Cast]].
  */
private[sql] object MaybeCast {
  @tailrec
  def unapply(any: Any): Option[Expression] =
    any match {
      case Cast(exp, _) => unapply(exp)
      case exp: Expression => Some(exp)
      case _ => None
    }
}

/**
  * Matches numeric [[Literal]] and returns its value as
  * [[BigDecimal]].
  */
private[sql] object NumericLiteral {
  def unapply(a: Any): Option[BigDecimal] = a match {
    case Literal(value: Int, _) => Some(value)
    case Literal(value: Long, _) => Some(value)
    case Literal(value: Double, _) => Some(value)
    case Literal(value: Float, _) => Some(value.toDouble)
    case Literal(value: Short, _) => Some(value.toInt)
    case Literal(value: Decimal, _) => Some(value.toBigDecimal)
    case Literal(value: java.math.BigDecimal, _) => Some(value)
    case Literal(value: BigDecimal, _) => Some(value)
    case _ => None
  }
}

// TODO optimize this. maybe we can substitute it completely with its logic.
object BinarySymbolExpression {
  def isBinaryExpressionWithSymbol(be: BinaryExpression): Boolean =
    be.isInstanceOf[BinaryArithmetic] || be.isInstanceOf[BinaryComparison]

  def getBinaryExpressionSymbol(be: BinaryExpression): String =
    be match {
      case be: BinaryComparison => be.symbol
      case be:BinaryArithmetic => be.symbol
      case _ => sys.error(s"${be.getClass.getName} has no symbol attribute")
    }

  def unapply(any: Any): Option[(Expression, String, Expression)] = any match {
    case be: BinaryExpression if isBinaryExpressionWithSymbol(be) =>
      Some(be.left, getBinaryExpressionSymbol(be), be.right)
    case _ => None
  }
}

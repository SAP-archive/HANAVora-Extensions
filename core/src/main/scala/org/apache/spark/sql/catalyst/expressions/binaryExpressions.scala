package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.types.Decimal

private[sql] object BinaryComparisonWithNumericLiteral {
  type ReturnType = (AttributeReference, BigDecimal)

  def unapply(exp: BinaryComparison): Option[ReturnType] = (exp.left, exp.right) match {
    case (attr: AttributeReference, literal@NumericLiteral(value)) => Some(attr, value)
    case (literal@NumericLiteral(value), attr: AttributeReference) => Some(attr, value)
    case (Cast(attr: AttributeReference, _), literal@NumericLiteral(value)) => Some(attr, value)
    case (literal@NumericLiteral(value), Cast(attr: AttributeReference, _)) => Some(attr, value)
    case _ => None
  }
}

// scalastyle:off cyclomatic.complexity
/**
 * Extractor for numeric literals.
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
    case Cast(Literal(value: Int, _), _) => Some(value)
    case Cast(Literal(value: Long, _), _) => Some(value)
    case Cast(Literal(value: Double, _), _) => Some(value)
    case Cast(Literal(value: Float, _), _) => Some(value.toDouble)
    case Cast(Literal(value: Short, _), _) => Some(value.toInt)
    case Cast(Literal(value: Decimal, _), _) => Some(value.toBigDecimal)
    case Cast(Literal(value: java.math.BigDecimal, _), _) => Some(value)
    case Cast(Literal(value: BigDecimal, _), _) => Some(value)
    case _ => None
  }
}

// scalastyle:on cyclomatic.complexity

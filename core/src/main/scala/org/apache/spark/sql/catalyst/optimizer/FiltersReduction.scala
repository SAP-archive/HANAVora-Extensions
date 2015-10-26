package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types._

import scala.annotation.tailrec

/**
 * Removing redundant filters.
 */
object FiltersReduction extends Rule[LogicalPlan] with PredicateHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case q: LogicalPlan => q transformExpressionsUp {
      case exp@And(_, _) =>
        foldExpressions(splitConjunctivePredicates(exp), foldAndExpression).reduce(And)
      case exp@Or(_, _) =>
        foldExpressions(splitDisjunctivePredicates(exp), foldOrExpression).reduce(Or)
    }
  }

  @tailrec
  private def foldExpressions(expressions: Seq[Expression],
                              foldingFunction: (BinaryComparison, BinaryComparison)
                                => Option[Expression],
                              acc: Seq[Expression] = Seq()): Seq[Expression] =
    expressions match {
      case (first@Literal(_, BooleanType)) :: tail => first :: tail
      case first :: Seq() => first +: acc
      case (first: BinaryComparison) :: (second: BinaryComparison) :: tail =>
        foldingFunction(first, second) match {
          case Some(foldedExp) => foldExpressions(foldedExp :: tail, foldingFunction, acc)
          case None => foldExpressions(first :: tail, foldingFunction, acc :+ second)
        }
      case first :: second :: tail => foldExpressions(first :: tail, foldingFunction, acc :+ second)
    }

  // scalastyle:off cyclomatic.complexity
  private def foldAndExpression(left: BinaryComparison,
                                right: BinaryComparison): Option[Expression] =
    (left, right) match {
      case (BinaryComparisonWithNumericLiteral(leftAttr, leftVal),
      BinaryComparisonWithNumericLiteral(rightAttr, rightVal))
        if leftAttr == rightAttr => (left, right) match {
        /**
         * c1 > 5 && c1 > 3 => c1 > 5
         * c1 > 5 && c1 > 5 => c1 > 5
         * c1 > 3 && c1 > 5 => c1 > 5
         * c1 > 5 && c1 >= 3 => c1 > 5
         * c1 > 5 && c1 >= 5 => c1 > 5
         * c1 > 3 && c1 >= 5 => c1 >= 5
         * c1 >= 5 && c1 >= 3 => c1 >= 5
         * c1 >= 5 && c1 >= 5 => c1 >= 5
         * c1 >= 3 && c1 >= 5 => c1 >= 5
         */
        case (GreaterThan(_, _), GreaterThan(_, _))
             | (GreaterThan(_, _), GreaterThanOrEqual(_, _))
             | (GreaterThanOrEqual(_, _), GreaterThanOrEqual(_, _)) =>
          if (leftVal >= rightVal) Some(left)
          else Some(right)

        /**
         * c1 >= 5 && c1 > 3 => c1 >= 5
         * c1 >= 5 && c1 > 5 => c1 > 5
         * c1 >= 3 && c1 > 5 => c1 > 5
         */
        case (GreaterThanOrEqual(_, _), GreaterThan(_, _)) =>
          if (leftVal > rightVal) Some(left)
          else Some(right)

        /**
         * c1 > 5 && c1 == 3 => false
         * c1 > 5 && c1 == 5 => false
         * c1 > 3 && c1 == 5 => c1 == 5
         */
        case (GreaterThan(_, _), EqualTo(_, _)) =>
          if (leftVal >= rightVal) Some(Literal(false))
          else Some(right)

        /**
         * c1 == 5 && c1 > 3 => c1 == 5
         * c1 == 5 && c1 > 5 => false
         * c1 == 3 && c1 > 5 => false
         */
        case (EqualTo(_, _), GreaterThan(_, _)) =>
          if (leftVal > rightVal) Some(left)
          else Some(Literal(false))

        /**
         * c1 >= 5 && c1 == 3 => false
         * c1 >= 5 && c1 == 5 => c1 == 5
         * c1 >= 3 && c1 == 5 => c1 == 5
         */
        case (GreaterThanOrEqual(_, _), EqualTo(_, _)) =>
          if (leftVal > rightVal) Some(Literal(false))
          else Some(right)

        /**
         * c1 == 5 && c1 >= 3 => c1 == 5
         * c1 == 5 && c1 >= 5 => false
         * c1 == 3 && c1 >= 5 => false
         */
        case (EqualTo(_, _), GreaterThanOrEqual(_, _)) =>
          if (leftVal >= rightVal) Some(left)
          else Some(Literal(false))

        /**
         * c1 < 3 && c1 < 5 => c1 < 3
         * c1 < 5 && c1 < 5 => c1 < 5
         * c1 < 5 && c1 < 3 => c1 < 3
         * c1 < 3 && c1 <= 5 => c1 < 3
         * c1 < 5 && c1 <= 5 => c1 < 5
         * c1 < 5 && c1 <= 3 => c1 <= 3
         * c1 <= 3 && c1 <= 5 => c1 <= 3
         * c1 <= 5 && c1 <= 5 => c1 <= 5
         * c1 <= 5 && c1 <= 3 => c1 <= 3
         */
        case (LessThan(_, _), LessThan(_, _))
             | (LessThan(_, _), LessThanOrEqual(_, _))
             | (LessThanOrEqual(_, _), LessThanOrEqual(_, _)) =>
          if (leftVal <= rightVal) Some(left)
          else Some(right)

        /**
         * c1 <= 3 && c1 < 5 => c1 <= 3
         * c1 <= 5 && c1 < 5 => c1 < 5
         * c1 <= 5 && c1 < 3 => c1 < 3
         */
        case (LessThanOrEqual(_, _), LessThan(_, _)) =>
          if (leftVal < rightVal) Some(left)
          else Some(right)

        /**
         * c1 < 3 && c1 == 5 => false
         * c1 < 5 && c1 == 5 => false
         * c1 < 5 && c1 == 3 => c1 == 3
         */
        case (LessThan(_, _), EqualTo(_, _)) =>
          if (leftVal <= rightVal) Some(Literal(false))
          else Some(right)

        /**
         * c1 == 3 && c1 < 5 => c == 3
         * c1 == 5 && c1 < 5 => false
         * c1 == 5 && c1 < 3 => false
         */
        case (EqualTo(_, _), LessThan(_, _)) =>
          if (leftVal < rightVal) Some(left)
          else Some(Literal(false))

        /**
         * c1 <= 3 && c1 == 5 => false
         * c1 <= 5 && c1 == 5 => c1 == 5
         * c1 <= 5 && c1 == 3 => c1 == 3
         */
        case (LessThanOrEqual(_, _), EqualTo(_, _)) =>
          if (leftVal < rightVal) Some(Literal(false))
          else Some(right)

        /**
         * c1 == 3 && c1 <= 5 => c1 == 3
         * c1 == 5 && c1 <= 5 => c1 == 5
         * c1 == 5 && c1 <= 3 => false
         */
        case (EqualTo(_, _), LessThanOrEqual(_, _)) =>
          if (leftVal <= rightVal) Some(left)
          else Some(Literal(false))

        /**
         * c1 > 5 && c1 < 3 => false
         * c1 > 5 && c1 < 5 => false
         * c1 > 3 && c1 < 5 => c1 > 3 && c1 < 5 [NOT REDUCED]
         * c1 > 5 && c1 <= 3 => false
         * c1 > 5 && c1 <= 5 => false
         * c1 > 3 && c1 <= 5 => c1 > 3 && c1 <= 5 [NOT REDUCED]
         * c1 >= 5 && c1 < 3 => false
         * c1 >= 5 && c1 < 5 => false
         * c1 >= 3 && c1 < 5 => c1 >= 3 && c1 < 5 [NOT REDUCED]
         */
        case (GreaterThan(_, _), LessThan(_, _))
             | (GreaterThan(_, _), LessThanOrEqual(_, _))
             | (GreaterThanOrEqual(_, _), LessThan(_, _)) =>
          if (leftVal >= rightVal) Some(Literal(false))
          else None

        /**
         * c1 >= 5 && c1 <= 3 => false
         * c1 >= 5 && c1 <= 5 => c1 == 5
         * c1 >= 3 && c1 <= 5 => c1 >= 3 && c1 <= 5 [NOT REDUCED]
         */
        case (GreaterThanOrEqual(_, _), LessThanOrEqual(_, _)) =>

          val value = if (leftVal > rightVal) Some(Literal(false))
          else if (leftVal == rightVal) Some(EqualTo(left.left, left.right))
          else None
          value

        /**
         * c1 < 3 && c1 > 5 => false
         * c1 < 5 && c1 > 5 => false
         * c1 < 5 && c1 > 3 => c1 < 5 && c1 > 3 [NOT REDUCED]
         * c1 < 3 && c1 >= 5 => false
         * c1 < 5 && c1 >= 5 => false
         * c1 < 5 && c1 >= 3 => c1 < 5 && c1 >= 3 [NOT REDUCED]
         * c1 <= 3 && c1 > 5 => false
         * c1 <= 5 && c1 > 5 => false
         * c1 <= 5 && c1 > 3 => c1 <= 5 && c1 > 3 [NOT REDUCED]
         */
        case (LessThan(_, _), GreaterThan(_, _))
             | (LessThan(_, _), GreaterThanOrEqual(_, _))
             | (LessThanOrEqual(_, _), GreaterThan(_, _)) =>
          if (leftVal <= rightVal) Some(Literal(false))
          else None

        /**
         * c1 <= 3 && c1 >= 5 => false
         * c1 <= 5 && c1 >= 5 => c1 <= 5 && c1 >= 5 [NOT REDUCED]
         * c1 <= 5 && c1 >= 3 => c1 <= 5 && c1 >= 3 [NOT REDUCED]
         */
        case (LessThanOrEqual(_, _), GreaterThanOrEqual(_, _)) =>
          if (leftVal < rightVal) Some(Literal(false))
          else if (leftVal == rightVal) Some(EqualTo(left.left, left.right))
          else None

        /**
         * c1 == 3 && c1 == 3 => c1 == 3
         * c1 == 3 && c1 == 5 => false
         */
        case (EqualTo(_, _), EqualTo(_, _)) =>
          if (leftVal == rightVal) Some(left)
          else Some(Literal(false))
        // Otherwise: Keep it like it is.
        case _ => None
      }
      // leftAttr != rightAttr -> Keep it like it is.
      case _ => None
    }

  private def foldOrExpression(left: BinaryComparison,
                               right: BinaryComparison): Option[Expression] =
    (left, right) match {
      case (BinaryComparisonWithNumericLiteral(leftAttr, leftVal),
      BinaryComparisonWithNumericLiteral(rightAttr, rightVal))
        if leftAttr == rightAttr => (left, right) match {
        /**
         * c1 > 3 || c1 > 5 => c1 > 3
         * c1 > 5 || c1 > 5 => c1 > 5
         * c1 > 5 || c1 > 3 => c1 > 3
         * c1 > 3 || c1 >= 5 => c1 > 3
         * c1 > 5 || c1 >= 5 => c1 >= 5
         * c1 > 5 || c1 >= 3 => c1 >= 3
         * c1 >= 3 || c1 >= 5 => c1 >= 3
         * c1 >= 5 || c1 >= 5 => c1 >= 5
         * c1 >= 5 || c1 >= 3 => c1 >= 3
         */
        case (GreaterThan(_, _), GreaterThan(_, _))
             | (GreaterThan(_, _), GreaterThanOrEqual(_, _))
             | (GreaterThanOrEqual(_, _), GreaterThanOrEqual(_, _)) =>
          if (leftVal < rightVal) Some(left)
          else Some(right)

        /**
         * c1 >= 3 || c1 > 5 => c1 >= 3
         * c1 >= 5 || c1 > 5 => c1 >= 5
         * c1 >= 5 || c1 > 3 => c1 > 3
         */
        case (GreaterThanOrEqual(_, _), GreaterThan(_, _)) =>
          if (leftVal <= rightVal) Some(left)
          else Some(right)

        /**
         * c1 > 3 || c1 == 5 => c1 > 3
         * c1 > 5 || c1 == 5 => c1 >= 5
         * c1 > 5 || c1 == 3 => c1 > 5 || c1 == 3 [NOT REDUCED]
         */
        case (GreaterThan(_, _), EqualTo(_, _)) =>
          if (leftVal < rightVal) Some(left)
          else if (leftVal == rightVal) Some(GreaterThanOrEqual(left.left, left.right))
          else None

        /**
         * c1 == 5 || c1 > 3 => c1 > 3
         * c1 == 5 || c1 > 5 => c1 >= 5
         * c1 == 3 || c1 > 5 => c1 == 3 || c1 > 5 [NOT REDUCED]
         */
        case (EqualTo(_, _), GreaterThan(_, _)) =>
          if (leftVal > rightVal) Some(right)
          else if (leftVal == rightVal) Some(GreaterThanOrEqual(left.left, left.right))
          else None

        /**
         * c1 >= 3 || c1 == 5 => c1 >= 3
         * c1 >= 5 || c1 == 5 => c1 >= 5
         * c1 >= 5 || c1 == 3 => c1 >= 5 || c1 == 3 [NOT REDUCED]
         */
        case (GreaterThanOrEqual(_, _), EqualTo(_, _)) =>
          if (leftVal <= rightVal) Some(left)
          else None

        /**
         * c1 == 5 || c1 => 3 => c1 => 3
         * c1 == 5 || c1 => 5 => c1 >= 5
         * c1 == 3 || c1 => 5 => c1 == 3 || c1 => 5 [NOT REDUCED]
         */
        case (EqualTo(_, _), GreaterThanOrEqual(_, _)) =>
          if (leftVal >= rightVal) Some(right)
          else None

        /**
         * c1 < 5 || c1 < 3 => c1 < 5
         * c1 < 5 || c1 < 5 => c1 < 5
         * c1 < 3 || c1 < 5 => c1 < 5
         * c1 < 5 || c1 <= 3 => c1 < 5
         * c1 < 5 || c1 <= 5 => c1 <= 5
         * c1 < 3 || c1 <= 5 => c1 <= 5
         * c1 <= 5 || c1 <= 3 => c1 <= 5
         * c1 <= 5 || c1 <= 5 => c1 <= 5
         * c1 <= 3 || c1 <= 5 => c1 <= 5
         */
        case (LessThan(_, _), LessThan(_, _))
             | (LessThan(_, _), LessThanOrEqual(_, _))
             | (LessThanOrEqual(_, _), LessThanOrEqual(_, _)) =>
          if (leftVal > rightVal) Some(left)
          else Some(right)

        /**
         * c1 <= 3 || c1 < 5 => c1 < 5
         * c1 <= 5 || c1 < 5 => c1 <= 5
         * c1 <= 5 || c1 < 3 => c1 <= 5
         */
        case (LessThanOrEqual(_, _), LessThan(_, _)) =>
          if (leftVal < rightVal) Some(right)
          else Some(left)

        /**
         * c1 < 5 || c1 == 3 => c1 < 5
         * c1 < 5 || c1 == 5 => c1 <= 5
         * c1 < 3 || c1 == 5 => c1 < 3 || c1 == 5 [NOT REDUCED]
         */
        case (LessThan(_, _), EqualTo(_, _)) =>
          if (leftVal > rightVal) Some(left)
          else if (leftVal == rightVal) Some(LessThanOrEqual(left.left, left.right))
          else None

        /**
         * c1 == 3 || c1 < 5 => c1 < 5
         * c1 == 5 || c1 < 5 => c1 <= 5
         * c1 == 5 || c1 < 3 => c1 == 5 || c1 < 3 [NOT REDUCED]
         */
        case (EqualTo(_, _), LessThan(_, _)) =>
          if (leftVal < rightVal) Some(right)
          else if (leftVal == rightVal) Some(LessThanOrEqual(left.left, left.right))
          else None

        /**
         * c1 <= 5 || c1 == 3 => c1 <= 5
         * c1 <= 5 || c1 == 5 => c1 <= 5
         * c1 <= 3 || c1 == 5 => c1 <= 3 || c1 == 5 [NOT REDUCED]
         */
        case (LessThanOrEqual(_, _), EqualTo(_, _)) =>
          if (leftVal >= rightVal) Some(left)
          else None

        /**
         * c1 == 3 || c1 <= 5 => c1 <= 5
         * c1 == 5 || c1 <= 5 => c1 <= 5
         * c1 == 5 || c1 <= 3 => c1 == 5 || c1 <= 3 [NOT REDUCED]
         */
        case (EqualTo(_, _), LessThanOrEqual(_, _)) =>
          if (leftVal <= rightVal) Some(right)
          else None

        /**
         * c1 > 3 || c1 <= 5 => true
         * c1 > 5 || c1 <= 5 => true
         * c1 > 5 || c1 <= 3 => c1 > 5 || c1 <= 3 [NOT REDUCED]
         * c1 >= 3 || c1 < 5 => true
         * c1 >= 5 || c1 < 5 => true
         * c1 >= 5 || c1 < 3 => c1 >= 5 || c1 < 3 [NOT REDUCED]
         * c1 >= 3 || c1 <= 5 => true
         * c1 >= 5 || c1 <= 5 => true
         * c1 >= 5 || c1 <= 3 => c1 >= 5 || c1 <= 3 [NOT REDUCED]
         */
        case (GreaterThan(_, _), LessThanOrEqual(_, _))
             | (GreaterThanOrEqual(_, _), LessThan(_, _))
             | (GreaterThanOrEqual(_, _), LessThanOrEqual(_, _)) =>
          if (leftVal <= rightVal) Some(Literal(true))
          else None

        /**
         * c1 > 3 || c1 < 5 => true
         * c1 > 5 || c1 < 5 => c1 > 5 || c1 < 5 [NOT REDUCED]
         * c1 > 5 || c1 < 3 => c1 > 5 || c1 < 3 [NOT REDUCED]
         */
        case (GreaterThan(_, _), LessThan(_, _)) =>
          if (leftVal < rightVal) Some(Literal(true))
          else None

        /**
         * c1 < 5 || c1 >= 3 => true
         * c1 < 5 || c1 >= 5 => true
         * c1 < 3 || c1 >= 5 => c1 < 3 || c1 >= 5 [NOT REDUCED]
         * c1 <= 5 || c1 > 3 => true
         * c1 <= 5 || c1 > 5 => true
         * c1 <= 3 || c1 > 5 => c1 <= 3 || c1 > 5 [NOT REDUCED]
         * c1 <= 5 || c1 >= 3 => true
         * c1 <= 5 || c1 >= 5 => true
         * c1 <= 3 || c1 >= 5 => c1 <= 3 || c1 >= 5 [NOT REDUCED]
         */
        case (LessThan(_, _), GreaterThanOrEqual(_, _))
             | (LessThanOrEqual(_, _), GreaterThan(_, _))
             | (LessThanOrEqual(_, _), GreaterThanOrEqual(_, _)) =>
          if (leftVal >= rightVal) Some(Literal(true))
          else None

        /**
         * c1 < 5 || c1 > 3 => true
         * c1 < 5 || c1 > 5 => c1 < 5 || c1 > 5 [NOT REDUCED]
         * c1 < 3 || c1 > 5 => c1 < 3 || c1 > 5 [NOT REDUCED]
         */
        case (LessThan(_, _), GreaterThan(_, _)) =>
          if (leftVal > rightVal) Some(Literal(true))
          else None
        case _ => None
      }
      case _ => None
    }

  // scalastyle:on cyclomatic.complexity
}

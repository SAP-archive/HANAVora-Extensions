package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{And, Or, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * More rules to simplify the boolean logic in the logical plan
 */
object ExtraBooleanSimplification extends Rule[LogicalPlan] with PredicateHelper {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case q: LogicalPlan => q transformExpressionsUp {
      case Or(And(left, right), exp) => And(Or(left, exp), Or(right, exp))
      case Or(exp, And(left, right)) => And(Or(left, exp), Or(right, exp))
    }
  }
}

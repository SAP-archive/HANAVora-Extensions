package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.ScalaUDF
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule

import scala.annotation.tailrec

/**
 * Rule to expand OR expression, convert them into AND based one and detect if it contains
 * some down pushable sub-expressions. If we detect some, we add them, even if they are redundant,
 * in order to anticipate some filter execution in the datasource.
 * To sum up, it transforms an OR expression into a more optimal AND expression containing the
 * original OR expression along with the down pushable transformed filters. E.g:
 * The query:
 * SELECT * FROM t1, t2 WHERE ((t1.c1 < 5 && t2.c1) || t1.c2)
 * , will turn into:
 * SELECT * FROM t1, t2 WHERE ((t1.c1 < 5 && t2.c1) || t1.c2) && (t1.c1 || t1.c2)
 */
object RedundantDownPushableFilters extends Rule[LogicalPlan] with PredicateHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case Filter(condition, child) =>
      val childrenAttributes = retrieveChildrenAttributes(child)
      val exp = if (childrenAttributes.nonEmpty) {
        transformExpression(condition, childrenAttributes) match {
          case None => condition
          case Some(e) => And(condition, e)
        }
      } else {
        condition
      }
      Filter(exp, child)
  }

  /**
   * Looks for a logical plan with multiple children and returns the attribute
   * set for each of them.
   * @param plan Current logical plan to check children.
   * @return Sequence of attribute set per children.
   */
  @tailrec
  private def retrieveChildrenAttributes(plan: LogicalPlan): Seq[AttributeSet] =
    if (plan.children.length == 1) retrieveChildrenAttributes(plan.children.head)
    else if (plan.children.length > 1) plan.children.map(_.outputSet)
    else Seq()

  /**
   * Transforms an OR expression into a more optimal AND expression containing the original OR
   * expression along with the down pushable transformed filters.
   * @param expr Expression to be transformed.
   * @param childrenAttributes Children attribute sets.
   * @return Expression transformed if possible.
   */
  private def transformExpression(expr: Expression,
                                  childrenAttributes: Seq[AttributeSet]): Option[Expression] =
    expr match {
      case or: Or if !containsNonDeterministicExpressions(Seq(or)) =>
        // Expressions included in the or expression
        val subExpressions = splitDisjunctivePredicates(or).map(splitConjunctivePredicates)
        // Expressions cross product
        val combinations = expressionsCrossProduct(subExpressions)
        // Expressions referencing same logical plan child
        val compatibleExp: Seq[Seq[Expression]] =
          combinations.filter(combination => combination.forall(
            exp => checkCompatibleReferences(combination.map(_.references).
              reduce((x, y) => x ++ y), childrenAttributes)))
        // Creating the AND expression containing down pushable OR filters
        compatibleExp.flatMap(_.reduceLeftOption(Or)).reduceLeftOption(And)
      case and: And =>
        val predicates: Seq[Expression] = splitConjunctivePredicates(and)
        predicates.flatMap(transformExpression(_, childrenAttributes)).reduceLeftOption(And)
      case exp => None
    }

  /**
   * Checks if all refs in attribute set are contained in a single logical plan child.
   * @param refs Expressions attribute set.
   * @param columns Logical plan children attribute sets.
   * @return true, if references are compatible with a single logical plan child; false, otherwise.
   */
  private def checkCompatibleReferences(refs: AttributeSet,
                                        columns: Seq[AttributeSet]): Boolean =
    columns.exists(_.intersect(refs).size == refs.size)

  /**
   * Iterative cross product between the tuples in the Seq.
   * @param tuples Tuples to be combined.
   * @return Tuples multiple cross product.
   */
  private def expressionsCrossProduct(tuples: Seq[Seq[Expression]]): Seq[Seq[Expression]] = {
    tuples.foldLeft(Seq(Seq.empty): Seq[Seq[Expression]])(
      (acc, expressions) => for (x <- acc; y <- expressions) yield x :+ y)
  }

  /**
   * Checks if there is a non deterministic expression on a set of expressions.
   * @param expressions Set of expressions.
   * @return true, if there is at least one non deterministic expression; false, otherwise.
   */
  private def containsNonDeterministicExpressions(expressions: Seq[Expression]): Boolean =
    expressions.exists(isNonDeterministicExpression) ||
      expressions.exists(exp => containsNonDeterministicExpressions(exp.children))

  /**
   * Checks if a single expression is one of the marked as non deterministic.
   * @param exp Expression to be checked.
   * @return true, if it is a random expression or an user defined function (which might be
   *         non deterministic but we can't be sure by now); false, otherwise.
   */
  // TODO Adapt this function when we migrate to Spark 1.5.X. There is a non deterministic
  // trait extended by every non deterministic expression
  private def isNonDeterministicExpression(exp: Expression): Boolean = exp match {
    case Rand(_) | Randn(_) | _: ScalaUDF => true
    case _ => false
  }
}

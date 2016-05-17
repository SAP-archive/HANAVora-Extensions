package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.Catalog
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types.Node
import org.apache.spark.sql.catalyst.expressions.EqualTo
import org.apache.spark.sql.catalyst.expressions.AttributeReference

/**
  * A rule to do checks correctness of [[Hierarchy]].
  *
  * - A check makes sure that a binary UDF does not have allow using [[Node]] columns
  *   from different hierarchies as arguments.
  * - Parenthood expression is adjacency list hierarchy should be equality.
  * - Level-based hierarchy supports PATH levels only.
  * - All levels in level-based hierarchy must be of the same type.
 *
 */
private[sql] case class HierarchyAnalysis(catalog: Catalog) extends (LogicalPlan => Unit) {

  val MIXED_NODES_ERROR = "It is not allowed to use Node columns from different " +
    "hierarchies in the %s UDF."

  val LEVELED_HIERARCHY_TYPE_ERROR = "A level-based hierarchy expects all level columns to be" +
    " of the same type, however columns of the following types are provided: %s."

  val ADJACENCY_HIERARCHY_EXPRESSION_ERROR = "The parenthood expression of an adjacency list " +
    "hierarchy is expected to be simple equality between two attributes of the same type, " +
    "however %s is provided."

  val LEVELED_HIERARCHY_MATCHER_ERROR = "Level-based hierarchy currently only supports PATH " +
    "levels, check VORASPARK-273 for more information."

  // scalastyle:off cyclomatic.complexity
  def apply(plan: LogicalPlan): Unit = {
    if (plan.resolved) {
      plan.foreach {
        case Hierarchy(AdjacencyListHierarchySpec(_, _, parenthoodExp, _, _), _) =>
          parenthoodExp match {
            case EqualTo(l: AttributeReference, r: AttributeReference) if l.dataType
              .sameType(r.dataType) => // ok
            case a: Expression => throw new AnalysisException(ADJACENCY_HIERARCHY_EXPRESSION_ERROR
              .format(a.toString))
          }
        case Hierarchy(LevelBasedHierarchySpec(_, levels, _, _, _), _) if levels
          .map(_.dataType).distinct.size > 1 =>
          throw new AnalysisException(LEVELED_HIERARCHY_TYPE_ERROR.format(
            levels.map(_.dataType).distinct.mkString(",")))
        case Hierarchy(LevelBasedHierarchySpec(_, _, _, _, matcher), _) if matcher != MatchPath =>
          throw new AnalysisException(LEVELED_HIERARCHY_MATCHER_ERROR)
        case _ =>
      }
      plan.foreach {
        case p => p.expressions.foreach(e => supportsExpression(e, plan))
      }
    }
  }
  // scalastyle:on cyclomatic.complexity

  /**
    * Checks if the expression is supported.
    *
    * @param expr The expression.
    * @param plan The plan having references for the expression.
    * @throws AnalysisException if the expression is not supported.
    */
  private[this] def supportsExpression(expr: Expression, plan: LogicalPlan): Unit = {
    expr match {
      case np: NodePredicate =>
        (np.left, np.right) match {
          case (l: AttributeReference, r: AttributeReference) =>
            val hl = getReferencedHierarchy(plan, l.exprId)
            val hr = getReferencedHierarchy(plan, r.exprId)
            if (hl.identifier != hr.identifier) {
              throw new AnalysisException(MIXED_NODES_ERROR.format(np.symbol))
            }
          case _ => // OK
        }
      case _ => // OK
    }
    expr.children.foreach(e => supportsExpression(e, plan))
  }

  private def getReferencedHierarchy(plan: LogicalPlan, exprId: ExprId): Hierarchy = {
    plan.collectFirst {
      case h@Hierarchy(_, a) if a.exprId.equals(exprId) => h
    }.get
  }
}

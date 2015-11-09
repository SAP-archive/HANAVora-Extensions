package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.Catalog
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types._

/**
 * A rule to do checks on [[Hierarchy]] UDFs.
 *
 * The check makes sure that a binary UDF does not have allow mixing [[Node]] columns
 * from different hierarchies.
 *
 */
private[sql] case class HierarchyUDFAnalysis(catalog: Catalog) extends (LogicalPlan => Unit) {

  val MIXED_NODES_ERROR = "It is not allowed to use Node columns from different " +
    "hierarchies in the %s UDF."

  private def failAnalysis(msg: String): Unit = {
    throw new AnalysisException(msg)
  }

  def apply(plan: LogicalPlan): Unit = {
    plan transform {
      case p =>
        p.expressions.foreach(e => supportsExpression(e, plan))
        p
    }
  }

  private def supportsExpression(expr: Expression, plan: LogicalPlan): Unit = {
    expr match {
      case np: NodePredicate =>
        (np.left, np.right) match {
          case (l: AttributeReference, r: AttributeReference) =>
            val hl = getHierarchy(plan, l.qualifiers.head)
            val hr = getHierarchy(plan, r.qualifiers.head)
            (hl, hr) match {
              case (Some(h1), Some(h2)) => if (!h1.identifier.equals(h2.identifier)) {
                failAnalysis(MIXED_NODES_ERROR.format(np.symbol))
              }
              case _ => // OK
            }
          case _ => // OK
        }
      case _ => // OK
    }
    expr.children.foreach(e => supportsExpression(e, plan))
  }

  private def getHierarchy(plan: LogicalPlan, qualifier: String): Option[Hierarchy] = {
    var hs: Option[Hierarchy] = None
    plan foreach  {
      case s: Subquery if s.alias.equals(qualifier) =>
        internalSubquery(s) match {
          case Some(h) => hs = Some(h)
          case None => // OK
        }
      case _ => None
    }
    hs
  }

  private def internalSubquery(plan: Subquery): Option[Hierarchy] = plan.child match {
    case h: Hierarchy => Some(h)
    case s: Subquery => internalSubquery(s)
    case _ => None
  }
}

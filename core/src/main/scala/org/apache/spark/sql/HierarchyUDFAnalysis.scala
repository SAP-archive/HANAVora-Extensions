package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.Catalog
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types.Node

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
            val hl = getReferencedHierarchy(plan, l.exprId)
            val hr = getReferencedHierarchy(plan, r.exprId)
            if (!hl.identifier.equals(hr.identifier)) {
              failAnalysis(MIXED_NODES_ERROR.format(np.symbol))
            }
          case _ => // OK
        }
      case _ => // OK
    }
    expr.children.foreach(e => supportsExpression(e, plan))
  }

  private def getReferencedHierarchy(plan: LogicalPlan, exprId: ExprId): Hierarchy = {
    plan.collectFirst {
      case h@Hierarchy(_, _, _, _, _, a) if a.exprId.equals(exprId) => h
    }.get
  }
}

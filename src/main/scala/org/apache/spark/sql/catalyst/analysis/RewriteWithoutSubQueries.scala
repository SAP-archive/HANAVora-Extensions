package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Subquery}
import org.apache.spark.sql.catalyst.rules.Rule


/**
 * This rule replaces the default [[EliminateSubQueries]].
 * It does rewrite attribute qualifiers when removing a subquery.
 */
object RewriteWithoutSubQueries extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan =
    removeSubqueries(plan)._1

  private def removeSubqueries(plan: LogicalPlan): (LogicalPlan, Map[Seq[String],Attribute]) =
    plan match {
      case Subquery(alias, child) =>
        val replacements = child.output.map({ att => (att.qualifiers :+ att.name , att) }).toMap
        (child, replacements)
      case p: LogicalPlan if p.children.nonEmpty =>
        val (newChildren, replacements) = p.children
          .foldLeft((Seq[LogicalPlan](), Map[Seq[String],Attribute]())) { (acc, child) =>
            val (newChild, replacements) = removeSubqueries(child)
            (acc._1 :+ newChild, acc._2 ++ replacements)
          }
        val planWithNewChildren = p.withNewChildren(newChildren)
        val transformedPlan = planWithNewChildren transformExpressions {
          case att: AttributeReference =>
            val key = att.qualifiers :+ att.name
            replacements.get(key) match {
              case None => att
              case Some(newAtt) => newAtt
            }
        }
        (transformedPlan, replacements)
      case p: LogicalPlan => (p, Map())
    }

}

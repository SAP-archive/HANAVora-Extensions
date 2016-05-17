package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule

case class ResolveReferencesWithHierarchies(analyzer: Analyzer) extends Rule[LogicalPlan] {

  // scalastyle:off cyclomatic.complexity
  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case p: LogicalPlan if !p.childrenResolved => p

    // Special handling for cases when self-join introduce duplicate expression ids.
    case j @ Join(left, right, _, _) if !j.selfJoinResolved =>
      val conflictingAttributes = left.outputSet.intersect(right.outputSet)
      logDebug(s"Conflicting attributes ${conflictingAttributes.mkString(",")} in $j")

      right.collect {
        case oldVersion@Hierarchy(_, nodeAttr) if conflictingAttributes.contains(nodeAttr) =>
          (oldVersion, oldVersion.copy(node = nodeAttr.newInstance()))
      }
        .headOption match {
        case None =>
          j
        case Some((oldRelation, newRelation)) =>
          val attributeRewrites = AttributeMap(oldRelation.output.zip(newRelation.output))
          val newRight = right transformUp {
            case r if r == oldRelation => newRelation
          } transformUp {
            case other => other transformExpressions {
              case a: Attribute => attributeRewrites.get(a).getOrElse(a)
            }
          }
          j.copy(right = newRight)
      }

    case q: LogicalPlan => q
  }
  // scalastyle:on cyclomatic.complexity

}

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.plans.logical.{Hierarchy, LogicalPlan}
import org.apache.spark.sql.execution.{SparkPlan, HierarchyPhysicalPlan}

case class HierarchyStrategy(planner : ExtendedPlanner) extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case h @ Hierarchy(relation, childAlias, parenthoodExp, searchBy, startWhere, _) =>
      HierarchyPhysicalPlan(
        childAlias,
        parenthoodExp, searchBy, startWhere, h.nodeAttribute, planner.planLaterExt(relation)
      ) :: Nil
    case _ => Nil
  }
}

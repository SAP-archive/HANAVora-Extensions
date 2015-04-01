package org.apache.spark.sql

import org.apache.spark.sql.catalyst.plans.logical.{Hierarchy, LogicalPlan}
import org.apache.spark.sql.execution.{SparkPlan, HierarchyPhysicalPlan}

case class HierarchyStrategy(planner : ExtendedPlanner) extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case Hierarchy(
      alias, relation, childAlias, parenthoodExp,
      searchBy, startWhere, nodeAttribute) =>
      HierarchyPhysicalPlan(alias, childAlias, parenthoodExp, searchBy, startWhere, nodeAttribute,
        planner.planLaterExt(relation)) :: Nil
    case _ => Nil
  }
}

package org.apache.spark.sql.hierarchy

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.extension.ExtendedPlanner

private[sql] case class HierarchyStrategy(planner: ExtendedPlanner) extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case Hierarchy(
        a@AdjacencyListHierarchySpec(source, _, parenthoodExp, startWhere, orderBy), node) =>
      AdjacencyListHierarchyPlan(
        planner.planLaterExt(source),
        parenthoodExp,
        startWhere,
        orderBy,
        node,
        a.pathDataType) :: Nil
    case Hierarchy(
        a@LevelBasedHierarchySpec(source, levels, startWhere, orderBy, matcher), node) =>
      LevelHierarchyPlan(
        planner.planLaterExt(source),
        levels,
        startWhere,
        orderBy,
        matcher,
        node,
        a.pathDataType) :: Nil
    case _ => Nil
  }
}

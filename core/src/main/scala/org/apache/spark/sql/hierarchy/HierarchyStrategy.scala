package org.apache.spark.sql.hierarchy

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.{HierarchyPlan, SparkPlan}
import org.apache.spark.sql.extension.ExtendedPlanner

private[sql] case class HierarchyStrategy(planner: ExtendedPlanner) extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case Hierarchy(spec:HierarchySpec, node) =>
      HierarchyPlan(planner.planLaterExt(spec.source), spec, node) :: Nil
    case _ => Nil
  }
}

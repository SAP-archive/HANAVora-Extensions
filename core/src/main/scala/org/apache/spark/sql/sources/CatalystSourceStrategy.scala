package org.apache.spark.sql.sources

import org.apache.spark.sql.{execution, Strategy}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

private[sql] object CatalystSourceStrategy extends Strategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan.flatMap({
    case LogicalRelation(cs: CatalystSource) if
      cs.supportsLogicalPlan(plan)=>
      execution.PhysicalRDD(plan.output, cs.logicalPlanToRDD(plan)) :: Nil
    case _ => Nil
  }).headOption.toSeq
}

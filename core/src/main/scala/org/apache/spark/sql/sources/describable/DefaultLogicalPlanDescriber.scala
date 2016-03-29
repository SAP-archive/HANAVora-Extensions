package org.apache.spark.sql.sources.describable

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
  * The default describer of [[LogicalPlan]]s.
  * @param plan The [[LogicalPlan]] to describe.
  */
case class DefaultLogicalPlanDescriber(plan: LogicalPlan)
  extends LogicalPlanDescribable
  with NoAdditions

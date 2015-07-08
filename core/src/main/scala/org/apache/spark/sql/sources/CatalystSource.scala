package org.apache.spark.sql.sources

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * A relation supporting full logical plans straight from Catalyst.
 */
trait CatalystSource {

  /**
   * Checks if the logical plan is fully supported by this relation.
   *
   * @param plan Logical plan.
   * @return
   */
  def supportsLogicalPlan(plan: LogicalPlan): Boolean

  /**
   * Takes a logical plan and returns an RDD[Row].
   *
   * @param plan Logical plan.
   * @return
   */
  def logicalPlanToRDD(plan: LogicalPlan): RDD[Row]

}

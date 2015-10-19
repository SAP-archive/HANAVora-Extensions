package org.apache.spark.sql.extension

import org.apache.spark.Logging
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

@DeveloperApi
private[sql] trait ExtendedPlanner extends Logging {
  self: SQLContext#SparkPlanner =>

  def planLaterExt(p: LogicalPlan): SparkPlan = self.planLater(p)

  def optimizedPlan(p: LogicalPlan): LogicalPlan = self.sqlContext.executePlan(p).optimizedPlan

  override def plan(p: LogicalPlan): Iterator[SparkPlan] = {
    val iter = strategies.view.flatMap({ strategy =>
      val plans = strategy(p)
      if (plans.isEmpty) {
        logTrace(s"Strategy $strategy did not produce plans for $p")
      } else {
        logDebug(s"Strategy $strategy produced a plan for $p: ${plans.head}")
      }
      plans
    }).toIterator
    assert(iter.hasNext, s"No plan for $p")
    iter
  }

}

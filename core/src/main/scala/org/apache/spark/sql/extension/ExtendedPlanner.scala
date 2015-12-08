package org.apache.spark.sql.extension

import org.apache.spark.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

/**
  * Our planner exposes some methods that are needed for 3rd party strategies.
  * See SPARK-6320.
  *
  * @see [[ExtendableSQLContext]]
  */
private[sql] trait ExtendedPlanner extends Logging {
  self: SQLContext#SparkPlanner =>

  /**
    * Calls the planner on a subtree. This is to be used by strategies
    * internally.
    *
    * @param p Subtree.
    * @return Planned subtree.
    */
  def planLaterExt(p: LogicalPlan): SparkPlan = self.planLater(p)

  def optimizedPlan(p: LogicalPlan): LogicalPlan = self.sqlContext.executePlan(p).optimizedPlan

  def optimizedRelationLookup(u: UnresolvedRelation): Option[LogicalPlan] = {
    if (self.sqlContext.catalog.tableExists(u.tableIdentifier)) {
      Some(optimizedPlan(u))
    } else {
      None
    }
  }

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

package org.apache.spark.sql.sources.sql

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
  * This class represents a view which is merely a name holder for a logical plan.
  * @param name The (qualified) name of the view.
  * @param plan The logical plan of the view.
  */
case class View(name: TableIdentifier, plan: LogicalPlan) {

  val METADATA_VIEW_SQL = "SQL"
  def unapply(view: View): Option[(String, LogicalPlan)] = Some(name.table, plan)

}

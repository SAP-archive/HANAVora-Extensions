package org.apache.spark.sql.sources.sql

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * This class represents a cube view which is merely a name holder for a logical plan.
 *
 * @param name The (qualified) name of the cube view.
 * @param plan The logical plan of the cube view.
 */
case class CubeView(name: TableIdentifier, plan: LogicalPlan) {

  val METADATA_VIEW_SQL = "SQL"
  def unapply(view: CubeView): Option[(String, LogicalPlan)] = Some(name.table, plan)

}

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.sources.TemporaryFlagRelation

/**
 * This trait is a thin wrapper of a logical plan for a cube view statement.
 */
sealed trait CubeView extends LeafNode {
  val plan: LogicalPlan

  override def output: Seq[Attribute] = Seq.empty
}

object CubeView {
  def unapply(plan: LogicalPlan): Option[LogicalPlan] = plan match {
    case c: CubeView => Some(c.plan)
    case _ => None
  }
}

/**
 * This class represents a cube view that is persisted in the catalog of a data source.
 *
 * @param plan The query plan of the view.
 */
case class PersistedCubeView(plan: LogicalPlan)
  extends CubeView with TemporaryFlagRelation {
  override def isTemporary(): Boolean = false
}

/**
 * This class represents a cube view that is not persisted in a data source.
 *
 * @param plan The query plan of the view.
 */
case class NonPersistedCubeView(plan: LogicalPlan)
  extends CubeView


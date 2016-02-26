package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.sources.TemporaryFlagRelation

/**
 * This trait is a thin wrapper of a logical plan for a dimension view statement.
 */
sealed trait DimensionView extends LeafNode {
  val plan: LogicalPlan

  override def output: Seq[Attribute] = Seq.empty
}

object DimensionView {
  def unapply(plan: LogicalPlan): Option[LogicalPlan] = plan match {
    case p: PersistedDimensionView => Some(p.plan)
    case n: NonPersistedDimensionView => Some(n.plan)
    case _ => None
  }
}

/**
 * This class represents a dimension view that is persisted in the catalog of a data source.
 *
 * @param plan The query plan of the view.
 */
case class PersistedDimensionView(plan: LogicalPlan)
  extends DimensionView with TemporaryFlagRelation {
  override def isTemporary(): Boolean = false
}

/**
 * This class represents a dimension view that is not persisted in a data source.
 *
 * @param plan The query plan of the view.
 */
case class NonPersistedDimensionView(plan: LogicalPlan)
  extends DimensionView


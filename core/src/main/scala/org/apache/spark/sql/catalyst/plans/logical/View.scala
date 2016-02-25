package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.sources.TemporaryFlagRelation

/**
 * This trait is a thin wrapper of a logical plan for a view statement.
 */
sealed trait View extends LeafNode {
  val plan: LogicalPlan

  override def output: Seq[Attribute] = Seq.empty
}

object View {
  def unapply(plan: LogicalPlan): Option[LogicalPlan] = plan match {
    case p: PersistedView => Some(p.plan)
    case n: NonPersistedView => Some(n.plan)
    case _ => None
  }
}

/**
 * This class represents a view that is persisted in the catalog of a data source.
 *
 * @param plan The query plan of the view.
 */
case class PersistedView(plan: LogicalPlan) extends View with TemporaryFlagRelation {
  override def isTemporary(): Boolean = false
}

/**
 * This class represents a view that is not persisted in a data source.
 *
 * @param plan The query plan of the view.
 */
case class NonPersistedView(plan: LogicalPlan) extends View


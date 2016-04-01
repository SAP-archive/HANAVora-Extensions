package org.apache.spark.sql.catalyst.plans.logical

/**
 * This trait is a thin wrapper of a logical plan for a view statement.
 */
sealed trait View extends LeafNode with AbstractView with NoOutput

object View {
  def unapply(view: View): Option[LogicalPlan] = Some(view.plan)
}

/**
 * This class represents a view that is persisted in the catalog of a data source.
 *
 * @param plan The query plan of the view.
 */
case class PersistedView(plan: LogicalPlan) extends View with Persisted

/**
 * This class represents a view that is not persisted in a data source.
 *
 * @param plan The query plan of the view.
 */
case class NonPersistedView(plan: LogicalPlan) extends View with NonPersisted


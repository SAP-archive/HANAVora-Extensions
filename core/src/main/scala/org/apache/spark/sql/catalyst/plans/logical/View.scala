package org.apache.spark.sql.catalyst.plans.logical

/**
 * This trait is a thin wrapper of a logical plan for a view statement.
 */
sealed trait View extends AbstractView

/**
 * This class represents a view that is persisted in the catalog of a data source.
 *
 * @param plan The query plan of the view.
 */
case class PersistedView(plan: LogicalPlan)
  extends AbstractTaggedViewBase[PersistedView]
  with View
  with Persisted

/**
 * This class represents a view that is not persisted in a data source.
 *
 * @param plan The query plan of the view.
 */
case class NonPersistedView(plan: LogicalPlan)
  extends AbstractViewBase
  with View
  with NonPersisted


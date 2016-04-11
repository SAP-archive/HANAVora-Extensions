package org.apache.spark.sql.catalyst.plans.logical

/**
 * This trait is a thin wrapper of a logical plan for a dimension view statement.
 */
sealed trait DimensionView extends AbstractView

/**
 * This class represents a dimension view that is persisted in the catalog of a data source.
 *
 * @param plan The query plan of the view.
 */
case class PersistedDimensionView(plan: LogicalPlan)
  extends AbstractTaggedViewBase[PersistedDimensionView]
  with DimensionView
  with Persisted

/**
 * This class represents a dimension view that is not persisted in a data source.
 *
 * @param plan The query plan of the view.
 */
case class NonPersistedDimensionView(plan: LogicalPlan)
  extends AbstractViewBase
  with DimensionView
  with NonPersisted


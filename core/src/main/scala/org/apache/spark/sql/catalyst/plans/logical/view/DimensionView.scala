package org.apache.spark.sql.catalyst.plans.logical.view

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.sources.{Dimension, ViewHandle}

/**
 * This trait is a thin wrapper of a logical plan for a dimension view statement.
 */
sealed trait DimensionView extends AbstractView with Dimension

/**
 * This class represents a dimension view that is persisted in the catalog of a data source.
 *
 * @param plan The query plan of the view.
 */
case class PersistedDimensionView(plan: LogicalPlan, handle: ViewHandle, provider: String)
  extends AbstractView
  with DimensionView
  with Persisted

/**
 * This class represents a dimension view that is not persisted in a data source.
 *
 * @param plan The query plan of the view.
 */
case class NonPersistedDimensionView(plan: LogicalPlan)
  extends AbstractView
  with DimensionView
  with NonPersisted


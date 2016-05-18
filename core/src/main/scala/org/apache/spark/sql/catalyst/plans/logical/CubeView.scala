package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.sources.ViewHandle
import org.apache.spark.sql.sources.sql.{ViewKind, Cube => CubeKind}

/**
 * This trait is a thin wrapper of a logical plan for a cube view statement.
 */
sealed trait CubeView extends AbstractView {
  override val kind: ViewKind = CubeKind
}

/**
 * This class represents a cube view that is persisted in the catalog of a data source.
 *
 * @param plan The query plan of the view.
 */
case class PersistedCubeView(plan: LogicalPlan, handle: ViewHandle)
  extends AbstractView
  with CubeView
  with Persisted

/**
 * This class represents a cube view that is not persisted in a data source.
 *
 * @param plan The query plan of the view.
 */
case class NonPersistedCubeView(plan: LogicalPlan)
  extends AbstractView
  with CubeView
  with NonPersisted


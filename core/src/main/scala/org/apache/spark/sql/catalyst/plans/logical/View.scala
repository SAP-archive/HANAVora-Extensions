package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.sources.ViewHandle
import org.apache.spark.sql.sources.sql.{Plain, ViewKind}

/**
 * This trait is a thin wrapper of a logical plan for a view statement.
 */
sealed trait View extends AbstractView {
  override val kind: ViewKind = Plain
}

/**
 * This class represents a view that is persisted in the catalog of a data source.
 *
 * @param plan The query plan of the view.
 */
case class PersistedView(plan: LogicalPlan, handle: ViewHandle, provider: String)
  extends AbstractView
  with View
  with Persisted

/**
 * This class represents a view that is not persisted in a data source.
 *
 * @param plan The query plan of the view.
 */
case class NonPersistedView(plan: LogicalPlan)
  extends AbstractView
  with View
  with NonPersisted


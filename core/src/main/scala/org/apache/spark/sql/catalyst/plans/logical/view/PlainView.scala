package org.apache.spark.sql.catalyst.plans.logical.view

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.sources.{Plain, ViewHandle}

/**
 * This trait is a thin wrapper of a logical plan for a view statement.
 */
sealed trait PlainView extends AbstractView with Plain

/**
 * This class represents a view that is persisted in the catalog of a data source.
 *
 * @param plan The query plan of the view.
 */
case class PersistedPlainView(plan: LogicalPlan, handle: ViewHandle, provider: String)
  extends AbstractView
  with PlainView
  with Persisted

/**
 * This class represents a view that is not persisted in a data source.
 *
 * @param plan The query plan of the view.
 */
case class NonPersistedPlainView(plan: LogicalPlan)
  extends AbstractView
  with PlainView
  with NonPersisted


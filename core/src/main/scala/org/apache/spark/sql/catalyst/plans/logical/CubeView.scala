package org.apache.spark.sql.catalyst.plans.logical

/**
 * This trait is a thin wrapper of a logical plan for a cube view statement.
 */
sealed trait CubeView extends LeafNode with AbstractView with NoOutput

object CubeView {
  def unapply(cubeView: CubeView): Option[LogicalPlan] = Some(cubeView.plan)
}

/**
 * This class represents a cube view that is persisted in the catalog of a data source.
 *
 * @param plan The query plan of the view.
 */
case class PersistedCubeView(plan: LogicalPlan) extends CubeView with Persisted

/**
 * This class represents a cube view that is not persisted in a data source.
 *
 * @param plan The query plan of the view.
 */
case class NonPersistedCubeView(plan: LogicalPlan) extends CubeView with NonPersisted


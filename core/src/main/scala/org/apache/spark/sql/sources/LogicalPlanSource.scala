package org.apache.spark.sql.sources

import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, PersistedCubeView, PersistedDimensionView, PersistedView}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.{DataFrame, SQLContext}

/** Source from which a [[LogicalPlan]] can be obtained. */
trait LogicalPlanSource {
  /** Instantiates a [[LogicalPlan]] with the given sqlContext.
    *
    * @param sqlContext The sqlContext
    * @return The created [[LogicalPlan]]
    */
  def logicalPlan(sqlContext: SQLContext): LogicalPlan
}

/** Source of a [[org.apache.spark.sql.DataFrame]] from a BaseRelation.
  *
  * @param baseRelation The baseRelation from which the [[DataFrame]] is created.
  */
case class BaseRelationSource(baseRelation: BaseRelation) extends LogicalPlanSource {
  def logicalPlan(sqlContext: SQLContext): LogicalPlan = {
    LogicalRelation(baseRelation)
  }
}

/** Source of a [[org.apache.spark.sql.DataFrame]] from a create persistent view statement
  *
  * @param createViewStatement The sql query string.
  */
case class CreatePersistentViewSource(createViewStatement: String, handle: ViewHandle)
  extends LogicalPlanSource {

  def logicalPlan(sqlContext: SQLContext): LogicalPlan = {
    sqlContext.parseSql(createViewStatement) match {
      // This might seem repetitive but in the future the commands might drastically differ
      case CreatePersistentViewCommand(kind, _, plan, _, provider, _, _) =>
        kind.createPersisted(plan, handle, provider)

      case unknown =>
        throw new RuntimeException(s"Could not extract view query from $unknown")
    }
  }
}


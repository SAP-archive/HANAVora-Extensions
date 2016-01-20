package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.sources.BaseRelation

/**
 * Due to visibility issues this object is needed to determine if a
 * [[LogicalPlan]] is a [[BaseRelation]].
 * Used in the datasource.
 */
object IsLogicalRelation {

  def unapply(plan: LogicalPlan): Option[BaseRelation] = {
    plan.isInstanceOf[LogicalRelation] match {
      case true => Some(plan.asInstanceOf[LogicalRelation].relation)
      case false => None
    }
  }

}

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.sources.{BaseRelation, LogicalRelation}

/**
 * FIXME: Workaround to the lack of visibility of some Spark SQL APIs.
 * E.g.
 * https://issues.apache.org/jira/browse/SPARK-7275
 */
object IsLogicalRelation {

  def unapply(plan: LogicalPlan): Option[BaseRelation] = plan match {
    case LogicalRelation(baseRelation) => Some(baseRelation)
    case _ => None
  }

}

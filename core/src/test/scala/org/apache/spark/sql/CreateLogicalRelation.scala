package org.apache.spark.sql

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.sources.{BaseRelation, LogicalRelation}

/**
 * FIXME: Workaround until LogicalRelation is public.
 */
object CreateLogicalRelation {

  def apply(base: BaseRelation): LogicalPlan =
    LogicalRelation(base)

}

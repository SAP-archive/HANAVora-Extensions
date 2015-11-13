package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.sources.BaseRelation

/**
 * FIXME: Workaround until LogicalRelation is public.
 */
object CreateLogicalRelation {

  def apply(base: BaseRelation): LogicalPlan =
    LogicalRelation(base)

}

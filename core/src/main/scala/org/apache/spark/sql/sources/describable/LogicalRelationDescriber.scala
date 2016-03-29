package org.apache.spark.sql.sources.describable

import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.StructField

/**
  * A [[Describable]] that describes the given [[LogicalRelation]] as well as
  * its [[org.apache.spark.sql.sources.BaseRelation]]
  *
  * @param plan The [[LogicalRelation]] to describe.
  */
case class LogicalRelationDescriber(plan: LogicalRelation)
  extends LogicalPlanDescribable {
  override def additionalValues: Seq[Any] = relation.describe() :: Nil

  val relation = Describable(plan.relation)

  override def additionalFields: Seq[StructField] =
    StructField("relation", relation.describeOutput) :: Nil
}

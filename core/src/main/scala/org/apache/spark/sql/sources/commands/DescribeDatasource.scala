package org.apache.spark.sql.sources.commands

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Attribute}
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import org.apache.spark.sql.types.{MetadataBuilder, StringType}

/**
 * Command to get debugging information about a specific table.
 */
case class DescribeDatasource(relation: UnresolvedRelation) extends LogicalPlan with Command {
  override def output: Seq[Attribute] = Seq(
    AttributeReference(
      "key",
      StringType,
      nullable = false,
      new MetadataBuilder()
        .putString("comment", "Attribute that is described").build())(),
    AttributeReference(
      "value",
      StringType,
      nullable = true,
      new MetadataBuilder()
        .putString("comment", "Value of the description").build())()
  )

  override def children: Seq[LogicalPlan] = Seq.empty
}

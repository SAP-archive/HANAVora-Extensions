package org.apache.spark.sql.sources.commands

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Attribute}
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import org.apache.spark.sql.types.{StructField, StructType, MetadataBuilder, StringType}

/**
  * The representation of ''SHOW TABLES ... USING '' in the logical plan.
  *
  * @param provider The data source class identifier.
  * @param options The options map.
  */
private[sql]
case class ShowTablesUsingCommand(provider: String, options: Map[String, String])
  extends LogicalPlan
  with Command {

  override def output: Seq[Attribute] = StructType(
    StructField("TABLE_NAME", StringType, nullable = false) ::
    StructField("IS_TEMPORARY", StringType, nullable = false) ::
    StructField("KIND", StringType, nullable = false) ::
    Nil
  ).toAttributes

  override def children: Seq[LogicalPlan] = Seq.empty
}

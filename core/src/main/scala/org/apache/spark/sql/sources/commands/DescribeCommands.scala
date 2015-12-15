package org.apache.spark.sql.sources.commands

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import org.apache.spark.sql.types._

/**
 * A command for describing a table.
 * @param relation The relation name.
 */
// TODO (AC) Remove this once table-valued function are rebased on top.
private[sql] case class DescribeRelationCommand(relation: UnresolvedRelation)
  extends LogicalPlan with Command {

  override def output: Seq[Attribute] = DescribeCommand.output

  override def children: Seq[LogicalPlan] = Seq.empty
}

/**
 * A command for describing a select query.
 * @param query The select query.
 */
private[sql] case class DescribeQueryCommand(query: LogicalPlan)
  extends LogicalPlan with Command {
  override def output: Seq[Attribute] = DescribeCommand.output

  override def children: Seq[LogicalPlan] = Seq.empty
}

private[this] object DescribeCommand {
  def output: Seq[Attribute] = StructType(
    StructField("NAME", StringType, nullable = false) ::
      StructField("POSITION", IntegerType, nullable = false) ::
      StructField("DATA_TYPE", StringType, nullable = false) ::
      StructField("ANNOTATION_KEY", StringType, nullable = true) ::
      StructField("ANNOTATION_VALUE", StringType, nullable = true) ::
      Nil).toAttributes
}


package org.apache.spark.sql.sources.commands

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * The representation of ''DESCRIBE TABLE ... USING '' in the logical plan.
  *
  * @param tblIdent The (qualified) name of the relation.
  * @param provider The data source class identifier.
  * @param options The options map.
  */
private[sql]
case class DescribeTableUsingCommand(tblIdent: TableIdentifier,
                                     provider: String,
                                     options: Map[String, String])
  extends LogicalPlan
    with Command {

  override def output: Seq[Attribute] = StructType(
    StructField("TABLE_NAME", StringType, nullable = false) ::
    StructField("DDL_STMT", StringType, nullable = false) ::
    Nil
  ).toAttributes

  override def children: Seq[LogicalPlan] = Seq.empty
}

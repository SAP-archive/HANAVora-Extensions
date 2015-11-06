package org.apache.spark.sql.sources.commands

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}

private[sql] case class RegisterTableUsing(
                                            tableName: String,
                                            provider: String,
                                            options: Map[String, String],
                                            ignoreConflict: Boolean)
  extends LogicalPlan
  with Command {
  override def output: Seq[Attribute] = Seq.empty

  override def children: Seq[LogicalPlan] = Seq.empty
}

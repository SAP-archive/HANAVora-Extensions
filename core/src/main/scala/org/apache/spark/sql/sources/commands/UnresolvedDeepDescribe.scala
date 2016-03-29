package org.apache.spark.sql.sources.commands

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan, UnaryNode}

/**
 * Command to get debugging information about a specific relation.
 */
case class UnresolvedDeepDescribe(relation: LogicalPlan)
  extends UnaryNode
  with Command {
  override def output: Seq[Attribute] = Nil

  override def child: LogicalPlan = relation

  override lazy val resolved: Boolean = false
}

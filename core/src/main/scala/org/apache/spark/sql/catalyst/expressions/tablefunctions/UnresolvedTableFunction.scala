package org.apache.spark.sql.catalyst.expressions.tablefunctions

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

case class UnresolvedTableFunction(name: String, arguments: Seq[LogicalPlan]) extends LogicalPlan {
  def children: Seq[LogicalPlan] = Seq.empty

  override def output: Seq[Attribute] = Seq.empty
}

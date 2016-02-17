package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

case class ResolvedTableFunction(f: TableFunction, arguments: Seq[LogicalPlan])
  extends LogicalPlan {

  override def output: Seq[Attribute] = f.output

  def children: Seq[LogicalPlan] = Seq.empty
}

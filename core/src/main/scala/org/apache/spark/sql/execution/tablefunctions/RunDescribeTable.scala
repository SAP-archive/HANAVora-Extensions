package org.apache.spark.sql.execution.tablefunctions

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.tablefunctions.DescribeTableStructure
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

/** Physical plan of describing a table
  *
  * @param rows The description result.
  */
case class RunDescribeTable(rows: Seq[Seq[Any]])
  extends PhysicalTableFunction {
  override protected def run(): Seq[Seq[Any]] = {
    rows
  }

  override lazy val output: Seq[Attribute] = DescribeTableStructure.output

  override def children: Seq[SparkPlan] = Seq.empty
}


package org.apache.spark.sql.execution.tablefunctions
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan

/**
  * Simple, predefined output of a table valued function that is not evaluated during runtime.
  *
  * @param data The data to output
  * @param output The structure of the output
  */
case class SimpleTableFunctionOutput(data: Seq[Seq[Any]], output: Seq[Attribute])
  extends PhysicalTableFunction {

  /** @inheritdoc */
  override protected def run(sqlContext: SQLContext): Seq[Seq[Any]] = data

  /** @inheritdoc */
  override def children: Seq[SparkPlan] = Seq.empty
}

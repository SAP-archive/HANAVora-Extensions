package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.types.{DataType, StringType}


case class DataSourceExpression(name: String, child: Seq[Expression]) extends Expression
{
  override type EvaluatedType = StringType
  override def eval(input: Row): EvaluatedType = {
    sys.error(s"Data source functions cannot be evaluated in Spark")
    }
  override def nullable: Boolean = true
  override def dataType: DataType = StringType
  override def children: Seq[Expression] = child
}

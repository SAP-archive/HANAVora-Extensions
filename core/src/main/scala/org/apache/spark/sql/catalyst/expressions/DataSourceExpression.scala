package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.compat.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.compat._
import org.apache.spark.sql.types.compat._


case class DataSourceExpression(name: String, children: Seq[Expression])
  extends BackportedExpression with CodegenFallback {
  override def eval(input: InternalRow): Any = {
    sys.error(s"Data source functions cannot be evaluated in Spark")
    }
  override def dataType: DataType = StringType
  override def nullable: Boolean = true
}

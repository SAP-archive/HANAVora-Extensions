package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._

case class DataSourceExpression(name: String, children: Seq[Expression])
  extends Expression with CodegenFallback {
  override def eval(input: InternalRow): Any = {
    sys.error(s"Data source functions cannot be evaluated in Spark")
    }
  override def dataType: DataType = StringType
  override def nullable: Boolean = true
}

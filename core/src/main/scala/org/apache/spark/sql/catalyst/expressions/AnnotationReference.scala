package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.{InternalRow, trees}
import org.apache.spark.sql.types.DataType

/**
  * A node that holds a reference to the metadata in another node.
  *
  * @param key The referenced metadata.
  * @param child The referenced node.
  *
  * TODO (YH) implement this in the resolution phase.
  */
case class AnnotationReference(key:String, child:NamedExpression) extends Expression
  with CodegenFallback {

  override def eval(input: InternalRow): Any = child.eval(input)

  override def nullable: Boolean = false

  override def dataType: DataType = child.dataType

  override lazy val resolved = child.resolved

  override def children: Seq[Expression] = child :: Nil
}

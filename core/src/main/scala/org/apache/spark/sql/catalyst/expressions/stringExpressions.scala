package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.compat.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.compat._
import org.apache.spark.unsafe.types.compat._
import org.apache.spark.sql.types.compat._

/** Return the se with all found sub-strings fe replaced by pe */
case class Replace(se: Expression, fe: Expression, pe: Expression)
  extends BackportedTernaryExpression
  with ImplicitCastInputTypes with CodegenFallback {

  override def inputTypes: Seq[AbstractDataType] = Seq.fill(3)(StringType)

  override def eval(input: InternalRow): Any = {
    val s = se.eval(input).asInstanceOf[UTF8String]
    val f = fe.eval(input).asInstanceOf[UTF8String]
    val p = pe.eval(input).asInstanceOf[UTF8String]
    (s, f, p) match {
      case (null, _, _) | (_, null, _) | (null, null, _) => null
      case (stre, strf, null) =>
        UTF8String.fromString(stre.toString()
          .replaceAllLiterally(strf.toString(), ""))
      case (stre, strf, strp) =>
        UTF8String.fromString(stre.toString()
          .replaceAllLiterally(strf.toString(), strp.toString()))
      case _ =>
        sys.error(s"Unexpected input")
    }
  }

  override def nullable: Boolean = se.nullable

  override def dataType: DataType = StringType

  override def children: Seq[Expression] = se :: fe :: pe :: Nil
}

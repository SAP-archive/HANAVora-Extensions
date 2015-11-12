package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.compat.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.compat._
import org.apache.spark.sql.types.{BinaryType, IntegerType, DataType, StringType}
import org.apache.spark.unsafe.types.compat._

//
// Backported from Spark 1.5.
//
// These classes are preserved equal to the Spark 1.5 implementation
// as much as possible. These are the exceptions:
//  - genCode methods are removed.
//  - ExpressionDescription annotations are removed.
//  - extends Expression replaced with BackportedExpression, BackportedUnaryExpression.
//  - Removed expressions that already existed in Spark 1.4 and those that we
//    did not support before.
//

// scalastyle:off

////////////////////////////////////////////////////////////////////////////////////////////////////
// This file defines expressions for string operations.
////////////////////////////////////////////////////////////////////////////////////////////////////

/**
  * An expression that concatenates multiple input strings into a single string.
  * If any input is null, concat returns null.
  */
case class Concat(children: Seq[Expression]) extends BackportedExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq.fill(children.size)(StringType)
  override def dataType: DataType = StringType

  override def nullable: Boolean = children.exists(_.nullable)
  override def foldable: Boolean = children.forall(_.foldable)

  override def eval(input: InternalRow): Any = {
    val inputs = children.map(_.eval(input).asInstanceOf[UTF8String])
    UTF8String.concat(inputs : _*)
  }

  /* XXX: REMOVED genCode */

}

/* XXX: REMOVED ConcatWs */

trait String2StringExpression extends ImplicitCastInputTypes {
  self: BackportedUnaryExpression with Product =>

  def convert(v: UTF8String): UTF8String

  override def dataType: DataType = StringType
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType)

  protected override def nullSafeEval(input: Any): Any =
    convert(input.asInstanceOf[UTF8String])
}

/* XXX: REMOVED Upper (already exists in Spark 1.4 */

/* XXX: REMOVED Lower (already exists in Spark 1.4 */

/** A base trait for functions that compare two strings, returning a boolean. */
trait StringPredicate extends Predicate with ImplicitCastInputTypes {
  self: BackportedBinaryExpression with Product =>

  /* XXX: Added EvaluatedType */
  override type EvaluatedType = Any

  def compare(l: UTF8String, r: UTF8String): Boolean

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType)

  protected override def nullSafeEval(input1: Any, input2: Any): Any =
    compare(input1.asInstanceOf[UTF8String], input2.asInstanceOf[UTF8String])

  override def toString: String = s"$nodeName($left, $right)"
}

/* XXX: REMOVED Contains (already exists in Spark 1.4 */

/* XXX: REMOVED StartsWith (already exists in Spark 1.4 */

/* XXX: REMOVED EndsWith (already exists in Spark 1.4 */

/* XXX: REMOVED StringTranslate */

/* XXX: REMOVED FindInSet */

/**
  * A function that trim the spaces from both ends for the specified string.
  */
case class StringTrim(child: Expression)
  extends BackportedUnaryExpression with String2StringExpression {

  def convert(v: UTF8String): UTF8String = v.trim()

  override def prettyName: String = "trim"

  /* XXX: REMOVED genCode */
}

/**
  * A function that trim the spaces from left end for given string.
  */
case class StringTrimLeft(child: Expression)
  extends BackportedUnaryExpression with String2StringExpression {

  def convert(v: UTF8String): UTF8String = v.trimLeft()

  override def prettyName: String = "ltrim"

  /* XXX: REMOVED genCode */
}

/**
  * A function that trim the spaces from right end for given string.
  */
case class StringTrimRight(child: Expression)
  extends BackportedUnaryExpression with String2StringExpression {

  def convert(v: UTF8String): UTF8String = v.trimRight()

  override def prettyName: String = "rtrim"

  /* XXX: REMOVED genCode */
}

/* XXX: REMOVED StringInstr */

/* XXX: REMOVED SubstringIndex */

/**
  * A function that returns the position of the first occurrence of substr
  * in given string after position pos.
  */
case class StringLocate(substr: Expression, str: Expression, start: Expression)
  extends BackportedTernaryExpression with ImplicitCastInputTypes with CodegenFallback {

  def this(substr: Expression, str: Expression) = this(substr, str, Literal(0))

  override def children: Seq[Expression] = substr :: str :: start :: Nil
  override def dataType: DataType = IntegerType
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType, IntegerType)

  override def eval(input: InternalRow): Any = {
    val s = start.eval(input)
    if (s == null) {
      // if the start position is null, we need to return 0, (conform to Hive)
      0
    } else {
      val r = substr.eval(input)
      if (r == null) {
        null
      } else {
        val l = str.eval(input)
        if (l == null) {
          null
        } else {
          l.asInstanceOf[UTF8String].indexOf(
            r.asInstanceOf[UTF8String],
            s.asInstanceOf[Int]) + 1
        }
      }
    }
  }

  override def prettyName: String = "locate"

  /* XXX: Add makeCopy to avoid constructor conflict */
  override def makeCopy(newArgs: Array[AnyRef]): StringLocate.this.type = {
    val e = newArgs.map(_.asInstanceOf[Expression])
    new StringLocate(e(0), e(1), e(2)).asInstanceOf[StringLocate.this.type]
  }
}

/**
  * Returns str, left-padded with pad to a length of len.
  */
case class StringLPad(str: Expression, len: Expression, pad: Expression)
  extends BackportedTernaryExpression with ImplicitCastInputTypes {

  override def children: Seq[Expression] = str :: len :: pad :: Nil
  override def dataType: DataType = StringType
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, IntegerType, StringType)

  override def nullSafeEval(str: Any, len: Any, pad: Any): Any = {
    str.asInstanceOf[UTF8String].lpad(len.asInstanceOf[Int], pad.asInstanceOf[UTF8String])
  }

  /* XXX: REMOVED genCode */

  override def prettyName: String = "lpad"
}

/**
  * Returns str, right-padded with pad to a length of len.
  */
case class StringRPad(str: Expression, len: Expression, pad: Expression)
  extends BackportedTernaryExpression with ImplicitCastInputTypes {

  override def children: Seq[Expression] = str :: len :: pad :: Nil
  override def dataType: DataType = StringType
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, IntegerType, StringType)

  override def nullSafeEval(str: Any, len: Any, pad: Any): Any = {
    str.asInstanceOf[UTF8String].rpad(len.asInstanceOf[Int], pad.asInstanceOf[UTF8String])
  }

  /* XXX: REMOVED genCode */

  override def prettyName: String = "rpad"
}

/* XXX: REMOVED FormatString */

/* XXX: REMOVED InitCap */

/* XXX: REMOVED StringRepeat */

/**
  * Returns the reversed given string.
  */
case class StringReverse(child: Expression)
  extends BackportedUnaryExpression with String2StringExpression {
  override def convert(v: UTF8String): UTF8String = v.reverse()

  override def prettyName: String = "reverse"

  /* XXX: REMOVED genCode */
}

/* XXX: REMOVED StringSpace */

/* XXX: REMOVED Substring (already exists in Spark 1.4) */

/**
  * A function that return the length of the given string or binary expression.
  */
case class Length(child: Expression)
  extends BackportedUnaryExpression with BackportedExpectsInputTypes {
  override def dataType: DataType = IntegerType
  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection(StringType, BinaryType))

  protected override def nullSafeEval(value: Any): Any = child.dataType match {
    case StringType => value.asInstanceOf[UTF8String].numChars
    case BinaryType => value.asInstanceOf[Array[Byte]].length
  }

  /* XXX: REMOVED genCode */
}

/* XXX: REMOVED Levenshtein */

/* XXX: REMOVED SoundEx */

/* XXX: REMOVED Ascii */

/* XXX: REMOVED Base64 */

/* XXX: REMOVED UnBase64 */

/* XXX: REMOVED Decode */

/* XXX: REMOVED Encode */

/* XXX: REMOVED FormatNumber */

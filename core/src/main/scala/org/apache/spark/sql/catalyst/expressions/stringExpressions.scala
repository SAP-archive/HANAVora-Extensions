package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.types.{UTF8String, DataType, IntegerType, StringType}

/** Return the e w/o initial and trailing white chars (spaces, CR, LF, tab) */
case class Trim(e: Expression) extends Expression {

  override type EvaluatedType = UTF8String

  override def eval(input: Row): EvaluatedType = {
    e.eval(input) match {
      case null => null
      case s: UTF8String => UTF8String(s.toString().trim)
      case other =>
        sys.error(s"Type $other does not support string operations")
    }
  }

  override def nullable: Boolean = e.nullable

  override def dataType: DataType = StringType

  override def children: Seq[Expression] = e :: Nil
}

/** Return the se w/o initial white chars (spaces, CR, LF, tab) */
case class LTrim(e: Expression) extends Expression {

  override type EvaluatedType = UTF8String

  override def eval(input: Row): EvaluatedType = {
    e.eval(input) match {
      case null => null
      case s: UTF8String =>
        UTF8String(s.toString()
          .dropWhile({ c => c == ' ' || c == '\t' || c == '\n' || c == '\r' }))
      case other =>
        sys.error(s"Type $other does not support string operations")
    }
  }

  override def nullable: Boolean = e.nullable

  override def dataType: DataType = StringType

  override def children: Seq[Expression] = e :: Nil
}

/** Return the e w/o trailing white chars (spaces, CR, LF, tab) */
case class RTrim(e: Expression) extends Expression {

  override type EvaluatedType = UTF8String

  override def eval(input: Row): EvaluatedType = {
    e.eval(input) match {
      case null => null
      case s: UTF8String =>
        UTF8String(s.toString().reverse
          .dropWhile({ c => c == ' ' || c == '\t' || c == '\n' || c == '\r' })
          .reverse)
      case other =>
        sys.error(s"Type $other does not support string operations")
    }
  }

  override def nullable: Boolean = e.nullable

  override def dataType: DataType = StringType

  override def children: Seq[Expression] = e :: Nil
}

/** Return the se reversed (last char is first) */
case class Reverse(e: Expression) extends Expression {

  override type EvaluatedType = UTF8String

  override def eval(input: Row): EvaluatedType = {
    e.eval(input) match {
      case null => null
      case s: UTF8String => UTF8String(s.toString().reverse)
      case other =>
        sys.error(s"Type $other does not support string operations")
    }
  }

  override def nullable: Boolean = e.nullable

  override def dataType: DataType = StringType

  override def children: Seq[Expression] = e :: Nil
}

/** Return the se right padded with pe so that it reaches le */
case class RPad(se: Expression, le: Expression, pe: Expression) extends Expression {

  override type EvaluatedType = UTF8String

  override def eval(input: Row): EvaluatedType = {
    se.eval(input) match {
      case null => null
      case s: UTF8String =>
        val l = le.eval(input)
        val len: Int = if (l == null) 0 else l.asInstanceOf[Int]
        var str = s.toString()
        if (str.length < len) {
          val p = pe.eval(input)
          val pattern = if (p == null) " " else p.toString

          // TODO
          while (str.length < len) str = str + pattern
        }
        if (len < str.length) {
          UTF8String(str.substring(0, len))
        } else {
          UTF8String(str)
        }
      case other =>
        sys.error(s"Type $other does not support string operations")
    }
  }

  override def nullable: Boolean = se.nullable

  override def dataType: DataType = StringType

  override def children: Seq[Expression] = se :: le :: pe :: Nil
}

/** Return the se left padded with pe so that it reaches le */
case class LPad(se: Expression, le: Expression, pe: Expression) extends Expression {

  override type EvaluatedType = UTF8String

  override def eval(input: Row): EvaluatedType = {
    se.eval(input) match {
      case null => null
      case s: UTF8String =>
        var str = s.toString()
        val l = le.eval(input)
        val len: Integer = if (l == null) 0 else l.asInstanceOf[Integer]
        if (str.length < len) {
          val p = pe.eval(input)
          val pattern = if (p == null) " " else p.toString

          // TODO
          while (str.length < len) str = pattern + str
        }
        if (len < str.length) {
          UTF8String(str.substring(0, len))
        } else {
          UTF8String(str)
        }
      case other =>
        sys.error(s"Type $other does not support string operations")
    }
  }

  override def nullable: Boolean = se.nullable

  override def dataType: DataType = StringType

  override def children: Seq[Expression] = se :: le :: pe :: Nil
}

/** Return the a1 and a2 concataned */
case class Concat(e1: Expression, e2: Expression) extends Expression {

  override type EvaluatedType = UTF8String

  override def eval(input: Row): EvaluatedType = {
    val str1 = e1.eval(input)
    val str2 = e2.eval(input)
    (str1, str2) match {
      case (null, _) | (_, null) | (null, null) => null
      case _ => UTF8String(str1.toString + str2.toString)
    }
  }

  override def nullable: Boolean = e1.nullable || e2.nullable

  override def dataType: DataType = StringType

  override def children: Seq[Expression] = e1 :: e2 :: Nil
}

/** Return the index of the first occurrence of p in s or -1 if none */
case class Locate(s: Expression, p: Expression) extends Expression {

  override type EvaluatedType = Integer

  override def eval(input: Row): EvaluatedType = {
    val strEval = s.eval(input)
    val patEval = p.eval(input)
    (strEval, patEval) match {
      case (null, _) | (_, null) | (null, null) => -1
      case (se: UTF8String, sp: UTF8String) => se.toString().indexOf(sp.toString())
      case _ => -1
    }
  }

  override def nullable: Boolean = s.nullable

  override def dataType: DataType = IntegerType

  override def children: Seq[Expression] = s :: p :: Nil
}

/** Return the se with all found sub-strings fe replaced by pe */
case class Replace(se: Expression, fe: Expression, pe: Expression) extends Expression {

  override type EvaluatedType = UTF8String

  override def eval(input: Row): EvaluatedType = {
    val s = se.eval(input)
    val f = fe.eval(input)
    val p = pe.eval(input)
    (s, f, p) match {
      case (null, _, _) | (_, null, _) | (null, null, _) => null
      case (stre: UTF8String, strf: UTF8String, null) =>
        UTF8String(stre.toString().replaceAll(strf.toString(), ""))
      case (stre: UTF8String, strf: UTF8String, strp: UTF8String) =>
        UTF8String(stre.toString().replaceAll(strf.toString(), strp.toString()))
      case _ => null
    }
  }

  override def nullable: Boolean = se.nullable

  override def dataType: DataType = StringType

  override def children: Seq[Expression] = se :: fe :: pe :: Nil
}

/** Return the s length */
case class Length(s: Expression) extends Expression {

  override type EvaluatedType = Integer

  override def eval(input: Row): EvaluatedType = {
    s.eval(input) match {
      case null => 0
      case s: UTF8String => s.length()
      case other
      => sys.error(s"Type $other does not support string operations")
    }
  }

  override def nullable: Boolean = s.nullable

  override def dataType: DataType = IntegerType

  override def children: Seq[Expression] = s :: Nil
}

/** Return the s as string */
case class ToVarChar(s: Expression) extends Expression {

  override type EvaluatedType = UTF8String

  override def eval(input: Row): EvaluatedType = {
    val strEval = s.eval(input)
    if (strEval == null) {
      null
    } else {
      UTF8String(strEval.toString)
    }
  }

  override def nullable: Boolean = s.nullable

  override def dataType: DataType = StringType

  override def children: Seq[Expression] = s :: Nil
}

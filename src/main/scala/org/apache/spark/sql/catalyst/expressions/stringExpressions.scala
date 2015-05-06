package org.apache.spark.sql.catalyst.expressions

import java.sql.Timestamp
import java.util.Calendar

import org.apache.spark.sql.catalyst.expressions.DateFlag._
import org.apache.spark.sql.types.{DataType, StringType, IntegerType}

/** Return the e w/o initial and trailing white chars (spaces, CR, LF, tab) */
case class Trim(e: Expression) extends Expression {

  override type EvaluatedType = String

  override def eval(input: Row): EvaluatedType = {
    e.eval(input) match {
      case null => null
      case s : String => s.trim
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

  override type EvaluatedType = String

  override def eval(input: Row): EvaluatedType = {
    e.eval(input) match {
      case null => null
      case s : String => {
        val l = s.length
        var i = 0
        while (i<l && s.charAt(i) <=' ') { 
          i = i+1
        }
        s.substring(i)
      }
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

  override type EvaluatedType = String

  override def eval(input: Row): EvaluatedType = {
    e.eval(input) match {
      case null => null
      case s : String => {
        var l = s.length
        while (0<l && s.charAt(l-1) <=' ') { 
          l = l-1
        }
        s.substring(0,l)
      }
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

  override type EvaluatedType = String

  override def eval(input: Row): EvaluatedType = {
    e.eval(input) match {
      case null => null
      case s : String => s.reverse
      case other =>
        sys.error(s"Type $other does not support string operations")
    }
  }

  override def nullable: Boolean = e.nullable
  override def dataType: DataType = StringType
  override def children: Seq[Expression] = e :: Nil
}

/** Return the se right padded with pe so that it reaches le */
case class RPad(se: Expression, le:Expression, pe: Expression) extends Expression {

  override type EvaluatedType = String

  override def eval(input: Row): EvaluatedType = {
    se.eval(input) match {
      case null => null
      case s : String => {
        val l = le.eval(input)
        val len : Int = if (l==null) 0 else l.asInstanceOf[Int]
        var str = s.asInstanceOf[String] 
        if (str.length < len) {
          val p = pe.eval(input)
          val pattern = if (p == null) " " else p.toString

          while (str.length < len) str = str + pattern
        }
        if (len < str.length)
          str.substring(0,len)
        else
          str
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
case class LPad(se: Expression, le:Expression, pe: Expression) extends Expression {

  override type EvaluatedType = String

  override def eval(input: Row): EvaluatedType = {
    se.eval(input) match {
      case null => null
      case s : String => {
        var str = s.asInstanceOf[String]
        val l = le.eval(input)
        val len : Integer = if (l==null) 0 else l.asInstanceOf[Integer]
        if (str.length < len) {
          val p = pe.eval(input)
          val pattern = if (p == null) " " else p.toString
  
          while (str.length < len) str = pattern + str
        }
        if (len < str.length)
          str.substring(0,len)
        else
          str
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

  override type EvaluatedType = String

  override def eval(input: Row): EvaluatedType = {
    val str1 = e1.eval(input)
    val str2 = e2.eval(input)
    (str1,str2) match {
      case (null,_) | (_,null) | (null,null) => null
      case (s1: String, s2: String) => {
        s1 + s2
      }
      case _ => str1.toString() + str2.toString()
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
    (strEval,patEval) match {
      case (null,_) | (_,null) | (null,null) => -1
      case (se: String, sp: String) => {
        se.indexOf(sp)
      }
      case _ => -1
    }
  }

  override def nullable: Boolean = s.nullable
  override def dataType: DataType = IntegerType
  override def children: Seq[Expression] = s :: p :: Nil
}

/** Return the se with all found sub-strings fe replaced by pe */
case class Replace(se: Expression, fe: Expression, pe:Expression) extends Expression {

  override type EvaluatedType = String

  override def eval(input: Row): EvaluatedType = {
    val s = se.eval(input)
    val f = fe.eval(input)
    val p = pe.eval(input)
    (s,f,p) match {
      case (null,_,_) | (_,null,_) | (null,null,_) => null
      case (stre: String, strf: String, null) => {
        stre.replaceAll(strf,"")
      }
      case (stre: String, strf: String, strp: String) => {
        stre.replaceAll(strf, strp)
      }
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
      case s : String => s.length
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

  override type EvaluatedType = String

  override def eval(input: Row): EvaluatedType = {
    val strEval = s.eval(input)
    if (strEval == null) {
      null
    } else {
      strEval.toString
    }
  }

  override def nullable: Boolean = s.nullable
  override def dataType: DataType = StringType
  override def children: Seq[Expression] = s :: Nil
}

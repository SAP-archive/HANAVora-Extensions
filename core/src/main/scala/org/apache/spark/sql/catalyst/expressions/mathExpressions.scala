package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType}
import org.apache.commons.math3.util.FastMath
import org.apache.spark.sql.types.NumericType

/** Return the neperian logarithm of child */
case class Ln(child: Expression) extends UnaryExpression {
  type EvaluatedType = Any

  override def dataType: DataType = DoubleType
  override def foldable: Boolean = child.foldable
  override def nullable: Boolean = true
  override def toString(): String = s"LN($child)"

  lazy val numeric = child.dataType match {
    case n: NumericType => n.numeric.asInstanceOf[Numeric[Any]]
    case other =>
      sys.error(s"Type ${other.getClass} does not support non-negative numeric operations")
  }

  override def eval(input: Row): Any = {
    val evalE = child.eval(input)
    if (evalE == null) {
      null
    } else {
      val value = numeric.toDouble(evalE)
      if (value < 0) null
      else FastMath.log(value)
    }
  }
}

/** Return the decimal logarithm of child */
case class Log(child: Expression) extends UnaryExpression {
  type EvaluatedType = Any

  override def dataType: DataType = DoubleType
  override def foldable: Boolean = child.foldable
  override def nullable: Boolean = true
  override def toString(): String = s"LN($child)"

  lazy val numeric = child.dataType match {
    case n: NumericType => n.numeric.asInstanceOf[Numeric[Any]]
    case other =>
      sys.error(s"Type ${other.getClass} does not support non-negative numeric operations")
  }

  override def eval(input: Row): Any = {
    val evalE = child.eval(input)
    if (evalE == null) {
      null
    } else {
      val value = numeric.toDouble(evalE)
      if (value < 0) null
      else FastMath.log10(value)
    }
  }
}

/** Return the d multiplied by itself p times */
case class Power(d: Expression, p: Expression) extends Expression {

  override type EvaluatedType = Double

  // scalastyle:off cyclomatic.complexity
  override def eval(input: Row): EvaluatedType = {
    val ce = d.eval(input) match {
       case null => 0.0
      case d: Double => d
      case l: Long   => l.toDouble
      case i: Integer=> i.toDouble
      case f: Float  => f.toDouble
      case other =>
        sys.error(s"Type ${other.getClass} does not support numeric operations")
    }
    val cp = p.eval(input) match {
      case null => 1.0
      case d: Double => d
      case l: Long   => l.toDouble
      case i: Integer=> i.toDouble
      case f: Float  => f.toDouble
      case other =>
        sys.error(s"Type ${other.getClass} does not support numeric operations")
    }
    FastMath.pow(ce, cp)
  }
  // scalastyle:on cyclomatic.complexity

  override def nullable: Boolean = d.nullable
  override def dataType: DataType = DoubleType
  override def children: Seq[Expression] = d :: p :: Nil
}

/** Return the cosinus of d */
case class Cos(d: Expression) extends Expression {

  override type EvaluatedType = Double

  override def eval(input: Row): EvaluatedType = {
    d.eval(input) match {
      case null => 1.0
      case d: Double => FastMath.cos(d)
      case l: Long   => FastMath.cos(l.toDouble)
      case i: Integer=> FastMath.cos(i.toDouble)
      case f: Float  => FastMath.cos(f.toDouble)
      case other =>
        sys.error(s"Type ${other.getClass} does not support numeric operations")
    }
  }

  override def nullable: Boolean = d.nullable
  override def dataType: DataType = DoubleType
  override def children: Seq[Expression] = d :: Nil
}

/** Return the arc-cosinus of d */
case class Acos(d: Expression) extends Expression {

  override type EvaluatedType = Double

  override def eval(input: Row): EvaluatedType = {
    d.eval(input) match {
      case null => 0.0
      case d: Double => FastMath.acos(d)
      case l: Long   => FastMath.acos(l.toDouble)
      case i: Integer=> FastMath.acos(i.toDouble)
      case f: Float  => FastMath.acos(f.toDouble)
      case other =>
        sys.error(s"Type ${other.getClass} does not support numeric operations")
    }
  }

  override def nullable: Boolean = d.nullable
  override def dataType: DataType = DoubleType
  override def children: Seq[Expression] = d :: Nil
}

/** Return the sinus of d */
case class Sin(d: Expression) extends Expression {

  override type EvaluatedType = Double

  override def eval(input: Row): EvaluatedType = {
    d.eval(input) match {
      case null => 0
      case d: Double => FastMath.sin(d)
      case l: Long   => FastMath.sin(l.toDouble)
      case i: Integer=> FastMath.sin(i.toDouble)
      case f: Float  => FastMath.sin(f.toDouble)
      case other =>
        sys.error(s"Type ${other.getClass} does not support numeric operations")
    }
  }

  override def nullable: Boolean = d.nullable
  override def dataType: DataType = DoubleType
  override def children: Seq[Expression] = d :: Nil
}

/** Return the arc-sinus of d */
case class Asin(d: Expression) extends Expression {

  override type EvaluatedType = Double

  override def eval(input: Row): EvaluatedType = {
    d.eval(input) match {
      case null => 0
      case d: Double => FastMath.asin(d)
      case l: Long   => FastMath.asin(l.toDouble)
      case i: Integer=> FastMath.asin(i.toDouble)
      case f: Float  => FastMath.asin(f.toDouble)
      case other =>
        sys.error(s"Type ${other.getClass} does not support numeric operations")
    }
  }

  override def nullable: Boolean = d.nullable
  override def dataType: DataType = DoubleType
  override def children: Seq[Expression] = d :: Nil
}

/** Return the tangent of d */
case class Tan(d: Expression) extends Expression {

  override type EvaluatedType = Double

  override def eval(input: Row): EvaluatedType = {
    d.eval(input) match {
      case null => 0
      case d: Double => FastMath.tan(d)
      case l: Long   => FastMath.tan(l.toDouble)
      case i: Integer=> FastMath.tan(i.toDouble)
      case f: Float  => FastMath.tan(f.toDouble)
      case other =>
        sys.error(s"Type ${other.getClass} does not support numeric operations")
    }
  }

  override def nullable: Boolean = d.nullable
  override def dataType: DataType = DoubleType
  override def children: Seq[Expression] = d :: Nil
}

/** Return the arc-tangent of d */
case class Atan(d: Expression) extends Expression {

  override type EvaluatedType = Double

  override def eval(input: Row): EvaluatedType = {
    d.eval(input) match {
      case null => 0
      case d: Double => FastMath.atan(d)
      case l: Long   => FastMath.atan(l.toDouble)
      case i: Integer=> FastMath.atan(i.toDouble)
      case f: Float  => FastMath.atan(f.toDouble)
      case other =>
        sys.error(s"Type ${other.getClass} does not support numeric operations")
    }
  }

  override def nullable: Boolean = d.nullable
  override def dataType: DataType = DoubleType
  override def children: Seq[Expression] = d :: Nil
}

case class Ceil(d: Expression) extends Expression {

  override type EvaluatedType = Double

  override def eval(input: Row): EvaluatedType = {
    d.eval(input) match {
      case null => 0
      case d: Double => FastMath.ceil(d)
      case l: Long   => FastMath.ceil(l.toDouble)
      case i: Integer=> FastMath.ceil(i.toDouble)
      case f: Float  => FastMath.ceil(f)
      case other =>
        sys.error(s"Type ${other.getClass} does not support numeric operations")
    }
  }

  override def nullable: Boolean = d.nullable
  override def dataType: DataType = DoubleType
  override def children: Seq[Expression] = d :: Nil
}

/** Return the d rounded with dec precision */
case class Round(d: Expression, dec: Expression) extends Expression {

  private val RAISED_NUMBER = 10

  override type EvaluatedType = Double

  // scalastyle:off cyclomatic.complexity
  override def eval(input: Row): EvaluatedType = {
    val de = d.eval(input)
    if (de == null) {
      0
    } else {
      val n: Int = dec.eval(input) match {
        case null => 0
        case i: Integer=> i
        case d: Double => d.toInt
        case l: Long   => l.toInt
        case other =>
          sys.error(s"Type ${other.getClass} does not support numeric operations")
      }
      val m = FastMath.pow(RAISED_NUMBER,n)
      de match {
        case d: Double => FastMath.round(d*m) / m
        case l: Long   => l
        case i: Integer=> i.toLong
        case f: Float  => FastMath.round(f*m) / m
        case other =>
          sys.error(s"Type ${other.getClass} does not support numeric operations")
      }
    }
  }
  // scalastyle:on cyclomatic.complexity

  override def nullable: Boolean = d.nullable
  override def dataType: DataType = DoubleType
  override def children: Seq[Expression] = d :: dec :: Nil
}

/** Return the d floored */
case class Floor(d: Expression) extends Expression {

  override type EvaluatedType = Double

  override def eval(input: Row): EvaluatedType = {
    d.eval(input) match {
      case null => 0.0
      case d: Double => FastMath.floor(d)
      case l: Long   => l.toDouble
      case i: Integer=> i.toDouble
      case other =>
        sys.error(s"Type ${other.getClass} does not support numeric operations")
    }
  }

  override def nullable: Boolean = d.nullable

  override def dataType: DataType = DoubleType

  override def children: Seq[Expression] = d :: Nil
}

/** Return the d sign */
case class Sign(d: Expression) extends Expression {

  override type EvaluatedType = Double

  override def eval(input: Row): EvaluatedType = {
    d.eval(input) match {
      case null => 0
      case d: Double => FastMath.signum(d)
      case l: Long   => FastMath.signum(l.toDouble)
      case i: Integer=> FastMath.signum(i.toDouble)
      case f: Float  => FastMath.signum(f)
      case other =>
        sys.error(s"Type ${other.getClass} does not support numeric operations")
    }
  }

  override def nullable: Boolean = d.nullable
  override def dataType: DataType = DoubleType
  override def children: Seq[Expression] = d :: Nil
}

/** Return the d as double */
case class ToDouble(s: Expression) extends Expression {

  override type EvaluatedType = Double

  override def eval(input: Row): EvaluatedType = {
    s.eval(input) match {
      case null => 0
      case s: String => s.toDouble
      case d: Double => d
      case l: Long   => l.toDouble
      case i: Integer=> i.toDouble
      case f: Float  => f.toDouble
      case other =>
        sys.error(s"Type ${other.getClass} does not support numeric operations")
    }
  }

  override def nullable: Boolean = s.nullable
  override def dataType: DataType = DoubleType
  override def children: Seq[Expression] = s :: Nil
}

/** Return the d as integer */
case class ToInteger(s: Expression) extends Expression {

  override type EvaluatedType = Integer

  override def eval(input: Row): EvaluatedType = {
    s.eval(input) match {
      case null => 0
      case s: String => s.toInt
      case d: Double => d.toInt
      case l: Long   => l.toInt
      case i: Integer=> i
      case f: Float  => f.toInt
      case other =>
        sys.error(s"Type ${other.getClass} does not support numeric operations")
    }
  }

  override def nullable: Boolean = s.nullable
  override def dataType: DataType = IntegerType
  override def children: Seq[Expression] = s :: Nil
}

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.compat.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.compat._

/* XXX: Preserve these inports. Required for Spark 1.5 compatibility. */
import org.apache.spark.sql.types.{DecimalType, IntegerType, DataType, NumericType}
import org.apache.spark.sql.types.{DoubleType, ShortType}
import org.apache.spark.sql.types.{ByteType, LongType, FloatType, Decimal}

// scalastyle:off

//
// Backported from Spark 1.5.
//

/**
  * A leaf expression specifically for math constants. Math constants expect no input.
  *
  * There is no code generation because they should get constant folded by the optimizer.
  *
  * @param c The math constant.
  * @param name The short name of the function
  */
abstract class LeafMathExpression(c: Double, name: String)
  extends BackportedLeafExpression with CodegenFallback {
  /* XXX: Added Product */
  self: Product =>

  override def dataType: DataType = DoubleType
  override def foldable: Boolean = true
  override def nullable: Boolean = false
  override def toString: String = s"$name()"

  override def eval(input: InternalRow): Any = c
}

/**
  * A unary expression specifically for math functions. Math Functions expect a specific type of
  * input format, therefore these functions extend `ExpectsInputTypes`.
  * @param f The math function.
  * @param name The short name of the function
  */
abstract class UnaryMathExpression(f: Double => Double, name: String)
  extends BackportedUnaryExpression with Serializable with ImplicitCastInputTypes {
  /* XXX: Added Product */
  self: Product =>

  override def inputTypes: Seq[AbstractDataType] = Seq(DoubleType)
  override def dataType: DataType = DoubleType
  override def nullable: Boolean = true
  override def toString: String = s"$name($child)"

  protected override def nullSafeEval(input: Any): Any = {
    f(input.asInstanceOf[Double])
  }

  // name of function in java.lang.Math
  def funcName: String = name.toLowerCase

  /* XXX: REMOVED codeGen */
}

abstract class UnaryLogExpression(f: Double => Double, name: String)
  extends UnaryMathExpression(f, name) {
  /* XXX: Added Product */
  self: Product =>

  // values less than or equal to yAsymptote eval to null in Hive, instead of NaN or -Infinity
  protected val yAsymptote: Double = 0.0

  protected override def nullSafeEval(input: Any): Any = {
    val d = input.asInstanceOf[Double]
    if (d <= yAsymptote) null else f(d)
  }

  /* XXX: REMOVED codeGen */
}

/**
  * A binary expression specifically for math functions that take two `Double`s as input and returns
  * a `Double`.
  * @param f The math function.
  * @param name The short name of the function
  */
abstract class BinaryMathExpression(f: (Double, Double) => Double, name: String)
  extends BackportedBinaryExpression with Serializable with ImplicitCastInputTypes {
  /* XXX: Added Product */
  self: Product =>

  override def inputTypes: Seq[AbstractDataType] = Seq(DoubleType, DoubleType)

  override def toString: String = s"$name($left, $right)"

  override def dataType: DataType = DoubleType

  protected override def nullSafeEval(input1: Any, input2: Any): Any = {
    f(input1.asInstanceOf[Double], input2.asInstanceOf[Double])
  }

  /* XXX: REMOVED codeGen */
}

////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////
// Leaf math functions
////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////

/**
  * Euler's number. Note that there is no code generation because this is only
  * evaluated by the optimizer during constant folding.
  */
case class EulerNumber() extends LeafMathExpression(math.E, "E")

/**
  * Pi. Note that there is no code generation because this is only
  * evaluated by the optimizer during constant folding.
  */
case class Pi() extends LeafMathExpression(math.Pi, "PI")

////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////
// Unary math functions
////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////

/*
 * XXX: REMOVED (already in Spark 1.4):
 * Acos, Asin, Atan, Cbrt, Ceil, Cos, Cosh
 */

/* XXX: REMOVED Conf */

/*
 * XXX: REMOVED (already in Spark 1.4):
 * Exp, Expm1, Floor
 */

/* XXX: REMOVED Factorial */

/*
 * XXX: REMOVED (already in Spark 1.4):
 * Log, Log10, Log1p, Rint, Signum,
 * Sin, Sinh, Sqrt, Tan, Tanh,
 * ToDegrees, ToRadians
 */

/* XXX: REMOVED Bin, Hex, UnHex */

////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////
// Binary math functions
////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////

/* XXX: REMOVED Atan2 */

/* XXX: REMOVED (already in Spark 1.4): Pow */

/* XXX: REMOVED ShiftLeft, ShiftRight, ShiftRightUnsigned */

/* XXX: REMOVED (already in Spark 1.4): Hypot */

/**
  * Computes the logarithm of a number.
  * @param left the logarithm base, default to e.
  * @param right the number to compute the logarithm of.
  */
case class Logarithm(left: Expression, right: Expression)
  extends BinaryMathExpression((c1, c2) => math.log(c2) / math.log(c1), "LOG") {

  def this(child: Expression) = this(EulerNumber(), child)

  protected override def nullSafeEval(input1: Any, input2: Any): Any = {
    val dLeft = input1.asInstanceOf[Double]
    val dRight = input2.asInstanceOf[Double]
    // Unlike Hive, we support Log base in (0.0, 1.0]
    if (dLeft <= 0.0 || dRight <= 0.0) null else math.log(dRight) / math.log(dLeft)
  }

  /* XXX: Added makeCopy, otherwise the constructors give problems */
  override def makeCopy(newArgs: Array[AnyRef]): this.type =
    if (newArgs.length != 2) {
      sys.error("Logarithm requires 2 arguments")
    } else {
      val e = newArgs.map(_.asInstanceOf[Expression])
      Logarithm(e(0), e(1)).asInstanceOf[this.type]
    }

  /* XXX: REMOVED codeGen */
}

/**
  * Round the `child`'s result to `scale` decimal place when `scale` >= 0
  * or round at integral part when `scale` < 0.
  * For example, round(31.415, 2) = 31.42 and round(31.415, -1) = 30.
  *
  * Child of IntegralType would round to itself when `scale` >= 0.
  * Child of FractionalType whose value is NaN or Infinite would always round to itself.
  *
  * Round's dataType would always equal to `child`'s dataType except for DecimalType,
  * which would lead scale decrease from the origin DecimalType.
  *
  * @param child expr to be round, all [[NumericType]] is allowed as Input
  * @param scale new scale to be round to, this should be a constant int at runtime
  */
case class Round(child: Expression, scale: Expression)
  extends BackportedBinaryExpression with ImplicitCastInputTypes {

  import BigDecimal.RoundingMode.HALF_UP

  /* XXX: Moved default constructor to companion object */

  override def left: Expression = child
  override def right: Expression = scale

  // round of Decimal would eval to null if it fails to `changePrecision`
  override def nullable: Boolean = true

  override def foldable: Boolean = child.foldable

  override lazy val dataType: DataType = child.dataType match {
    // if the new scale is bigger which means we are scaling up,
    // keep the original scale as `Decimal` does
    case DecimalType.Fixed(p, s) => DecimalType(p, if (_scale > s) s else _scale)
    case t => t
  }

  /* XXX: Changed NumericType to DoubleType */
  override def inputTypes: Seq[AbstractDataType] = Seq(DoubleType, IntegerType)

  /* XXX: Removed checkInputDataTypes */

  // Avoid repeated evaluation since `scale` is a constant int,
  // avoid unnecessary `child` evaluation in both codegen and non-codegen eval
  // by checking if scaleV == null as well.
  private lazy val scaleV: Any = scale.eval(EmptyRow)
  private lazy val _scale: Int = scaleV.asInstanceOf[Int]

  override def eval(input: InternalRow): Any = {
    if (scaleV == null) { // if scale is null, no need to eval its child at all
      null
    } else {
      val evalE = child.eval(input)
      if (evalE == null) {
        null
      } else {
        nullSafeEval(evalE)
      }
    }
  }

  // not overriding since _scale is a constant int at runtime
  def nullSafeEval(input1: Any): Any = {
    child.dataType match {
      case _: DecimalType =>
        val decimal = input1.asInstanceOf[Decimal]
        if (decimal.changePrecision(decimal.precision, _scale)) decimal else null
      case ByteType =>
        BigDecimal(input1.asInstanceOf[Byte]).setScale(_scale, HALF_UP).toByte
      case ShortType =>
        BigDecimal(input1.asInstanceOf[Short]).setScale(_scale, HALF_UP).toShort
      case IntegerType =>
        BigDecimal(input1.asInstanceOf[Int]).setScale(_scale, HALF_UP).toInt
      case LongType =>
        BigDecimal(input1.asInstanceOf[Long]).setScale(_scale, HALF_UP).toLong
      case FloatType =>
        val f = input1.asInstanceOf[Float]
        if (f.isNaN || f.isInfinite) {
          f
        } else {
          BigDecimal(f).setScale(_scale, HALF_UP).toFloat
        }
      case DoubleType =>
        val d = input1.asInstanceOf[Double]
        if (d.isNaN || d.isInfinite) {
          d
        } else {
          BigDecimal(d).setScale(_scale, HALF_UP).toDouble
        }
    }
  }

  /* XXX: REMOVED codeGen */
}

object Round {
  def apply(child: Expression): Round = Round(child, Literal(0))
}

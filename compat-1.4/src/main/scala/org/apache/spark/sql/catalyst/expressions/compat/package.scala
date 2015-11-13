package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.types.compat._

package object compat {
  type ScalaUDF = org.apache.spark.sql.catalyst.expressions.ScalaUdf
  val ScalaUDF = org.apache.spark.sql.catalyst.expressions.ScalaUdf

  implicit def dataType2AbstractDataType(dt: DataType): AbstractDataType =
    ConcreteDataType(dt)

  type Acos = mathfuncs.Acos
  val Acos = mathfuncs.Acos
  type Asin = mathfuncs.Asin
  val Asin = mathfuncs.Asin
  type Atan = mathfuncs.Atan
  val Atan = mathfuncs.Atan
  type Atan2 = mathfuncs.Atan2
  val Atan2 = mathfuncs.Atan2
  type BinaryMathExpression = mathfuncs.BinaryMathExpression
  type Cbrt = mathfuncs.Cbrt
  val Cbrt = mathfuncs.Cbrt
  type Ceil = mathfuncs.Ceil
  val Ceil = mathfuncs.Ceil
  type Cos = mathfuncs.Cos
  val Cos = mathfuncs.Cos
  type Cosh = mathfuncs.Cosh
  val Cosh = mathfuncs.Cosh
  type Exp = mathfuncs.Exp
  val Exp = mathfuncs.Exp
  type Expm1 = mathfuncs.Expm1
  val Expm1 = mathfuncs.Expm1
  type Floor = mathfuncs.Floor
  val Floor = mathfuncs.Floor
  type Hypot = mathfuncs.Hypot
  val Hypot = mathfuncs.Hypot
  type Log = mathfuncs.Log
  val Log = mathfuncs.Log
  type Log1p = mathfuncs.Log1p
  val Log1p = mathfuncs.Log1p
  type Log10 = mathfuncs.Log10
  val Log10 = mathfuncs.Log10
  type Pow = mathfuncs.Pow
  val Pow = mathfuncs.Pow
  type Rint = mathfuncs.Rint
  val Rint = mathfuncs.Rint
  type Signum = mathfuncs.Signum
  val Signum = mathfuncs.Signum
  type Sin = mathfuncs.Sin
  val Sin = mathfuncs.Sin
  type Sinh = mathfuncs.Sinh
  val Sinh = mathfuncs.Sinh
  type Tan = mathfuncs.Tan
  val Tan = mathfuncs.Tan
  type Tanh = mathfuncs.Tanh
  val Tanh = mathfuncs.Tanh
  type ToDegrees = mathfuncs.ToDegrees
  val ToDegrees = mathfuncs.ToDegrees
  type ToRadians = mathfuncs.ToRadians
  val ToRadians = mathfuncs.ToRadians
  type UnaryMathExpression = mathfuncs.UnaryMathExpression

}

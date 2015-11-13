package org.apache.spark.sql.catalyst.expressions

package object compat {

  type BackportedExpression = Expression
  type BackportedLeafExpression = LeafExpression
  type BackportedUnaryExpression = UnaryExpression
  type BackportedBinaryExpression = BinaryExpression
  type BackportedTernaryExpression = TernaryExpression
  type ImplicitCastInputTypes = org.apache.spark.sql.catalyst.expressions.ImplicitCastInputTypes

  type ScalaUDF = org.apache.spark.sql.catalyst.expressions.ScalaUDF
  val ScalaUDF = org.apache.spark.sql.catalyst.expressions.ScalaUDF

  type AbstractDataType = org.apache.spark.sql.types.AbstractDataType

}

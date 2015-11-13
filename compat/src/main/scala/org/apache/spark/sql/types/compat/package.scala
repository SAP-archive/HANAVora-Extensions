package org.apache.spark.sql.types

/**
  * Provides access to all types in [[org.apache.spark.sql.types]]
  * that are present in every supported Spark version.
  *
  * Replace the use of [[org.apache.spark.sql.types]] with this package:
  * {{{
  *   import org.apache.spark.sql.compat._
  * }}}
  */
package object compat {

  //
  // Alias every type that is present both on Spark 1.4 and 1.5.
  //

  type ArrayType = org.apache.spark.sql.types.ArrayType
  val ArrayType = org.apache.spark.sql.types.ArrayType
  type AtomicType = org.apache.spark.sql.types.AtomicType
  type BinaryType = org.apache.spark.sql.types.BinaryType
  val BinaryType = org.apache.spark.sql.types.BinaryType
  type BooleanType = org.apache.spark.sql.types.BooleanType
  val BooleanType = org.apache.spark.sql.types.BooleanType
  type ByteType = org.apache.spark.sql.types.ByteType
  val ByteType = org.apache.spark.sql.types.ByteType
  type DataType = org.apache.spark.sql.types.DataType
  val DataType = org.apache.spark.sql.types.DataType
  type DateType = org.apache.spark.sql.types.DateType
  val DateType = org.apache.spark.sql.types.DateType
  type DataTypeException = org.apache.spark.sql.types.DataTypeException
  type DataTypeParser = org.apache.spark.sql.types.DataTypeParser
  val DataTypeParser = org.apache.spark.sql.types.DataTypeParser
  type DataTypes = org.apache.spark.sql.types.DataTypes
  type Decimal = org.apache.spark.sql.types.Decimal
  val Decimal = org.apache.spark.sql.types.Decimal
  type DecimalType = org.apache.spark.sql.types.DecimalType
  val DecimalType = org.apache.spark.sql.types.DecimalType
  type DoubleType = org.apache.spark.sql.types.DoubleType
  val DoubleType = org.apache.spark.sql.types.DoubleType
  type FloatType = org.apache.spark.sql.types.FloatType
  val FloatType = org.apache.spark.sql.types.FloatType
  type FractionalType = org.apache.spark.sql.types.FractionalType
  val FractionalType = org.apache.spark.sql.types.FractionalType
  type IntegralType = org.apache.spark.sql.types.IntegralType
  val IntegralType = org.apache.spark.sql.types.IntegralType
  type IntegerType = org.apache.spark.sql.types.IntegerType
  val IntegerType = org.apache.spark.sql.types.IntegerType
  type LongType = org.apache.spark.sql.types.LongType
  val LongType = org.apache.spark.sql.types.LongType
  type MapType = org.apache.spark.sql.types.MapType
  val MapType = org.apache.spark.sql.types.MapType
  type Metadata = org.apache.spark.sql.types.Metadata
  val Metadata = org.apache.spark.sql.types.Metadata
  type MetadataBuilder = org.apache.spark.sql.types.MetadataBuilder
  type NullType = org.apache.spark.sql.types.NullType
  val NullType = org.apache.spark.sql.types.NullType
  type NumericType = org.apache.spark.sql.types.NumericType
  val NumericType = org.apache.spark.sql.types.NumericType
  type ShortType = org.apache.spark.sql.types.ShortType
  val ShortType = org.apache.spark.sql.types.ShortType
  type StringType = org.apache.spark.sql.types.StringType
  val StringType = org.apache.spark.sql.types.StringType
  type StructType = org.apache.spark.sql.types.StructType
  val StructType = org.apache.spark.sql.types.StructType
  type StructField = org.apache.spark.sql.types.StructField
  val StructField = org.apache.spark.sql.types.StructField
  type TimestampType = org.apache.spark.sql.types.TimestampType
  val TimestampType = org.apache.spark.sql.types.TimestampType
  type UserDefinedType[T] = org.apache.spark.sql.types.UserDefinedType[T]
  type SQLUserDefinedType = org.apache.spark.sql.types.SQLUserDefinedType

  // UTF8String moved in Spark 1.5.
  // type UTF8String = org.apache.spark.sql.types.UTF8String
  // val UTF8String = org.apache.spark.sql.types.UTF8String
}

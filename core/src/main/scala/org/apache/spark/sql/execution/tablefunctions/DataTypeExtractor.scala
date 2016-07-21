package org.apache.spark.sql.execution.tablefunctions

import org.apache.spark.sql.sources.sql.SqlBuilder
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.GenericUtil._

/**
  * An extractor for various properties of Spark [[DataType]]s.
  *
  * @param dataType The [[DataType]] to extract from.
  */
case class DataTypeExtractor(dataType: DataType) {
  private lazy val sqlBuilder = new SqlBuilder

  /**
    * Returns the inferred matching standard SQL type to the given spark type.
    *
    * @return The inferred SQL type string.
    */
  def inferredSqlType: String = dataType match {
    case _: NodeType => "<INTERNAL>"
    case default => sqlBuilder.typeToSql(default)
  }

  // scalastyle:off magic.number
  /** Returns the numeric precision of the data type.
    *
    * The numeric precision refers to the maximum number of digits
    * that can be present in a number.
    * @return The numeric precision of the data type
    */
  def numericPrecision: Option[Int] = dataType matchOptional {
    case d: DecimalType => d.precision
    case _: IntegerType => 32 // Maximum number of digits as seen in binary
    case _: DoubleType => 53
    case _: FloatType => 24
    case _: LongType => 64
  }

  /** Returns the numeric precision radix of the data type.
    *
    * The numeric precision radix refers to the base of which the
    * data type is.
    * @return The numeric precision radix of the data type
    */
  def numericPrecisionRadix: Option[Int] = dataType matchOptional {
    case _: DecimalType => 10
    case _: FloatType => 2
    case _: IntegerType => 2
    case _: DoubleType => 2
    case _: LongType => 2
  }

  /** Returns the numeric scale of the data type.
    *
    * The numeric scale refers to the maximum number of decimal
    * places the data type can represent.
    * @return The numeric scale of the data type
    */
  def numericScale: Option[Int] = dataType matchOptional {
    case p: DecimalType => p.scale
    case _: IntegerType => 0
    case _: LongType => 0
  }
  // scalastyle:on magic.number
}

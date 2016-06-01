package org.apache.spark.sql.execution.tablefunctions

import org.apache.spark.sql.sources.sql.SqlBuilder
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.GenericUtil._

case class FieldExtractor(index: Int, field: Field, checkStar: Boolean) {
  def tableName: String = field.tableName

  def name: String = field.name

  lazy val sqlBuilder = new SqlBuilder()

  // TODO (YH, AC): Improve it once native types have landed
  def dataType: String = {
    field.dataType match {
      case NodeType => "<INTERNAL>"
      case _ => sqlBuilder.typeToSql(field.dataType)
    }
  }

  lazy val annotations: Map[String, String] =
    MetadataAccessor.metadataToMap(field.metadata)
                    .filter {
                      case (k, v) if checkStar => k != "*"
                      case _ => true
                    }
                    .mapValues(_.toString)

  def isNullable: Boolean = field.isNullable

  // scalastyle:off magic.number
  /** Returns the numeric precision of the data type.
    *
    * The numeric precision refers to the maximum number of digits
    * that can be present in a number.
    * @return The numeric precision of the data type
    */
  def numericPrecision: Option[Int] = field.dataType matchOptional {
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
  def numericPrecisionRadix: Option[Int] = field.dataType matchOptional {
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
  def numericScale: Option[Int] = field.dataType matchOptional {
    case p: DecimalType => p.scale
    case _: IntegerType => 0
    case _: LongType => 0
  }
  // scalastyle:on magic.number

  def extract(): Seq[Seq[Any]] = {
    // This step assures that there are rows in the first place
    val nonEmptyAnnotations = if (annotations.isEmpty) Map(None -> None) else annotations
    nonEmptyAnnotations.map {
      case (key, value) =>
        tableName ::
          name ::
          index ::
          isNullable ::
          dataType ::
          numericPrecision ::
          numericPrecisionRadix ::
          numericScale ::
          key ::
          value :: Nil
    }.toSeq
  }
}

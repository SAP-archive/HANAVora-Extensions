package org.apache.spark.sql.execution.tablefunctions

import org.apache.spark.sql.sources.sql.SqlBuilder
import org.apache.spark.sql.types.{DecimalType, MetadataAccessor}
import org.apache.spark.sql.util.GenericUtil._

case class FieldExtractor(index: Int, field: Field, checkStar: Boolean) {
  def tableName: String = field.tableName

  def name: String = field.name

  // TODO (YH, AC): Improve it once native types have landed
  def dataType: String = new SqlBuilder().typeToSql(field.dataType)

  lazy val annotations: Map[String, String] =
    MetadataAccessor.metadataToMap(field.metadata)
                    .filter {
                      case (k, v) if checkStar => k != "*"
                      case _ => true
                    }
                    .mapValues(_.toString)

  def isNullable: Boolean = field.isNullable

  def numericPrecision: Option[Int] = field.dataType matchOptional {
    case p: DecimalType => p.precision
  }

  def numericScale: Option[Int] = field.dataType matchOptional {
    case p: DecimalType => p.scale
  }

  def extract(): Seq[Seq[Any]] = {
    // This step assures that there are rows in the first place
    val nonEmptyAnnotations = if (annotations.isEmpty) Map("" -> "") else annotations
    nonEmptyAnnotations.map {
      case (key, value) =>
        tableName ::
          name ::
          index ::
          isNullable ::
          dataType ::
          numericPrecision ::
          numericScale ::
          key ::
          value :: Nil
    }.toSeq
  }
}

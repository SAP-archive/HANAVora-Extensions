package org.apache.spark.sql.execution.tablefunctions

import org.apache.spark.sql.types.{DataType, Metadata}

case class FieldExtractor(
    index: Int,
    tableName: String,
    name: String,
    dataTypeExtractor: DataTypeExtractor,
    annotationsExtractor: AnnotationsExtractor,
    isNullable: Boolean) {

  def inferredSqlType: String = dataTypeExtractor.inferredSqlType

  def numericPrecision: Option[Int] = dataTypeExtractor.numericPrecision

  def numericPrecisionRadix: Option[Int] = dataTypeExtractor.numericPrecisionRadix

  def numericScale: Option[Int] = dataTypeExtractor.numericScale

  def dataType: DataType = dataTypeExtractor.dataType

  def annotations: Map[String, String] = annotationsExtractor.annotations
}

object FieldExtractor {
  def apply(index: Int,
            tableName: String,
            name: String,
            dataType: DataType,
            metadata: Metadata,
            isNullable: Boolean,
            checkStar: Boolean): FieldExtractor =
    new FieldExtractor(
      index,
      tableName,
      name,
      DataTypeExtractor(dataType),
      AnnotationsExtractor(metadata, checkStar),
      isNullable)
}

package org.apache.spark.sql.execution.tablefunctions

import org.apache.spark.sql.types.{DataType, Metadata}

/**
  * Extracts various data for given values related to [[org.apache.spark.sql.types.StructField]]s.
  *
  * @param index The index of the field in the output schema.
  * @param tableName The name of the table the field belongs to.
  * @param originalTableName The name of the table where the field originates from.
  * @param name The current name of the field (through aliases etc. this might be different from
  *             the originalName).
  * @param originalName The original name of the field in the original table.
  * @param dataTypeExtractor A [[DataTypeExtractor]] for extracting [[DataType]] related values.
  * @param annotationsExtractor An [[AnnotationsExtractor]] for extraction annotation values.
  * @param isNullable `true` if the field is nullable, `false` otherwise.
  */
case class FieldExtractor(
    index: Int,
    tableName: String,
    originalTableName: String,
    name: String,
    originalName: String,
    dataTypeExtractor: DataTypeExtractor,
    annotationsExtractor: AnnotationsExtractor,
    isNullable: Boolean) {

  /**
    * Returns the inferred matching standard SQL type to the given spark type.
    *
    * @return The inferred SQL type string.
    */
  def inferredSqlType: String = dataTypeExtractor.inferredSqlType

  /** Returns the numeric precision of the data type.
    *
    * The numeric precision refers to the maximum number of digits
    * that can be present in a number.
    * @return The numeric precision of the data type
    */
  def numericPrecision: Option[Int] = dataTypeExtractor.numericPrecision

  /** Returns the numeric precision radix of the data type.
    *
    * The numeric precision radix refers to the base of which the
    * data type is.
    * @return The numeric precision radix of the data type
    */
  def numericPrecisionRadix: Option[Int] = dataTypeExtractor.numericPrecisionRadix

  /** Returns the numeric scale of the data type.
    *
    * The numeric scale refers to the maximum number of decimal
    * places the data type can represent.
    * @return The numeric scale of the data type
    */
  def numericScale: Option[Int] = dataTypeExtractor.numericScale

  /** The [[DataType]] of the field. */
  def dataType: DataType = dataTypeExtractor.dataType

  /**
    * The field's annotations, without key-value pairs where '*' is key if checkStar is `true`.
    */
  def annotations: Map[String, String] = annotationsExtractor.annotations
}

object FieldExtractor {
  /**
    * Creates a new [[FieldExtractor]] with the given values.
    *
    * For the given [[DataType]] and annotations map, a [[DataTypeExtractor]] and an
    * [[AnnotationsExtractor]] will be created.
    *
    * @param index The index of the field in the output schema.
    * @param tableName The name of the table the field belongs to.
    * @param originalTableName The name of the table where the field originates from.
    * @param name The current name of the field (through aliases etc. this might be different from
    *             the originalName).
    * @param originalName The original name of the field in the original table.
    * @param dataType The [[DataType]] of the field.
    * @param metadata The metadata of the field.
    * @param isNullable `true` if the field is nullable, `false` otherwise.
    * @return The created [[FieldExtractor]].
    */
  def apply(index: Int,
            tableName: String,
            originalTableName: String,
            name: String,
            originalName: String,
            dataType: DataType,
            metadata: Metadata,
            isNullable: Boolean,
            checkStar: Boolean): FieldExtractor =
    new FieldExtractor(
      index,
      tableName,
      originalTableName,
      name,
      originalName,
      DataTypeExtractor(dataType),
      AnnotationsExtractor(metadata, checkStar),
      isNullable)
}

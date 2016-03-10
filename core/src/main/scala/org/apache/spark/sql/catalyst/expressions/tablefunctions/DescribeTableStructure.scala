package org.apache.spark.sql.catalyst.expressions.tablefunctions

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Attribute}
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType, DataType}

/** The output structure of the describe table command. */
object DescribeTableStructure {
  private sealed trait NullableInfo { val isNullable: Boolean }
  private sealed abstract class BaseNullable(val isNullable: Boolean) extends NullableInfo
  private object Nullable extends BaseNullable(true)
  private object NotNullable extends BaseNullable(false)

  /** This method transforms (name, dataType, nullableInfo) into an Attribute for readability. */
  private implicit def toAttribute
    (info: (String, DataType, NullableInfo)): Attribute = info match {
    case (name, dataType, nullableInfo) =>
      AttributeReference(name, dataType, nullableInfo.isNullable)()
  }

  /** Fields used for the description of a table */
  object TableField {
    val schema: Attribute  = ("TABLE_SCHEMA", StringType, Nullable)
  }

  /** The output structure that only has values related to table information. */
  val tableRelatedStructure: Seq[Attribute] =
    TableField.schema :: Nil

  /** Fields used for the description of a column */
  object ColumnField {
    val tableName: Attribute              = ("TABLE_NAME", StringType, NotNullable)
    val name: Attribute                   = ("COLUMN_NAME", StringType, NotNullable)
    val ordinalPosition: Attribute        = ("ORDINAL_POSITION", IntegerType, NotNullable)
    val isNullable: Attribute             = ("IS_NULLABLE", BooleanType, NotNullable)
    val dataType: Attribute               = ("DATA_TYPE", StringType, NotNullable)
    val numericPrecision: Attribute       = ("NUMERIC_PRECISION", IntegerType, Nullable)
    val numericPrecisionRadix: Attribute  = ("NUMERIC_PRECISION_RADIX", IntegerType, Nullable)
    val numericScale: Attribute           = ("NUMERIC_SCALE", IntegerType, Nullable)
    val annotationKey: Attribute          = ("ANNOTATION_KEY", StringType, Nullable)
    val annotationValue: Attribute        = ("ANNOTATION_VALUE", StringType, Nullable)
  }

  /** The output structure that only has values related to column information. */
  val columnRelatedStructure: Seq[Attribute] =
    ColumnField.tableName ::
    ColumnField.name ::
    ColumnField.ordinalPosition ::
    ColumnField.isNullable ::
    ColumnField.dataType ::
    ColumnField.numericPrecision ::
    ColumnField.numericPrecisionRadix ::
    ColumnField.numericScale ::
    ColumnField.annotationKey ::
    ColumnField.annotationValue :: Nil

  /** The structure of the describe table function call result. */
  val output = tableRelatedStructure ++ columnRelatedStructure
}

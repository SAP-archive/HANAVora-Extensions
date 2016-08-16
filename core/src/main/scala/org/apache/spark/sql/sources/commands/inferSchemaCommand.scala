package org.apache.spark.sql.sources.commands

import org.apache.spark.sql.catalyst.analysis.systables.SchemaEnumeration
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.execution.datasources.parquet.ParquetRelation
import org.apache.spark.sql.execution.tablefunctions.DataTypeExtractor
import org.apache.spark.sql.hive.orc.OrcRelation
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

/**
  * An unresolved INFER SCHEMA command.
  * @param path The path to the file of which the schema shall be inferred.
  * @param explicitFileType The explicit file type, if specified.
  */
case class UnresolvedInferSchemaCommand(
    path: String,
    explicitFileType: Option[FileType])
  extends LeafNode{

  /** @inheritdoc */
  override lazy val output: Seq[Attribute] = InferSchemaCommand.schema.toAttributes

  /** @inheritdoc */
  override lazy val resolved = false
}

/**
  * A file type of which a schema can be inferred.
  */
sealed trait FileType {

  /**
    * Reads the schema of the file at the given path.
    *
    * @param sqlContext The Spark [[SQLContext]].
    * @param path The path of the file of which the schema shall be inferred.
    * @return The inferred schema of the file.
    */
  def readSchema(sqlContext: SQLContext, path: String): StructType
}

/** The parquet file type */
case object Parquet extends FileType {

  /** @inheritdoc */
  override def readSchema(sqlContext: SQLContext, path: String): StructType =
    new ParquetRelation(
      paths = Array(path),
      maybeDataSchema = None,
      maybePartitionSpec = None,
      parameters = Map.empty)(sqlContext).schema
}

/** The orc file type */
case object Orc extends FileType {

  /** @inheritdoc */
  override def readSchema(sqlContext: SQLContext, path: String): StructType =
    new OrcRelation(
      paths = Array(path),
      maybeDataSchema = None,
      maybePartitionSpec = None,
      parameters = Map.empty)(sqlContext).schema
}

/**
  * A resolved [[UnresolvedInferSchemaCommand]]. Evaluates to the inferred schema.
  *
  * @param path The path to the file to infer the schema from.
  * @param fileType The [[FileType]] of the file.
  */
case class InferSchemaCommand(path: String, fileType: FileType) extends RunnableCommand {
  override lazy val output: Seq[Attribute] = InferSchemaCommand.schema.toAttributes

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val fileSchema = fileType.readSchema(sqlContext, path)
    fileSchema.zipWithIndex.map {
      case (StructField(name, dataType, nullable, _), idx) =>
        val dataTypeExtractor = DataTypeExtractor(dataType)
        Row(
          name,
          idx + 1, // idx + 1 since the ordinal position has to start at 1
          nullable,
          dataTypeExtractor.inferredSqlType,
          dataTypeExtractor.numericPrecision.orNull,
          dataTypeExtractor.numericPrecisionRadix.orNull,
          dataTypeExtractor.numericScale.orNull)
    }
  }
}

object InferSchemaCommand extends SchemaEnumeration {
  val name = Field("COLUMN_NAME", StringType, nullable = false)
  val ordinalPosition = Field("ORDINAL_POSITION", IntegerType, nullable = false)
  val isNullable = Field("IS_NULLABLE", BooleanType, nullable = false)
  val dataType = Field("DATA_TYPE", StringType, nullable = false)
  val numericPrecision = Field("NUMERIC_PRECISION", IntegerType, nullable = true)
  val numericPrecisionRadix = Field("NUMERIC_PRECISION_RADIX", IntegerType, nullable = true)
  val numericScale = Field("NUMERIC_SCALE", IntegerType, nullable = true)
}

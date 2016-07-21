package org.apache.spark.sql.sources

import org.apache.spark.sql.types.DataType

/**
  * Key to find a relation.
  *
  * @param relationName The relation name.
  * @param originalRelationName The original relation name.
  * @param schema The schema the relation resides in.
  */
case class RelationKey(
    relationName: String,
    originalRelationName: String,
    schema: Option[String] = None)

/**
  * A description of a table or view schema
  *
  * @param relationSchema A [[Seq]] of [[SchemaField]]s.
  */
case class SchemaDescription(relationSchema: Seq[SchemaField])

/**
  * A field in a relation schema.
  *
  * @param name The field name.
  * @param originalName The original name of the field.
  * @param typ The field type.
  * @param nullable `true` if the field is nullable, `false` otherwise.
  * @param sparkDataType [[Some]]([[DataType]]) if there is a Spark
  *                      [[DataType]] mapping for the type of this field,
  *                      [[None]] otherwise.
  * @param numericPrecision The numeric precision of the data type, if any.
  * @param numericPrecisionRadix The numeric precision radix of the data type, if any.
  * @param numericScale The numeric scale of the data type, if any.
  * @param metadata The field metadata.
  * @param comment The field comment. This field is optional.
  */
case class SchemaField(
    name: String,
    originalName: String,
    typ: String,
    nullable: Boolean,
    sparkDataType: Option[DataType],
    numericPrecision: Option[Int],
    numericPrecisionRadix: Option[Int],
    numericScale: Option[Int],
    metadata: Map[String, String],
    comment: Option[String])

/**
  * A class which represents information about a relation.
  *
  * @param name The name of the relation.
  * @param isTemporary true if the relation is temporary, otherwise false.
  * @param kind The kind of the relation (e.g. table, view, dimension, ...).
  * @param ddl The original SQL statement issued by the user to create the relation.
  * @param provider The provider of the relation.
  */
case class RelationInfo(
    name: String,
    isTemporary: Boolean,
    kind: String,
    ddl: Option[String],
    provider: String)

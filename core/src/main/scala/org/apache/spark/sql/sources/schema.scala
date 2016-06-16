package org.apache.spark.sql.sources

import org.apache.spark.sql.sources.commands.RelationKind
import org.apache.spark.sql.types.DataType

/**
  * Key to find a relation.
  *
  * @param relationName The relation name.
  * @param schema The schema the relation resides in.
  */
case class RelationKey(relationName: String, schema: Option[String] = None)

/**
  * A description of a table or view schema
  *
  * @param kind The [[RelationKind]] of the relation described.
  * @param relationSchema A [[Seq]] of [[SchemaField]]s.
  */
case class SchemaDescription(kind: RelationKind, relationSchema: Seq[SchemaField])

/**
  * A field in a relation schema.
  *
  * @param name The field name.
  * @param typ The field type.
  * @param nullable `true` if the field is nullable, `false` otherwise.
  * @param sparkDataType [[Some]]([[DataType]]) if there is a Spark
  *                      [[DataType]] mapping for the type of this field,
  *                      [[None]] otherwise.
  * @param metadata The field metadata.
  */
case class SchemaField(
    name: String,
    typ: String,
    nullable: Boolean,
    sparkDataType: Option[DataType],
    metadata: Map[String, Any])

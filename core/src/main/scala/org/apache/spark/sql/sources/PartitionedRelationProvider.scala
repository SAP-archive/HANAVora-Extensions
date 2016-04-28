package org.apache.spark.sql.sources

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType

/**
 * This trait provides methods for creating relations with partitioning logic.
 * It supports both types of relations: with and without schema.
 */
trait PartitionedRelationProvider
  extends SchemaRelationProvider
  with TemporaryAndPersistentNature {

  def createRelation(sqlContext: SQLContext,
                     tableName: Seq[String],
                     parameters: Map[String, String],
                     partitioningFunction: Option[String],
                     partitioningColumns: Option[Seq[String]],
                     isTemporary: Boolean,
                     allowExisting: Boolean): BaseRelation

  def createRelation(sqlContext: SQLContext,
                     tableName: Seq[String],
                     parameters: Map[String, String],
                     schema: StructType,
                     partitioningFunction: Option[String],
                     partitioningColumns: Option[Seq[String]],
                     isTemporary: Boolean,
                     allowExisting: Boolean): BaseRelation

}

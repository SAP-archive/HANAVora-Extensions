package org.apache.spark.sql.sources

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.compat._

/**
 * In general SQL terms temporary tables go away when the context closes (i.e. user session)
 * and persistent ones stay. The difference in the spark sense is they do not disappear
 * after spark is shut down.
 *
 * If this is the case, the datasource can inherit this trait to extend the standard
 * SchemaRelationProvider with the temporary and persistent table creation support.
 */
trait TemporaryAndPersistentSchemaRelationProvider
  extends SchemaRelationProvider
  with TemporaryAndPersistentNature {

  def createRelation(sqlContext: SQLContext,
                     parameters: Map[String, String],
                     schema: StructType,
                     isTemporary: Boolean,
                     allowExisting: Boolean): BaseRelation
}

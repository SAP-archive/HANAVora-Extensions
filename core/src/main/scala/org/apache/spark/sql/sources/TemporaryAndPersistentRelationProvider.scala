package org.apache.spark.sql.sources

import org.apache.spark.sql.SQLContext

/**
 * In general SQL terms temporary tables go away when the context closes (i.e. user session)
 * and persistent ones stay. The difference in the spark sense is they do not disappear
 * after spark is shut down.
 *
 * If this is the case, the datasource can inherit this trait to extend the standard
 * RelationProvider with the temporary and persistent table creation support.
 */

trait TemporaryAndPersistentRelationProvider
  extends RelationProvider
  with TemporaryAndPersistentNature {

  def createRelation(sqlContext: SQLContext,
                     tableName: Seq[String],
                     parameters: Map[String, String],
                     isTemporary: Boolean,
                     allowExisting: Boolean): BaseRelation
}

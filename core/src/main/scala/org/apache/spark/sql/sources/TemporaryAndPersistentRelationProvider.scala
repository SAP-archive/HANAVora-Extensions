package org.apache.spark.sql.sources

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType

/**
 * In general SQL terms temporary tables go away when the context closes (i.e. user session)
 * and persistent ones stay. The difference in the spark sense is they do not disappear
 * after spark is shut down. If this is the case, the datasource can inherit that trait to support
 * "CREATE TABLE" statements as well. Note that you should still inherit RelationProvider and
 * SchemaRelationProvider for the datasource functionality but rather call these methods with
 * isTemporary set to true
 *
 * Adds the possibility for datasources to allow non temporary tables (in addition to the
 * temporary ones accessible via RelationProvider and SchemaRelationProvider traits
 */
trait TemporaryAndPersistentRelationProvider{

  def createRelation(sqlContext: SQLContext,
                     parameters: Map[String, String],
                     sparkSchema: StructType,
                     isTemporary : Boolean) : BaseRelation

  def createRelation(sqlContext: SQLContext,
                     parameters: Map[String, String],
                     isTemporary : Boolean) : BaseRelation

}

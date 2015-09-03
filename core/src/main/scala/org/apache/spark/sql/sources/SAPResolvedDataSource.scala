package org.apache.spark.sql.sources

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.ResolvedDataSource
import org.apache.spark.sql.types.StructType

/**
 * Resolves the datasources taking the temporary flag under account
 */

private[sql] object SAPResolvedDataSource {

  def apply(sqlContext: SQLContext,
            userSpecifiedSchema: Option[StructType],
            partitionColumns: Array[String],
            provider: String,
            options: Map[String, String],
            temporary: Boolean): SAPResolvedDataSource = {

    val dsProvider = lookupDataSource(provider)

    val relation : BaseRelation = dsProvider.newInstance() match {
      case drp : TemporaryAndPersistentRelationProvider => {
        userSpecifiedSchema match {
          case Some(schema: StructType) =>
            drp.createRelation(sqlContext, new CaseInsensitiveMap(options), schema, temporary)
          case None =>
            drp.createRelation(sqlContext, new CaseInsensitiveMap(options), temporary)
        }
      }
      case ds => userSpecifiedSchema match {
        case Some(schema: StructType) => ResolvedDataSource(sqlContext, userSpecifiedSchema,
          partitionColumns, provider, options).relation
        case None => ResolvedDataSource(sqlContext, userSpecifiedSchema, partitionColumns,
          provider, options).relation
      }
    }

    new SAPResolvedDataSource(dsProvider, relation)
  }

  def lookupDataSource(provider : String) : Class[_] = ResolvedDataSource.lookupDataSource(provider)
}

private[sql] case class SAPResolvedDataSource(val provider: Class[_],
                                              val relation: BaseRelation)

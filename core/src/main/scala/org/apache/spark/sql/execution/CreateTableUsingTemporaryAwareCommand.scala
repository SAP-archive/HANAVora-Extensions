package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * This command is used to register persistent tables if the datasource is capable of reporting
 * according to that.
 */
private[sql]
case class CreateTableUsingTemporaryAwareCommand(
    tableName: String,
    userSpecifiedSchema: Option[StructType],
    partitionColumns: Array[String],
    provider: String,
    options: Map[String, String],
    isTemporary: Boolean,
    allowExisting: Boolean)
  extends RunnableCommand {

  def run(sqlContext: SQLContext): Seq[Row] = {

    val dataSource: Any = ResolvedDataSource.lookupDataSource(provider).newInstance()

    val resolved: ResolvedDataSource = resolveDataSource(sqlContext,
      userSpecifiedSchema, partitionColumns, dataSource, options, isTemporary, allowExisting)
    // check if this class implements the DatabaseRelation Provider trait
    // this is also checked in the corresponding strategy CreatePersistentTableStrategy
    // however this class could also be called from somewhere else
    dataSource match {
      case _: TemporaryAndPersistentNature =>
        // make sure we register that properly in the catalog
        sqlContext.registerDataFrameAsTable(
          DataFrame(sqlContext, LogicalRelation(resolved.relation)), tableName)
        Seq.empty
      case _ if !isTemporary =>
        throw new RuntimeException("Datasource does not support non temporary tables!")
    }
  }

  /**
   * Returns a resolved datasource with temporary or persistent table creation handling.
   */
  private def resolveDataSource(sqlContext: SQLContext,
                                userSpecifiedSchema: Option[StructType],
                                partitionColumns: Array[String],
                                dataSource: Any,
                                options: Map[String, String],
                                isTemporary: Boolean,
                                allowExisting: Boolean): ResolvedDataSource = {

    dataSource match {
      case drp: TemporaryAndPersistentSchemaRelationProvider if userSpecifiedSchema.nonEmpty =>
            new ResolvedDataSource(drp.getClass,
              drp.createRelation(
                sqlContext,
                new CaseInsensitiveMap(options),
                userSpecifiedSchema.get,
                isTemporary,
                allowExisting))
      case drp: TemporaryAndPersistentRelationProvider =>
        new ResolvedDataSource(drp.getClass,
          drp.createRelation(sqlContext,
            new CaseInsensitiveMap(options), isTemporary, allowExisting))
      case _ => ResolvedDataSource(sqlContext, userSpecifiedSchema,
        partitionColumns, provider, options)
    }
  }
}

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.catalyst.TableIdentifierUtils._

/**
 * This command is used to register persistent tables if the datasource is capable of reporting
 * according to that.
 */
private[sql]
case class CreateTableUsingTemporaryAwareCommand(
    tableIdentifier: TableIdentifier,
    userSpecifiedSchema: Option[StructType],
    partitionColumns: Array[String],
    partitioningFunction: Option[String],
    partitioningColumns: Option[Seq[String]],
    provider: String,
    options: Map[String, String],
    isTemporary: Boolean,
    allowExisting: Boolean)
  extends RunnableCommand {

  def run(sqlContext: SQLContext): Seq[Row] = {
    // Convert the table name according to the case-sensitivity settings
    val tableId = alterByCatalystSettings(sqlContext, tableIdentifier)

    val dataSource: Any = ResolvedDataSource.lookupDataSource(provider).newInstance()

    // check if this class implements the DatabaseRelation Provider trait
    // this is also checked in the corresponding strategy CreatePersistentTableStrategy
    // however this class could also be called from somewhere else
    dataSource match {
      case _: TemporaryAndPersistentNature =>
        // make sure we register that properly in the catalog
        checkCreateTable(sqlContext, tableId)

        val resolved: ResolvedDataSource = resolveDataSource(sqlContext, dataSource, tableId)
        sqlContext.registerDataFrameAsTable(
          DataFrame(sqlContext, LogicalRelation(resolved.relation)), tableId.table)

        Seq.empty

      case _ if !isTemporary =>
        throw new RuntimeException("Datasource does not support non temporary tables!")
    }
  }

  private def checkCreateTable(sqlContext: SQLContext, tableId: TableIdentifier): Unit = {
    if (isTemporary) {
      if (allowExisting) {
        sys.error("allowExisting should be set to false when creating a temporary table.")
      }
    } else {
      if (sqlContext.catalog.tableExists(tableId) && !allowExisting) {
        sys.error(s"Table ${tableId.toString()} already exists")
      }
    }
  }

  /**
   * Returns a resolved datasource with temporary or persistent table creation handling.
   */
  // scalastyle:off method.length
  private def resolveDataSource(sqlContext: SQLContext,
                                dataSource: Any,
                                tableId: TableIdentifier): ResolvedDataSource = {
    // Convert the partitioning function according to the case-sensitivity settings
    val partitioningFunctionId = partitioningFunction
      .map(n => alterByCatalystSettings(sqlContext, n))

    dataSource match {
      case drp: PartitionedRelationProvider =>
        if (userSpecifiedSchema.isEmpty) {
          new ResolvedDataSource(drp.getClass,
            drp.createRelation(
              sqlContext,
              tableId.toSeq,
              new CaseInsensitiveMap(options),
              partitioningFunctionId,
              partitioningColumns,
              isTemporary,
              allowExisting))
        } else {
          new ResolvedDataSource(drp.getClass,
            drp.createRelation(
              sqlContext,
              tableId.toSeq,
              new CaseInsensitiveMap(options),
              userSpecifiedSchema.get,
              partitioningFunctionId,
              partitioningColumns,
              isTemporary,
              allowExisting))
        }
      case drp: TemporaryAndPersistentSchemaRelationProvider if userSpecifiedSchema.nonEmpty =>
            new ResolvedDataSource(drp.getClass,
              drp.createRelation(
                sqlContext,
                tableId.toSeq,
                new CaseInsensitiveMap(options),
                userSpecifiedSchema.get,
                isTemporary,
                allowExisting))
      case drp: TemporaryAndPersistentRelationProvider =>
        new ResolvedDataSource(drp.getClass,
          drp.createRelation(
            sqlContext,
            tableId.toSeq,
            new CaseInsensitiveMap(options),
            isTemporary,
            allowExisting))
      case _ => ResolvedDataSource(sqlContext, userSpecifiedSchema,
        partitionColumns, provider, options)
    }
  }
  // scalastyle:on method.length
}

package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.sources.{SAPResolvedDataSource, TemporaryAndPersistentRelationProvider, LogicalRelation}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * This command is used to register persistent tables if the datasource is capable of reporting
 * according to that.
 */
private[sql] case class CreateTableUsingTemporaryAwareCommand(tableName: String,
                                         userSpecifiedSchema: Option[StructType],
                                         partitionColumns: Array[String],
                                         provider: String,
                                         options: Map[String, String],
                                         isTemporary : Boolean) extends RunnableCommand {

  def run(sqlContext: SQLContext): Seq[Row] = {

    val resolved = SAPResolvedDataSource(
      sqlContext, userSpecifiedSchema, partitionColumns, provider, options, isTemporary)

    val providerClassInstance = SAPResolvedDataSource.lookupDataSource(provider).newInstance()

    // check if this class implements the DatabaseRelation Provider trait
    // this is also checked in the corresponding strategy CreatePersistentTableStrategy
    // however this class could also be called from somewhere else
    providerClassInstance match {
      case c : TemporaryAndPersistentRelationProvider => // ok
      case _ if !isTemporary =>
        throw new RuntimeException("Datasource does not support non temporary tables!")
    }

    // make sure we register that properly in the catalog
    sqlContext.registerDataFrameAsTable(
      DataFrame(sqlContext, LogicalRelation(resolved.relation)), tableName)
    Seq.empty
  }

}

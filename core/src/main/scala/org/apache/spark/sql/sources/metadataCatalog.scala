package org.apache.spark.sql.sources

import org.apache.spark.sql.SQLContext

/**
  * A provider that can provide metadata for its tables.
  */
trait MetadataCatalog {

  /**
    * Retrieves a [[Seq]] of [[TableMetadata]].
    *
    * @param sqlContext The [[SQLContext]].
    * @param options The options map.
    * @return A [[Seq]] of [[TableMetadata]].
    */
  def getTableMetadata(sqlContext: SQLContext, options: Map[String, String]): Seq[TableMetadata]

}

/**
  * Metadata for a table with the specified table name.
  *
  * @param tableName The name of the table.
  * @param metadata The metadata of the table.
  */
case class TableMetadata(tableName: String, metadata: Map[String, String])

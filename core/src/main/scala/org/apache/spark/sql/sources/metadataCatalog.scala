package org.apache.spark.sql.sources

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

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
  * A [[MetadataCatalog]] that can handle column scans and [[Filter]]s.
  */
trait PushDown {
  this: MetadataCatalog =>

  /**
    * Retrieves a [[Seq]] of [[Row]]s of table metadata computed by this datasource.
    *
    * @param sqlContext The Spark [[SQLContext]].
    * @param options The provider specific options map.
    * @param requiredColumns The required columns.
    * @param filters The [[Filter]]s.
    * @return The [[Row]]s with the required columns and passing the given [[Filter]]s.
    */
  def getTableMetadata(sqlContext: SQLContext,
                       options: Map[String, String],
                       requiredColumns: Seq[String],
                       filters: Seq[Filter]): RDD[Row]
}

/**
  * Metadata for a table with the specified table name.
  *
  * @param tableName The name of the table.
  * @param metadata The metadata of the table.
  */
case class TableMetadata(tableName: String, metadata: Map[String, String])

package org.apache.spark.sql.sources

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

/**
  * This trait indicates that the data source also has a catalog which could contain information
  * about already present relations. It offers methods to pull information about these relations.
 */
trait DatasourceCatalog {

  /**
    * Returns all relations in the catalog of the data source.
    *
    * @param sqlContext The SQL Context.
    * @param options The options map.
    * @return a sequence of all relations in the catalog of the data source.
    */
  def getRelations(sqlContext: SQLContext, options: Map[String, String]): Seq[RelationInfo]

  /**
    * Returns information about a relation in the catalog of the data source.
    *
    * @param sqlContext The SQL Context.
    * @param name The relation (qualified) name.
    * @param options The options map.
    * @return If the relation exists then it returns its information, otherwise [[None]].
    */
  def getRelation(sqlContext: SQLContext, name: Seq[String], options: Map[String, String]):
    Option[RelationInfo]

  /**
    * Gets the tables names from the catalog.
    *
    * @param sqlContext The SQL Context.
    * @param parameters The options.
    * @return The tables names.
    * @deprecated (YH) please use ''getRelations'' method.
    */
  def getTableNames(sqlContext: SQLContext, parameters: Map[String, String]): Seq[String]

  /**
    * Retrieves the schemas of the tables of this provider.
    *
    * @param sqlContext The [[SQLContext]].
    * @param options The options.
    * @return A mapping of relations and their [[SchemaDescription]]s.
    */
  def getSchemas(sqlContext: SQLContext,
                 options: Map[String, String]): Map[RelationKey, SchemaDescription] = Map.empty

}

/** A [[DatasourceCatalog]] that is capable of processing required filters and scans itself */
trait DatasourceCatalogPushDown extends DatasourceCatalog {

  /**
    * Returns all relations in the catalog of the data source.
    *
    * @param sqlContext The SQL Context.
    * @param options The options map.
    * @param requiredColumns The required columns.
    * @param filter An optional [[Filter]].
    * @return The [[Row]]s with the required columns and passing the given [[Filter]]s.
    */
  def getRelations(sqlContext: SQLContext,
                   options: Map[String, String],
                   requiredColumns: Seq[String],
                   filter: Option[Filter]): RDD[Row]

  /**
    * Retrieves the schemas of the tables of this provider.
    *
    * @param sqlContext The [[SQLContext]].
    * @param options The options.
    * @param requiredColumns The required columns.
    * @param filter An optional [[Filter]].
    * @return The [[Row]]s with the required columns and passing the given [[Filter]]s.
    */
  def getSchemas(sqlContext: SQLContext,
                 options: Map[String, String],
                 requiredColumns: Seq[String],
                 filter: Option[Filter]): RDD[Row]
}

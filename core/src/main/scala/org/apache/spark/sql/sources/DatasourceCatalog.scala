package org.apache.spark.sql.sources

import org.apache.spark.sql.SQLContext

/**
  * This trait indicates that the data source also has a catalog which could contain information
  * about already present relations. It offers methods to pull information about these relations.
 */
trait DatasourceCatalog {

  /**
    * A class which represents information about a relation.
    *
    * @param name The name of the relation.
    * @param isTemporary true if the relation is temporary, otherwise false.
    * @param kind The kind of the relation (e.g. table, view, dimension, ...).
    * @param ddl The original SQL statement issued by the user to create the relation.
    */
  case class RelationInfo(name: String, isTemporary: Boolean, kind: String, ddl: Option[String])

  /**
    * Returns all relations in the catalog of the data source.
    *
    * @param sqlContext The SQL Context.
    * @param options The options map.
    * @return a sequence of all relations in the catalog of the data source.
    */
  def getRelations(sqlContext: SQLContext, options: Map[String, String]): Seq[RelationInfo]

  /**
    * Gets the tables names from the catalog.
    *
    * @param sqlContext The SQL Context.
    * @param parameters The options.
    * @return The tables names.
    * @deprecated (YH) please use ''getRelations'' method.
    */
  def getTableNames(sqlContext: SQLContext, parameters: Map[String, String]): Seq[String]
}

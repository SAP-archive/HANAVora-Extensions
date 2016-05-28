package org.apache.spark.sql.sources

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.Attribute

/**
  * Indicates that a datasource supports the raw sql interface
  */
trait RawSqlSourceProvider {

  /**
    * Forwards a query to the engine/source it refers to
    *
    * @param sqlCommand SQL String that has to be queried
    * @return RDD representing the result of the SQL String
    */
  def getRDD(sqlCommand: String): RDD[Row]

  /**
    * Determines the schema of the raw sql string. Usually this happens by executing query to
    * extract the schema.
    *
    * @param sqlCommand user defined sql string
    * @return schema of the result of sqlCommand parameter
    */
  def getResultingAttributes(sqlCommand: String): Seq[Attribute]

  /**
    * Runs a DDL statement
    *
    * @param sqlCommand user defined sql string
    */
  def executeDDL(sqlCommand: String): Unit
}

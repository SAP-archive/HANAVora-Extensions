package org.apache.spark.sql.sources

/**
 * Relation that accepts the APPEND TABLE statement
 */
trait AppendRelation {

  /**
   * Append new files to an existing relation.
   * @param options Append command configuration
   * @return The relation itself
   */
  def appendFilesToTable(options: Map[String, String]): AppendRelation
}

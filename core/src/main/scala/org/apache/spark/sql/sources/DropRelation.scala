package org.apache.spark.sql.sources

/**
 * Relation that accepts the DROP TABLE statement
 */
trait DropRelation {

  /**
   * Drop the table from the catalog and Velocity.
   */
  def dropTable(): Unit
}

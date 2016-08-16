package org.apache.spark.sql.sources

/**
 * Table that can be dropped.
 */
trait DropRelation {
  this: Relation =>

  /**
   * Drop the table from the catalog and HANA Vora
   */
  def dropTable(): Unit
}

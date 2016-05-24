package org.apache.spark.sql.sources

import org.apache.spark.sql.sources.commands.WithExplicitRelationKind

/**
 * Table that can be dropped.
 */
trait DropRelation {
  this: WithExplicitRelationKind =>

  /**
   * Drop the table from the catalog and HANA Vora
   */
  def dropTable(): Unit
}

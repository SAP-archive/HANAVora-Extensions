package org.apache.spark.sql.sources

/**
 * A relation that is partitioned.
 */
trait PartitionedRelation {

  /**
   * Returns a partitioning function name for this relation or [[None]]
   * if no partitioning function is associated with this relation.
   *
   * @return Optionally the name if the partitioning function associated
   *         with this relation
   */
  def partitioningFunctionName: Option[String]

  /**
   * Extracts partitioning function columns for this relation or
   * returns [[None]] if no partitioning function is associated
   * with this relation.
   *
   * @return Optionally a [[Set]] of the partitioning function columns
   *         associated with this relation
   */
  def partitioningFunctionColumns: Option[Set[String]]

}

package org.apache.spark.sql.sources

/**
 * A relation which is capable of describing itself.
 */
trait DescribableRelation {
  /**
   * Describes this in form of key-value pairs.
   *
   * This method returns debug information.
   * @return A map with key-value pairs of descriptions.
   */
  def describe: Map[String, String]
}

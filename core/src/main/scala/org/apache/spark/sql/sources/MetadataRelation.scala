package org.apache.spark.sql.sources

/** A relation from which technical metadata can be retrieved */
trait MetadataRelation {
  this: Relation =>

  /**
    * @return The technical metadata of this relation.
    */
  def metadata: Map[String, String]
}

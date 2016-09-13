package org.apache.spark.sql.sources.sql

import org.apache.spark.sql.sources.Relation

/**
 * Marks a relation as SQL like.
 */
trait SqlLikeRelation {
  this: Relation =>

  /** The name space the relation resides in. [[None]] by default */
  def nameSpace: Option[String] = None

  /** The name of the relation. */
  def relationName: String
}

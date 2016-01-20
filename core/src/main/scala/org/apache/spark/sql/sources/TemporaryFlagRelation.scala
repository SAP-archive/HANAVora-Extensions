package org.apache.spark.sql.sources

/**
 * Trait if used in a BaseRelation allows to make a relation "non temporary"/persistent
 *
 * This one is meant for extending the BaseRelation!
 */
trait TemporaryFlagRelation {
  def isTemporary(): Boolean
}

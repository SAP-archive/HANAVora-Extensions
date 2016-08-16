package org.apache.spark.sql.sources.commands

import org.apache.spark.sql.sources.Relation

/**
  * A relation that knows about its origin.
  */
trait WithOrigin {
  this: Relation =>

  /**
    * Returns the origin provider of this.
    *
    * @return The name of the provider where this originates from.
    */
  def provider: String

}

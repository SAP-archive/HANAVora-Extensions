package org.apache.spark.sql.sources.commands

/**
  * A relation that knows about its origin.
  */
trait WithOrigin {

  /**
    * Returns the origin provider of this.
    *
    * @return The name of the provider where this originates from.
    */
  def provider: String

}

package org.apache.spark.sql.catalyst.analysis.systables

import org.apache.spark.sql.catalyst.plans.logical.UnresolvedSystemTable

/**
  * Exceptions for system tables.
  */
object SystemTableException {

  /**
    * An exception that is thrown when no system table for a given name could be found.
    *
    * @param name The name for which no system table could be found.
    */
  class NotFoundException(name: String)
    extends Exception(s"Could not find the system table $name")

  class InvalidProviderException(provider: SystemTableProvider, table: UnresolvedSystemTable)
    extends Exception(s"Invalid provider $provider for system table $table")
}

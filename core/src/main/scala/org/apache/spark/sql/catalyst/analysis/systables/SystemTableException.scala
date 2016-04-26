package org.apache.spark.sql.catalyst.analysis.systables

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
}

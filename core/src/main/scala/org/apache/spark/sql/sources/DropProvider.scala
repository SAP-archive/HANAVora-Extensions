package org.apache.spark.sql.sources

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.datasources.DropTarget

/**
  * Indicates that a datasource supports drop relation
  */
trait DropProvider {
  /**
    *
    * @param target The [[DropTarget]] of the relation to drop.
    * @param ifExists `true` if it should ignore not existing relations,
    *                 `false` otherwise, which will throw if the targeted relation does not exist.
    * @param tableIdentifier A [[Seq]] of [[String]]s to identify the table to be dropped.
    * @param cascade `true` if it should drop related relations,
    *                `false` otherwise, which will throw if related relations exist.
    * @param options A [[Map]] which contains the options.
    */
  def dropRelation(sqlContext: SQLContext,
                   target: DropTarget,
                   ifExists: Boolean,
                   tableIdentifier: Seq[String],
                   cascade: Boolean,
                   options: Map[String, String]): Unit
}

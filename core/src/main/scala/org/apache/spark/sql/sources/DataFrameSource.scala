package org.apache.spark.sql.sources

import org.apache.spark.sql.execution.datasources.{CreatePersistentViewCommand, LogicalRelation}
import org.apache.spark.sql.{DataFrame, SQLContext}

/** Source from which a [[org.apache.spark.sql.DataFrame]] can be obtained. */
trait DataFrameSource {
  /** Instantiates a [[org.apache.spark.sql.DataFrame]] with the given sqlContext.
    *
    * @param sqlContext The sqlContext
    * @return The created [[DataFrame]]
    */
  def dataFrame(sqlContext: SQLContext): DataFrame
}

/** Source of a [[org.apache.spark.sql.DataFrame]] from a BaseRelation.
  *
  * @param baseRelation The baseRelation from which the [[DataFrame]] is created.
  */
case class BaseRelationSource(baseRelation: BaseRelation) extends DataFrameSource {
  def dataFrame(sqlContext: SQLContext): DataFrame = {
    DataFrame(sqlContext, LogicalRelation(baseRelation))
  }
}

/** Source of a [[org.apache.spark.sql.DataFrame]] from a create persistent view statement
  *
  * @param createViewStatement The sql query string.
  */
case class CreatePersistentViewSource(createViewStatement: String) extends DataFrameSource {
  def dataFrame(sqlContext: SQLContext): DataFrame = {
    sqlContext.parseSql(createViewStatement) match {
      case CreatePersistentViewCommand(_, plan, _, _, _) =>
        DataFrame(sqlContext, plan)
      case unknown =>
        throw new RuntimeException(s"Could not extract view query from $unknown")
    }
  }
}


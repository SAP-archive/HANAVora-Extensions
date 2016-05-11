package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SelectWith, UnresolvedSelectWith}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.ResolvedDataSource
import org.apache.spark.sql.sources.RawSqlSourceProvider

import scala.util.{Failure, Success, Try}

/**
  * Resolves [[org.apache.spark.sql.catalyst.plans.logical.UnresolvedSelectWith]] to
  * [[org.apache.spark.sql.catalyst.plans.logical.SelectWith]]
  */
private[sql] case class ResolveSelectWith(analyzer: Analyzer) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case UnresolvedSelectWith(sqlCommand, className) => {
      val ds = Try(ResolvedDataSource.lookupDataSource(className).newInstance()) match {
        case Success(ds) => ds
        case Failure(_) => throw new AnalysisException(s"Exception during creation of the " +
          s"datasource object in $className - is the class name correct?")
      }
      ds match {
        case rawSql: RawSqlSourceProvider =>
          SelectWith(sqlCommand, className,
            // queries the source that will evaluate the query for a schema
            // NOTE: this might be expensive
            output = rawSql.getResultingAttributes(sqlCommand))

        case _ => throw new AnalysisException(s"'SQL' WITH ${className} only works if " +
          s"${className} implements the RawSqlSourceProvider Interface")
      }
    }
  }

}

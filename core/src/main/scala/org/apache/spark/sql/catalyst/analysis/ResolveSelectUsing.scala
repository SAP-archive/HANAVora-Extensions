package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SelectUsing, UnresolvedSelectUsing}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.ResolvedDataSource
import org.apache.spark.sql.sources.RawSqlSourceProvider

import scala.util.{Failure, Success, Try}

/**
  * Resolves [[org.apache.spark.sql.catalyst.plans.logical.UnresolvedSelectUsing]] to
  * [[org.apache.spark.sql.catalyst.plans.logical.SelectUsing]]
  */
private[sql] case class ResolveSelectUsing(analyzer: Analyzer) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    // no schema is present, we have to infer it
    case UnresolvedSelectUsing(sqlCommand, className, None) => {
      val ds = Try(ResolvedDataSource.lookupDataSource(className).newInstance()) match {
        case Success(ds) => ds
        case Failure(_) => throw new AnalysisException(s"Exception during creation of the " +
          s"datasource object in $className - is the class name correct?")
      }
      ds match {
        case rawSql: RawSqlSourceProvider =>
          SelectUsing(sqlCommand, className,
            // queries the source that will evaluate the query for a schema
            // NOTE: this might be expensive
            output = rawSql.getResultingAttributes(sqlCommand))

        case _ => throw new AnalysisException(s"'SQL' WITH ${className} only works if " +
          s"${className} implements the RawSqlSourceProvider Interface")
      }
    }
    // schema is present
    case UnresolvedSelectUsing(sqlCommand, className, Some(fields)) => {
      SelectUsing(sqlCommand, className,
        fields.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)()))
    }
  }

}

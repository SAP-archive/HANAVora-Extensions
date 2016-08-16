package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.{AnalysisException, SQLContext}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.sources.commands.{InferSchemaCommand, Orc, Parquet, UnresolvedInferSchemaCommand}

import scala.util.Try

/**
  * Resolves [[UnresolvedInferSchemaCommand]]s to [[InferSchemaCommand]]s.
  *
  * @param sqlContext The Spark [[SQLContext]] to read the schema with.
  */
case class ResolveInferSchemaCommand(sqlContext: SQLContext) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.transform {
    case UnresolvedInferSchemaCommand(path, explicitFileType) =>
      val fileType = explicitFileType.getOrElse(path.toLowerCase match {
        case p if p.endsWith(".orc") => Orc
        case p if p.endsWith(".parquet") => Parquet
        case invalid =>
          throw new AnalysisException(s"Could not determine file format of '$path'")
      })
      InferSchemaCommand(path, fileType)
  }
}


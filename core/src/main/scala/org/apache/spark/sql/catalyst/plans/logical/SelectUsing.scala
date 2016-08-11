package org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.sources.RawSqlExecution
import org.apache.spark.sql.types.StructType

/**
  * Class representing a direct sql call to a datasource implementing raw SQL interface
  * [[org.apache.spark.sql.sources.RawSqlSourceProvider]]
  */
case class SelectUsing(rawSqlExecution: RawSqlExecution)
  extends LeafNode {

  override def output: Seq[Attribute] = rawSqlExecution.output

  override lazy val resolved = true

  @transient override lazy val statistics: Statistics = rawSqlExecution.statistics
}

/**
  * Unresolved representation, will be resolved by the Analyzer rule
  * [[org.apache.spark.sql.catalyst.analysis.ResolveSelectUsing]]
  *
  * @param sqlCommand the raw SQL string to execute
  * @param provider the name of the data source
  * @param fields if None, the schema gets inferred from the data source, if set, it is assumed to
  *               be the correct schema
  */
case class UnresolvedSelectUsing(
    sqlCommand: String,
    provider: String,
    fields: Option[StructType] = None,
    options: Map[String, String] = Map.empty)
  extends LeafNode {

  override def output: Seq[Attribute] = Nil

  override lazy val resolved = false
}

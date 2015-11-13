package org.apache.spark.sql.execution

package object datasources {

  type LogicalRelation = org.apache.spark.sql.sources.LogicalRelation
  val LogicalRelation = org.apache.spark.sql.sources.LogicalRelation

  type DDLParser = org.apache.spark.sql.sources.DDLParser

  type ResolvedDataSource = org.apache.spark.sql.sources.ResolvedDataSource
  val ResolvedDataSource = org.apache.spark.sql.sources.ResolvedDataSource

  val DataSourceStrategy = org.apache.spark.sql.sources.DataSourceStrategy

  val PreInsertCastAndRename = org.apache.spark.sql.sources.PreInsertCastAndRename
  val PreWriteCheck = org.apache.spark.sql.sources.PreWriteCheck

  type CaseInsensitiveMap = org.apache.spark.sql.sources.CaseInsensitiveMap

}

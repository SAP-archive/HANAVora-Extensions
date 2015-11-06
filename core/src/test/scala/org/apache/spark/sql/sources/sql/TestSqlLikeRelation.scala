package org.apache.spark.sql.sources.sql

case class TestSqlLikeRelation(
                                override val nameSpace: Option[String],
                                override val tableName: String)
  extends SqlLikeRelation

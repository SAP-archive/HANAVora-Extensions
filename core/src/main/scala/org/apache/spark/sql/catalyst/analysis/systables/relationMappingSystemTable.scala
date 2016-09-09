package org.apache.spark.sql.catalyst.analysis.systables
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.sql.SqlLikeRelation
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{Row, SQLContext}

object RelationMappingSystemTableProvider extends SystemTableProvider with LocalSpark {

  /** @inheritdoc */
  override def create(sqlContext: SQLContext): SystemTable = RelationMappingSystemTable(sqlContext)
}

case class RelationMappingSystemTable(sqlContext: SQLContext)
  extends SystemTable
  with AutoScan {

  override def schema: StructType = RelationMappingSystemTable.schema

  /** @inheritdoc */
  override def execute(): Seq[Row] = {
    sqlContext.tableNames().map { tableName =>
      val plan = sqlContext.catalog.lookupRelation(TableIdentifier(tableName))
      val sqlName = plan.collectFirst {
        case s: SqlLikeRelation =>
          s.relationName
        case LogicalRelation(s: SqlLikeRelation, _) =>
          s.relationName
      }
      Row(tableName, sqlName)
    }
  }
}

object RelationMappingSystemTable extends SchemaEnumeration {
  val sparkName = Field("RELATION_NAME", StringType, nullable = false)
  val providerName = Field("SQL_NAME", StringType, nullable = true)
}

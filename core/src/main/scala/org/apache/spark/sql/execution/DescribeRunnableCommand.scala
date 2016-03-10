package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.IsLogicalRelation
import org.apache.spark.sql.sources.sql.SqlBuilder
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row

import scala.collection.mutable

/**
 * Returns the description of a select query.
 *
 * The query must be a SELECT statement. Moreover, it is allowed for the SELECT statement
 * to have extra annotations for retrieving specific meta data.
 */
// TODO (AC) Remove this once table-valued function are rebased on top.
private[sql]
case class DescribeRunnableCommand(plan: LogicalPlan) extends RunnableCommand {

  lazy val sqlBuilder = new SqlBuilder

  override val output = StructType(
      StructField("NAME", StringType, nullable = false) ::
      StructField("POSITION", IntegerType, nullable = false) ::
      StructField("DATA_TYPE", StringType, nullable = false) ::
      StructField("ANNOTATION_KEY", StringType, nullable = true) ::
      StructField("ANNOTATION_VALUE", StringType, nullable = true) ::
      Nil).toAttributes

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val (fields: Seq[(Metadata, String, DataType)] , checkStar) = plan match {
      case logical.Aggregate(_, _, logical.Union(left, _)) =>
        getAttributes(left)
      case logical.Union(left, _) =>
        getAttributes(left)
      case default =>
        getAttributes(default)
    }
    fields.zipWithIndex.flatMap {
      case ((metadata, name, dataType), index) =>
        getRows(metadata, name, dataType, index)(checkStar)
      case default =>
        throw new RuntimeException(s"could not get attributes of plan $default")
    }
  }

  private[this] def getAttributes(plan: LogicalPlan):
  (Seq[(Metadata, String, DataType)], Boolean) = plan match {
    case logical.Project(attributes, _) =>
      (attributes.map(a => (a.metadata, a.name, a.dataType)), true)
    case lr@IsLogicalRelation(_) =>
      (lr.schema.fields.map(a => (a.metadata, a.name, a.dataType)).toSeq, false)
  }

  private def getRows(data: Metadata, name: String, dataType: DataType, index: Int)
                     (checkStar: Boolean = false): Seq[Row] = {
    val metadata = MetadataAccessor.metadataToMap(data)
    metadata.isEmpty match {
      case true => Row(name, index, sqlType(dataType), null, null) :: Nil
      case false =>
        metadata.collect({
          case (k, v) if !checkStar || !k.equals("*") =>
            Row(name, index, sqlType(dataType), k, v.toString)
        }).toSeq
    }
  }

  def sqlType(dataType: DataType): String = {
    sqlBuilder.typeToSql(dataType)
  }
}

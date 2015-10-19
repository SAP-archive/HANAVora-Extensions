package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, SortOrder}
import org.apache.spark.sql.hierarchy.HierarchyRowBroadcastBuilder
import org.apache.spark.sql.types.{Node, NodeType, StructField, StructType}

case class HierarchyPhysicalPlan(childAlias: String,
                                 parenthoodExpression: Expression,
                                 searchBy: Seq[SortOrder],
                                 startWhere: Option[Expression],
                                 nodeAttribute: Attribute,
                                 child: SparkPlan) extends UnaryNode {

  override def output: Seq[Attribute] = child.output :+ nodeAttribute

  override def doExecute(): RDD[Row] = {
    val rdd = child.execute()
    val attributes = child.output

    /* XXX: Copied from DataFrame. See SPARK-4775 (weird duplicated rows). */
    val childSchema = child.schema
    val mappedRDD = rdd.mapPartitions({ iter => iter.map({case r:Row => Row(r.toSeq: _*)})})
    val resultRdd = HierarchyRowBroadcastBuilder(
      attributes,
      parenthoodExpression,
      startWhere,
      searchBy
    ).buildFromAdjacencyList(mappedRDD)
    val cachedResultRdd = resultRdd
      .mapPartitions({ iter =>
      val schemaWithNode =
        StructType(childSchema.fields ++ Seq(StructField("", NodeType, nullable = false)))
      val converter = CatalystTypeConverters.createToCatalystConverter(schemaWithNode)
      iter.map({ row =>
        val node = row.getAs[Node](row.length - 1)
        val rowWithoutNode = Row(row.toSeq.take(row.length - 1): _*)
        val convertedRowWithoutNode = converter(rowWithoutNode).asInstanceOf[Row]
        Row.fromSeq(convertedRowWithoutNode.toSeq :+ node)
      })
    }).cache()
    cachedResultRdd
  }

}

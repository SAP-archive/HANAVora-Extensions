package org.apache.spark.sql.hierarchy

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.{Node, NodeType, StructField, StructType}

case class HierarchyStrategy(child: SparkPlan,
                             attributes: Seq[Attribute], parenthoodExpression: Expression,
                             startWhere: Option[Expression], searchBy: Seq[SortOrder]) {

  private def parseConfig(sc: SparkContext): (String, Long) = {
    val impName = sc.conf.get("hierarchy.always", "undefined")
    val actualThreshold = sc.conf.get("hierarchy.threshold",
                                      HierarchyStrategy.THRESHOLD.toString).toLong
    if(actualThreshold < 0) throw new IllegalArgumentException("threshold must be non-negative")
    impName match  {
      case "join" | "broadcast" | "undefined" => (impName, actualThreshold)
      case _ => throw new IllegalArgumentException("unknown implementation: " + impName)
    }
  }

  private def useBroadcastHierarchy(conf: (String, Long), count: => Long): Boolean = {
    /* TODO (YH) delay the calculation of rows until it is very necessary */
    lazy val cnt = count
    conf match {
      case ("broadcast", _) => true
      case ("join", _) => false
      case ("undefined", threshold) if cnt < threshold => true
      case ("undefined", threshold) if cnt  >= threshold => false
      case _ => throw new IllegalStateException("Unknown configuration" + conf)
    }
  }

  def execute(rdd: RDD[Row]): RDD[Row] = {
    /* XXX: Copied from DataFrame. See SPARK-4775 (weird duplicated rows). */
    val childSchema = child.schema
    val cachedRdd = rdd.mapPartitions({ iter =>
      val converter = CatalystTypeConverters.createToScalaConverter(childSchema)
      iter.map(converter(_).asInstanceOf[Row])
    }).cache()
    val resultRdd = startWhere match {
      case Some(_) => HierarchyRowBroadcastBuilder(attributes, parenthoodExpression, startWhere,
        searchBy).buildFromAdjacencyList(cachedRdd)
      case None =>
        val count = cachedRdd.count()
        useBroadcastHierarchy(parseConfig(cachedRdd.sparkContext), count) match {
        case true => HierarchyRowBroadcastBuilder(attributes, parenthoodExpression, startWhere,
          searchBy).buildFromAdjacencyList(cachedRdd)
        case false => HierarchyRowJoinBuilder(
          attributes, parenthoodExpression, startWhere.get, searchBy)
          .buildFromAdjacencyList(cachedRdd)
      }
    }
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
    cachedRdd.unpersist(blocking = false)
    cachedResultRdd
  }
}

object HierarchyStrategy {
  val THRESHOLD = 1000000L
}

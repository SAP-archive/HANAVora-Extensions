package org.apache.spark.sql.hierarchy

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, SortOrder}

case class HierarchyStrategy(attributes: Seq[Attribute], parenthoodExpression: Expression,
                             startWhere: Expression, searchBy: Seq[SortOrder]) {

  private def parseConfig(sc : SparkContext) : (String, Long) = {
    val impName = sc.conf.get("hierarchy.always", "undefined")
    val actualThreshold = sc.conf.get("hierarchy.threshold",
                                      HierarchyStrategy.THRESHOLD.toString).toLong
    if(actualThreshold < 0) throw new IllegalArgumentException("threshold must be non-negative")
    impName match  {
      case "join" | "broadcast" | "undefined" => (impName, actualThreshold)
      case _ => throw new IllegalArgumentException("unknown implementation: " + impName)
    }
  }

  private def useBroadcastHierarchy(conf : (String, Long), count: => Long) : Boolean = {
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
    useBroadcastHierarchy(parseConfig(rdd.sparkContext), rdd count) match {
      case true => HierarchyRowBroadcastBuilder(attributes, parenthoodExpression, startWhere,
                                                searchBy).buildFromAdjacencyList(rdd)
      case false => HierarchyRowJoinBuilder(attributes, parenthoodExpression, startWhere, searchBy)
        .buildFromAdjacencyList(rdd)
    }
  }
}

object HierarchyStrategy {
  val THRESHOLD = 1000000L
}

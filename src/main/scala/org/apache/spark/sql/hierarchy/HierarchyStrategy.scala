package org.apache.spark.sql.hierarchy

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{SortOrder, Expression, Attribute}

case class HierarchyStrategy(attributes: Seq[Attribute], parenthoodExpression: Expression,
                             startWhere: Expression, searchBy: Seq[SortOrder]) {
  /* TODO (YH, SM) make this configurable at least for tests. */
  val threshold = 7L

  def execute(rdd: RDD[Row]): RDD[Row] = {
    if (rdd.count() < threshold) {
      HierarchyRowBroadcastBuilder(attributes, parenthoodExpression,
        startWhere, searchBy)
        .buildFromAdjacencyList(rdd)
    } else {
      HierarchyRowJoinBuilder(attributes, parenthoodExpression, startWhere, searchBy)
        .buildFromAdjacencyList(rdd)
    }
  }
}

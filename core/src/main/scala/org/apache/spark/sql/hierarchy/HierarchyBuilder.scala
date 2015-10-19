package org.apache.spark.sql.hierarchy

import org.apache.spark.rdd.RDD

trait HierarchyBuilder[T,O] {
  def buildFromAdjacencyList(rdd: RDD[T]): RDD[O]
}


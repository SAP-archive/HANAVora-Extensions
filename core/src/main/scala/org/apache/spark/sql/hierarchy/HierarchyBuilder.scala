package org.apache.spark.sql.hierarchy

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.DataType

trait HierarchyBuilder[T,O] {
  def buildFromAdjacencyList(rdd: RDD[T], pathDataType: DataType): RDD[O]
}


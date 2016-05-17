package org.apache.spark.sql.hierarchy

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.DataType

/**
  * Builds a hierarchy.
  *
  * @tparam T The input RDD type.
  * @tparam O The output RDD type.
  */
trait HierarchyBuilder[T, O] {

  def buildHierarchyRdd(rdd: RDD[T], pathDataType: DataType): RDD[O]

}

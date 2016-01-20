package org.apache.spark.sql.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.execution.RDDConversions
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row

/**
 * Converts a Row RDD to RDD.
 */
object RddUtils {

  def rowRddToRdd(rowRdd: RDD[InternalRow], schema: StructType): RDD[Row] = {
    rowRdd.mapPartitions { iter =>
      val converter = CatalystTypeConverters.createToScalaConverter(schema)
      iter.map(converter(_).asInstanceOf[Row])
    }
  }
}

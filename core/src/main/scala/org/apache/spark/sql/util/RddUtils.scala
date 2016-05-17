package org.apache.spark.sql.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row

/**
 * Converts a Row RDD to RDD.
 */
object RddUtils {

  /**
    * Convert an RDD[InternalRow] to RDD[Row] using Catalog type converters.
    *
    * @param rowRdd The source RDD of [[InternalRow]].
    * @param schema The schema, must match the rdd columns.
    * @return The result RDD of [[Row]].
    */
  def rowRddToRdd(rowRdd: RDD[InternalRow], schema: StructType): RDD[Row] = {
    rowRdd.mapPartitions { iter =>
      val converter = CatalystTypeConverters.createToScalaConverter(schema)
      iter.map(converter(_).asInstanceOf[Row])
    }
  }

  /**
    * Convert an RDD[Row] to RDD[InternalRow] using Catalog type converters.
    *
    * @param rdd The source RDD of [[Row]].
    * @param schema The schema, must match the rdd columns.
    * @return The result RDD of [[InternalRow]].
    */
  def rddToRowRdd(rdd: RDD[Row], schema: StructType): RDD[InternalRow] = {
    val converter = CatalystTypeConverters.createToCatalystConverter(schema)
      .andThen(_.asInstanceOf[InternalRow])
    rdd.mapPartitions { iter => iter.map(converter(_)) }
  }
}

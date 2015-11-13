package org.apache.spark.sql.execution

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.compat.InternalRow
import org.apache.spark.sql.types.compat._

/**
  * Utilities to convert [[org.apache.spark.rdd.RDD]]
  * of [[org.apache.spark.sql.Row]] and
  * [[org.apache.spark.sql.catalyst.compat.InternalRow]].
  *
  * This is similar to [[RDDConversions]], but provides compatibility
  * across different Spark versions (1.4.x, 1.5.x).
  */
@DeveloperApi
object CompatRDDConversions {

  def rowRddToRdd(rowRdd: RDD[InternalRow], schema: StructType): RDD[Row] = {
    rowRdd.mapPartitions { iter =>
      val converter = CatalystTypeConverters.createToScalaConverter(schema)
      iter.map(converter(_).asInstanceOf[Row])
    }
  }

  def rddToRowRdd(rdd: RDD[Row], schema: StructType): RDD[InternalRow] = {
    RDDConversions.rowToRowRdd(rdd, schema.fields.map(_.dataType))
  }

}

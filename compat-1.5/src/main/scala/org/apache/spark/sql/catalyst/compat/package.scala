package org.apache.spark.sql.catalyst

package object compat {
  type InternalRow = org.apache.spark.sql.catalyst.InternalRow
  val InternalRow = org.apache.spark.sql.catalyst.InternalRow
}

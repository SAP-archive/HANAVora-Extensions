package org.apache.spark.sql.sources.describable

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * A [[Describable]] that returns the [[String]] representation of the given argument.
  * @param any The [[Any]] to describe.
  */
case class DefaultDescriber(any: Any) extends Describable {
  override def describe(): Row = Row(any.toString)

  override def describeOutput: StructType =
    StructType(StructField("value", StringType) :: Nil)
}

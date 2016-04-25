package org.apache.spark.sql.sources

import org.apache.spark.sql.types.DataType

/**
  * A trait that represents a partitioning function.
  */
sealed trait PartitioningFunction extends Product {
  val name: String
  def dataTypes: Seq[DataType]
}

/**
  * A partitioning function that only has one data type.
  */
trait SimpleDataType {
  this: PartitioningFunction =>
  val dataType: DataType

  override def dataTypes: Seq[DataType] = Seq(dataType)
}

/**
  * A Hash partitioning function.
  *
  * @param name The name of the partitioning function.
  * @param dataTypes The datatypes of the partitioning function.
  * @param partitionsNo The number of partitions of this function.
  */
case class HashPartitioningFunction(
    name: String,
    dataTypes: Seq[DataType],
    partitionsNo: Option[Int])
  extends PartitioningFunction

/**
  * A partitioning function that partitions ranges by splitting them.
  *
  * @param name The name of the partitioning function.
  * @param dataType The data type of the partitioning function.
  * @param splitters The splitters that split up the ranges.
  * @param rightClosed True if this is right closed, false otherwise.
  */
case class RangeSplitPartitioningFunction(
    name: String,
    dataType: DataType,
    splitters: Seq[Int],
    rightClosed: Boolean)
  extends PartitioningFunction
  with SimpleDataType


/**
  * A partitioning function that partitions ranges by intervals.
  *
  * @param name The name of the partitioning function.
  * @param dataType The data type of this partitioning function.
  * @param start The start of the interval.
  * @param end The end of the interval.
  * @param dissector The dissector of the interval.
  */
case class RangeIntervalPartitioningFunction(
    name: String,
    dataType: DataType,
    start: Int,
    end: Int,
    dissector: Dissector)
  extends PartitioningFunction
  with SimpleDataType

/**
  * A dissector for [[RangeIntervalPartitioningFunction]]s.
  */
sealed trait Dissector extends Product {
  val n: Int
}

object Dissector {
  /**
    * Creates a [[Dissector]] from a given [[Either]][[[Int]], [[Int]]]
    *
    * @param either The [[Either]] to convert.
    * @return [[Stride]] if it was a [[Left]], otherwise [[Parts]]
    */
  def fromEither(either: Either[Int, Int]): Dissector = either match {
    case Left(int) => Stride(int)
    case Right(int) => Parts(int)
  }
}

/**
  * A stride dissector.
  *
  * @param n The value of the stride.
  */
case class Stride(n: Int) extends Dissector

/**
  * A parts dissector.
  *
  * @param n The number of the parts.
  */
case class Parts(n: Int) extends Dissector

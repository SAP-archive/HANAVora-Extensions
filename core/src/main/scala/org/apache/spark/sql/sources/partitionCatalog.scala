package org.apache.spark.sql.sources

/**
  * A catalog for [[PartitionFunction]]s and [[PartitionScheme]]s.
  */
trait PartitionCatalog {

  /**
    * Returns all [[PartitionScheme]]s of this catalog.
    *
    * @return A [[Seq]] of all [[PartitionScheme]]s in this catalog.
    */
  def partitionSchemes: Seq[PartitionScheme]

  /**
    * Tries to find a [[PartitionScheme]] with the given id.
    *
    * @param id The id of the [[PartitionScheme]] to find.
    * @return [[Some]] with the found [[PartitionScheme]] if it was found,
    *         [[None]] otherwise.
    */
  def findPartitionScheme(id: String): Option[PartitionScheme]

  /**
    * Returns all [[PartitionFunction]]s of this catalog.
    *
    * @return A [[Seq]] of all [[PartitionFunction]]s in this catalog.
    */
  def partitionFunctions: Seq[PartitionFunction]

  /**
    * Tries to find a [[PartitionFunction]] with the given id.
    *
    * @param id The id of the [[PartitionFunction]] to find.
    * @return [[Some]] with the found [[PartitionFunction]] if it was found,
    *         [[None]] otherwise.
    */
  def findPartitionFunction(id: String): Option[PartitionFunction]

  /**
    * Drops a [[PartitionFunction]] with the given name
    *
    * @param id the name of the partition function that should be dropped
    */
  def dropPartitionFunction(id: String): Unit
}

/**
  * A partition scheme.
  *
  * @param id The unique id of the [[PartitionScheme]].
  * @param partitionFunction The [[PartitionFunction]] used by this scheme.
  * @param autoColocation `true` if this is auto-colocated, `false` otherwise.
  */
case class PartitionScheme(
    id: String,
    partitionFunction: PartitionFunction,
    autoColocation: Boolean)

/**
  * A partition function.
  */
sealed trait PartitionFunction {

  /** The unique id of the partition function. */
  val id: String

  /** The columns this function partitions by. */
  val columns: Seq[PartitionColumn]
}

object PartitionFunction {
  def unapply(function: PartitionFunction): Option[(String, Seq[PartitionColumn])] =
    Some((function.id, function.columns))
}

/** A partition function for which the minimum and maximum number of partitions can be specified */
sealed trait MinMaxPartitions {
  this: PartitionFunction =>

  /** The optional minimum number of partitions. */
  val minPartitions: Option[Int]

  /** The optional maximum number of partitions. */
  val maxPartitions: Option[Int]
}

object MinMaxPartitions {
  def unapply(minMaxPartitions: MinMaxPartitions with PartitionFunction)
                                                  : Option[(Option[Int], Option[Int])] =
    Some(minMaxPartitions.minPartitions -> minMaxPartitions.maxPartitions)
}

/**
  * A partition function that partitions by ranges.
  *
  * @param id The unique id of the partition function.
  * @param columns The columns this function partitions by.
  * @param boundary The optional boundary definition.
  * @param minPartitions The optional minimum number of partitions.
  * @param maxPartitions The optional maximum number of partitions.
  */
case class RangePartitionFunction(
    id: String,
    columns: Seq[PartitionColumn],
    boundary: Option[String],
    minPartitions: Option[Int],
    maxPartitions: Option[Int])
  extends PartitionFunction
  with MinMaxPartitions

/**
  * A partition function that partitions by hashes.
  *
  * @param id The unique id of the partition function.
  * @param columns The columns this function partitions by.
  * @param minPartitions The optional minimum number of partitions.
  * @param maxPartitions The optional maximum number of partitions.
  */
case class HashPartitionFunction(
    id: String,
    columns: Seq[PartitionColumn],
    minPartitions: Option[Int],
    maxPartitions: Option[Int])
  extends PartitionFunction
  with MinMaxPartitions

/**
  * A partition function that partitions by blocks.
  *
  * @param id The unique id of the partition function.
  * @param columns The columns this function partitions by.
  */
case class BlockPartitionFunction(
    id: String,
    columns: Seq[PartitionColumn],
    blockSize: Int,
    partitions: Int)
  extends PartitionFunction

/**
  * A column partitioned by a [[PartitionFunction]].
  *
  * @param id The id of the column.
  * @param dataType The data type of the column.
  */
case class PartitionColumn(id: String, dataType: String)

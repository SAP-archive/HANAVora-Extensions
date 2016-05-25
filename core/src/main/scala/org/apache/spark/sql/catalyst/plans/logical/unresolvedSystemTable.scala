package org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.expressions.Attribute

/**
  * An unresolved system table.
  *
  * This can either be an [[UnresolvedSparkLocalSystemTable]] or
  * an [[UnresolvedProviderBoundSystemTable]].
  */
sealed trait UnresolvedSystemTable extends LeafNode {
  /** The name of the system table. */
  val name: String

  override def output: Seq[Attribute] = Seq.empty

  override lazy val resolved: Boolean = false
}

/**
  * An unresolved spark local system table.
  *
  * @param name The name of the system table.
  */
case class UnresolvedSparkLocalSystemTable(name: String)
  extends UnresolvedSystemTable

/**
  * An unresolved provider bound system table.
  *
  * @param name The name of the system table.
  * @param provider The provider of the system table.
  * @param options The options of the system table.
  */
case class UnresolvedProviderBoundSystemTable(
    name: String,
    provider: String,
    options: Map[String, String])
  extends UnresolvedSystemTable

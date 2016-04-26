package org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.expressions.Attribute

/**
  * An unresolved system table.
  *
  * @param name The name of the system table.
  * @param provider The provider of the system table.
  * @param options The options of the system table.
  */
case class UnresolvedSystemTable(
    name: String,
    provider: String,
    options: Map[String, String]) extends LeafNode {
  override def output: Seq[Attribute] = Seq.empty

  override lazy val resolved: Boolean = false
}

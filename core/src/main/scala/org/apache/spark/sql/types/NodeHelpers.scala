package org.apache.spark.sql.types

object NodeHelpers {
  implicit object OrderedNode extends Ordering[Node] {
    override def compare(left: Node, right: Node): Int = left.compareTo(right)
  }
}

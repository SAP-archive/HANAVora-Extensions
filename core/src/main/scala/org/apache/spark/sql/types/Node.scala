package org.apache.spark.sql.types

case class Node(
                 path: Seq[Any],
                 var preRank: java.lang.Integer = null,
                 var postRank: java.lang.Integer = null,
                 isLeaf: java.lang.Boolean = null,
                 var ordPath: Seq[Long] = null) {

  def compareToRecursive(left: Seq[Long], right: Seq[Long]): Int =
  {
    (left.isEmpty, right.isEmpty) match {
      case (true, true) => 0    // both are equal
      case (true, false) => -1  // left is smaller than right
      case (false, true) => 1   // right is smaller than right
      case (false, false) =>
        if(left.head == right.head) {
          compareToRecursive(left.tail, right.tail)
        } else {
          left.head.compareTo(right.head)
        }
      }
  }

  def compareTo(that: Node): Int = compareToRecursive(ordPath, that.ordPath)

  /* XXX: No-arg constructor is provided to allow Kryo serialization */
  protected def this() = this(null)

  if (path != null && path.isEmpty) {
    throw new IllegalStateException("A Node cannot contain an empty path")
  }
}

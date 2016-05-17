package org.apache.spark.sql.types

import java.lang.Boolean

@SQLUserDefinedType(udt = classOf[NodeType])
case class Node(path: Seq[Any],
                pathDataTypeJson: String,
                var preRank: java.lang.Integer = null,
                var postRank: java.lang.Integer = null,
                isLeaf: java.lang.Boolean = null,
                var ordPath: Seq[Long] = null) {

  lazy val effectivePath: Seq[Any] = path.reverse.dropWhile(_ == null).reverse

  def compareToRecursive(left: Seq[Long], right: Seq[Long]): Int =
  {
    (left.isEmpty, right.isEmpty) match {
      case (true, true) => 0    // both are equal
      case (true, false) => -1  // left is smaller than right
      case (false, true) => 1   // right is smaller than right
      case (false, false) =>
        if (left.head == right.head) {
          compareToRecursive(left.tail, right.tail)
        } else {
          left.head.compareTo(right.head)
        }
      }
  }

  def compareTo(that: Node): Int = compareToRecursive(ordPath, that.ordPath)

  /* XXX: No-arg constructor is provided to allow Kryo serialization */
  protected def this() = this(null, null)

  if (path != null && path.isEmpty) {
    throw new IllegalStateException("A Node cannot contain an empty path")
  }
}

case object Node {
  def apply(path: Seq[Any],
            pathDataType: DataType,
            preRank: Integer,
            postRank: Integer,
            isLeaf: Boolean,
            ordPath: Seq[Long]): Node =
    Node(path, if (pathDataType == null) null else pathDataType.json,
      preRank, postRank, isLeaf, ordPath)

  def apply(path: Seq[Any],
            pathDataType: DataType,
            preRank: Integer,
            postRank: Integer,
            isLeaf: Boolean): Node =
    Node(path, if (pathDataType == null) null else pathDataType.json, preRank, postRank, isLeaf)

  def apply(path: Seq[Any],
            pathDataType: DataType,
            ordPath: Seq[Long]): Node =
    Node(path, if (pathDataType == null) null else pathDataType.json, ordPath = ordPath)

  def apply(path: Seq[Any],
            pathDataType: DataType): Node =
    Node(path, if (pathDataType == null) null else pathDataType.json)

}

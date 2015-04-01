package org.apache.spark.sql.types

import org.apache.spark.sql.Row

import scala.collection.JavaConversions._

object NodeType extends UserDefinedType[Node] {

  override val sqlType = StructType(Seq(
    StructField("path", ArrayType(LongType, containsNull = false), nullable = false),
    StructField("preRank", IntegerType, nullable = true),
    StructField("postRank", IntegerType, nullable = true),
    StructField("isLeaf", BooleanType, nullable = true)
  ))

  override def serialize(obj: Any): Any = obj match {
    case node : Node => Row(node.path, node.preRank, node.postRank, node.isLeaf)
    case _ => throw new UnsupportedOperationException(s"Cannot serialize ${obj.getClass}")
  }

  override def deserialize(datum: Any): Node = datum match {
    case row : Row =>
      new Node(
        row.getList[Any](0).toSeq,
        Option(row.getInt(1)),
        Option(row.getInt(2)),
        Option(row.getBoolean(3))
      )
    case seq : Seq[Any] => new Node(seq)
    case node : Node => node
    case _ => throw new UnsupportedOperationException(s"Cannot deserialize ${datum.getClass}")
  }

  override def userClass: java.lang.Class[Node] = classOf[Node]
}

case class Node(
                 path : Seq[Any],
                 preRank : Option[Int] = None,
                 postRank : Option[Int] = None,
                 isLeaf : Option[Boolean] = None
                 ) {
  if (path.isEmpty) {
    throw new IllegalStateException("A Node cannot contain an empty path")
  }
}

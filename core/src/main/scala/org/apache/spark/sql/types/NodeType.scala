package org.apache.spark.sql.types

import org.apache.spark.sql.Row

import scala.collection.JavaConversions._

object NodeType extends UserDefinedType[Node] {

  override val sqlType = StructType(Seq(
    StructField("path", ArrayType(LongType, containsNull = false), nullable = false),
    StructField("preRank", IntegerType, nullable = true),
    StructField("postRank", IntegerType, nullable = true),
    StructField("isLeaf", BooleanType, nullable = true),
    StructField("ordPath", ArrayType(LongType, containsNull=false), nullable = true)
  ))

  override def serialize(obj: Any): Any = obj match {
    case node: Node => Row(node.path, node.preRank, node.postRank, node.isLeaf, node.ordPath)
    case _ => throw new UnsupportedOperationException(s"Cannot serialize ${obj.getClass}")
  }

  override def deserialize(datum: Any): Node = datum match {
    case row: Row =>
      Node(
        row.getList[Any](0).toSeq,
        if (row.isNullAt(1)) null else row.getInt(1),
        if (row.isNullAt(2)) null else row.getInt(2),
        if (row.isNullAt(3)) null else row.getBoolean(3),
        // scalastyle:off magic.number
        if(row.isNullAt(4)) null else row.getList[Long](4).toSeq
        // scalastyle:on magic.number
      )
    case seq: Seq[Any] => Node(seq)
    case node: Node => node
    case _ => throw new UnsupportedOperationException(s"Cannot deserialize ${datum.getClass}")
  }

  override def userClass: java.lang.Class[Node] = classOf[Node]
}

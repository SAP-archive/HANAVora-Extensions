package org.apache.spark.sql.hierarchy

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.Node

private[hierarchy]  case class HierarchyRowFunctions(inputTypes: Seq[DataType]) {

  private[hierarchy] def rowGet[K](i: Int): Row => K = (row: Row) => row.getAs[K](i)

  private[hierarchy] def rowInit[K](pk: Row => K, pathDataType: DataType):
  (Row, Option[Long]) => Row = { (row, myOrdKey) =>
    myOrdKey match {
      case Some(x) => Row(row.toSeq ++ Seq(Node(List(pk(row)),
        pathDataType, ordPath = List(x))): _*)
      case None => Row(row.toSeq ++ Seq(Node(List(pk(row)), pathDataType)): _*)
    }
  }

  private[hierarchy] def rowModifyAndOrder[K](pk: Row => K, pathDataType: DataType):
  (Row, Row, Option[Long]) => Row = {
    (left, right, myord) => {
      val pathComponent: K = pk(right)
      // TODO(weidner): is myNode a ref/ptr or a copy of node?:
      val myNode: Node = /* & */left.getAs[Node](left.length - 1)
      val path: Seq[Any] = myNode.path ++ List(pathComponent)

      var node: Node = null  // Node(path, ordPath = myOrdPath)
      myord match {
        case Some(ord) =>
          val parentOrdPath = myNode.ordPath match {
            case x: Seq[Long] => x
            case _ => List()
          }
          node = Node(path, pathDataType, ordPath = parentOrdPath ++ List(ord))
        case None => node = Node(path, pathDataType)
      }
      Row(right.toSeq :+ node: _*)
    }
  }

  private[hierarchy] def rowModify[K](pk: Row => K, pathDataType: DataType):
    (Row, Row) => Row = { (left, right) =>
    val pathComponent: K = pk(right)
    val path: Seq[Any] = left.getAs[Node](left.length - 1).path ++ List(pathComponent)
    val node: Node = Node(path, pathDataType)
    Row(right.toSeq :+ node: _*)
  }

  private[hierarchy] def rowAppend[K](row: Row, node: Node): Row = {
    Row(row.toSeq :+ node: _*)
  }

  private[hierarchy] def rowStartWhere[K](exp: Expression): Row => Boolean = { row =>
    val numColumns = inputTypes.length
    val converters = inputTypes.map(CatalystTypeConverters.createToCatalystConverter)
    val values = Stream.from(0).takeWhile(_ < numColumns).map({ i =>
      converters(i)(row(i))
    })
    val newRow = InternalRow.fromSeq(values)
    exp.eval(newRow).asInstanceOf[Boolean]
  }

  private[hierarchy] def bindExpression(exp: Expression, attributes: Seq[Attribute])
  : Expression = exp.transform {
    case a: AttributeReference =>
      val index = attributes.indexWhere(_.name == a.name)
      BoundReference(index, a.dataType, a.nullable)
  }

}

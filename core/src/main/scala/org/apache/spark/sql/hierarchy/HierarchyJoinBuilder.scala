package org.apache.spark.sql.hierarchy

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{StructType, Node, NodeHelpers}
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

case class HierarchyJoinBuilder[T: ClassTag, O: ClassTag, K: ClassTag]
(
  startWhere: T => Boolean,
  pk: T => K,
  pred: T => K,
  init: (T, Option[Long]) => O,
  ord: T => Any,
  modify: (O, T, Option[Long]) => O
  )
  extends HierarchyBuilder[T,O] {

  private[hierarchy] def extractNodeFromRow(a: O): Option[Node] = {
    a match {
      case r: Row if r.length > 0 =>
        r.get(r.length - 1) match {
          case n: Node => Some(n)
          case _ => None
        }
      case _ => None
    }
  }

  private[hierarchy] def getOrd(rightRow: T): Option[Long] = {
    // ord map {x=>
//    x match {
    ord match {
      case null => None
      case _ => ord(rightRow) match {
        case ll: Long => Some(ll)
        case ii: Int => Some(ii.toLong)
        case _ => None
      }
    }
  }

  override def buildFromAdjacencyList(rdd: RDD[T]): RDD[O] = {
    val left0 = (rdd filter startWhere) keyBy pk mapValues(x =>
      init(x, getOrd(x))
      ) persist()
    if(left0.isEmpty) {
      sys.error("The hierarchy does not have any roots.")
    }
    val right = (rdd keyBy pred) persist()
    val result = foldRdd(left0, right, pk, modify) map (_._2) persist()
    right unpersist (blocking = true)
    left0 unpersist (blocking = true)

    if(ord == null){
      result
    }
    else {
      /* Sibling order is provided:
         1. sort rows distributed by node ordPath (lexicographical by sequence)
         2. assign distributed list ranking: equals pre-order rank DFS (in lexic. sorted RDD)
      * */
      /* TODO(weidner): remove this code duplication!
                        Idea: sortBy(...)(NodeHelpers.OrderedNode, ???)
      */
      implicit object OrderedNode extends Ordering[Node] {
        override def compare(left: Node, right: Node): Int = left.compareTo(right)
      }
      val myRet =
      result
        .sortBy(extractNodeFromRow(_))
        .zipWithIndex().map(x => {
        x._1 match {
          case e: Row => e.get (e.length - 1)
            .asInstanceOf[Node].preRank = x._2.toInt + 1
            e.get (e.length - 1).asInstanceOf[Node].ordPath = null
          case _ =>
        }
        x._1
      }).persist()
      myRet
    }
  }

  private[hierarchy] def foldRdd(left0: RDD[(K,O)],
                                 right: RDD[(K,T)],
                                 pk: T => K,
                                 modify: (O, T, Option[Long]) => O): RDD[(K,O)] = {
    var last: Option[RDD[(K,O)]] = None
    val result = Stream.iterate[Option[RDD[(K,O)]]](Some(left0))({
      case None => None
      case Some(left) if left.isEmpty() => None
      case Some(left) =>
        val newLeft = (left join right) map {
          case (key, (leftRow, rightRow)) =>
            (pk(rightRow), modify(leftRow, rightRow, getOrd(rightRow)))
        } persist StorageLevel.MEMORY_ONLY_SER
        left.unpersist (blocking = true)
        last = Some(newLeft)
        last
    })
      .takeWhile(_.nonEmpty)
      .flatten
      .reduce((l, r) => l.union(r))
      .persist()
    last.foreach(_.unpersist())
    result
  }

}

object HierarchyRowJoinBuilder {
  def apply(
             attributes: Seq[Attribute],
             parenthoodExpression: Expression,
             startWhere: Expression, searchBy: Seq[SortOrder]
             ): HierarchyBuilder[Row, Row] = {

    val predSuccIndexes: (Int, Int) = parenthoodExpression match {
      case EqualTo(
      left@AttributeReference(ln, ldt, _, _),
      right@AttributeReference(rn, rdt, _, _)
      ) if ldt == rdt =>
        val predIndex = attributes.indexWhere(_.name == ln)
        val succIndex = attributes.indexWhere(_.name == rn)
        (predIndex, succIndex)
      case _ =>
        throw new UnsupportedOperationException(
          s"Unsupported parenthood expression: $parenthoodExpression"
        )
    }
    val predIdx = predSuccIndexes._1
    val pkIdx = predSuccIndexes._2

    val pk = HierarchyRowFunctions.rowGet[java.lang.Long](pkIdx)
    val pred = HierarchyRowFunctions.rowGet[java.lang.Long](predIdx)
    val startsWhere = HierarchyRowFunctions.rowStartWhere(
      HierarchyRowFunctions.bindExpression(startWhere, attributes))
    // Todo(Weidner): currently, only first ordering rule is applied:
    val ord = searchBy.isEmpty match{
      case true => null
      case false =>
        HierarchyRowFunctions.rowGet[java.lang.Long](
          attributes.indexWhere(_.name ==
            searchBy.head.child.asInstanceOf[AttributeReference].name))
    }
    val init = HierarchyRowFunctions.rowInit(pk)
    val modify = HierarchyRowFunctions.rowModifyAndOrder(pk)

    new HierarchyJoinBuilder[Row,Row,Any](startsWhere, pk, pred, init, ord, modify)
  }
}

private[hierarchy] object HierarchyRowFunctions {

  private[hierarchy] def rowGet[K](i: Int): Row => K = (row: Row) => row.getAs[K](i)

  private[hierarchy] def rowInit[K](pk: Row => K):
    (Row, Option[Long]) => Row = { (row, myOrdKey) =>
    myOrdKey match {
      case Some(x) => Row(row.toSeq ++ Seq(Node(List(pk(row)), ordPath = List(x))): _*)
      case None => Row(row.toSeq ++ Seq(Node(List(pk(row)))): _*)
    }
  }

  private[hierarchy] def rowModifyAndOrder[K](pk: Row => K): (Row, Row, Option[Long]) => Row
  = {
    (left, right, myord) => {
      val pathComponent: K = pk(right)
      // TODO(weidner): is myNode a ref/ptr or a copy of node?:
      val myNode: Node = /* & */left.getAs[Node](left.length - 1)
      val path: Seq[Any] = myNode.path ++ List(pathComponent)

      var node: Node = null  // Node(path, ordPath = myOrdPath)
      myord match {
        case Some(ord) => {
          val parentOrdPath = {
            myNode.ordPath match {
              case x: Seq[Long] => x
              case _ => List()
            }
          }
          node = Node(path, ordPath = parentOrdPath ++ List(ord))
        }
        case None => node = Node(path)
      }
      Row(right.toSeq :+ node: _*)
    }
  }

  private[hierarchy] def rowModify[K](pk: Row => K): (Row, Row) => Row = { (left, right) =>
    val pathComponent: K = pk(right)
    val path: Seq[Any] = left.getAs[Node](left.length - 1).path ++ List(pathComponent)
    val node: Node = Node(path)
    Row(right.toSeq :+ node: _*)
  }

  private[hierarchy] def rowAppend[K](row: Row, node: Node): Row = {
    Row(row.toSeq :+ node: _*)
  }

  private[hierarchy] def rowStartWhere[K](exp: Expression): Row => Boolean = { row =>
    exp.eval(row).asInstanceOf[Boolean]
  }

  private[hierarchy] def bindExpression(exp: Expression, attributes: Seq[Attribute])
    : Expression = exp.transform {
    case a: AttributeReference =>
      val index = attributes.indexWhere(_.name == a.name)
      BoundReference(index, a.dataType, a.nullable)
  }

}

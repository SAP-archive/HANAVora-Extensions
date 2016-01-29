package org.apache.spark.sql.hierarchy

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, Node}
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

  override def buildFromAdjacencyList(rdd: RDD[T], pathDataType: DataType): RDD[O] = {
    val left0 = (rdd filter startWhere) keyBy pk mapValues(x =>
      init(x, getOrd(x))
      ) persist()
    if(left0.isEmpty()) {
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

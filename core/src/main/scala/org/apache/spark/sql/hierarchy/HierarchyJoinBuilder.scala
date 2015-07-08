package org.apache.spark.sql.hierarchy

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, Attribute}
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.types.Node
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

case class HierarchyJoinBuilder[T : ClassTag, O : ClassTag, K : ClassTag]
(
  startWhere: T => Boolean,
  pk : T => K, pred : T => K,
  init : T => O,
  modify : (O, T) => O
  )
  extends HierarchyBuilder[T,O] {

  override def buildFromAdjacencyList(rdd : RDD[T]) : RDD[O] = {
    val left0 = (rdd filter startWhere) keyBy pk mapValues init persist()
    val right = (rdd keyBy pred) persist()
    val result = foldRdd(left0, right, pk, modify) map (_._2) persist()
    right unpersist (blocking = true)
    left0 unpersist (blocking = true)
    result
  }

  private[hierarchy] def foldRdd(left0 : RDD[(K,O)],
                                 right : RDD[(K,T)],
                                 pk : T => K,
                                 modify : (O, T) => O) : RDD[(K,O)] = {
    var last : Option[RDD[(K,O)]] = None
    val result = Stream.iterate[Option[RDD[(K,O)]]](Some(left0))({
      case None => None
      case Some(left) if left.isEmpty() => None
      case Some(left) =>
        val newLeft = (left join right) map {
          case (key, (leftRow, rightRow)) =>
            (pk(rightRow), modify(leftRow, rightRow))
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
             attributes : Seq[Attribute],
             parenthoodExpression : Expression,
             startWhere : Expression, searchBy : Seq[SortOrder]
             ) : HierarchyBuilder[Row,Row] = {

    val predSuccIndexes : (Int, Int) = parenthoodExpression match {
      case EqualTo(
      left @ AttributeReference(ln, ldt, _, _),
      right @ AttributeReference(rn, rdt, _, _)
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
    val startsWhere = HierarchyRowFunctions.rowStartWhere(pred)
    val init = HierarchyRowFunctions.rowInit(pk)
    val modify = HierarchyRowFunctions.rowModify(pk)

    new HierarchyJoinBuilder[Row,Row,Any](startsWhere, pk, pred, init, modify) {
      override def buildFromAdjacencyList(rdd : RDD[Row]) : RDD[Row] = {
        /* FIXME: Hack to prevent wrong join results between Long and MutableLong? */
        val cleanRdd = rdd.map(row => Row(row.toSeq : _*))
        super.buildFromAdjacencyList(cleanRdd)
      }
    }
  }
}

private[hierarchy] object HierarchyRowFunctions {

  private[hierarchy] def rowGet[K](i : Int) : Row => K = (row : Row) => row.getAs[K](i)

  private[hierarchy] def rowInit[K](pk : Row => K) : Row => Row = { row =>
    Row(row.toSeq ++ Seq(Node(List(pk(row)))) : _*)
  }

  private[hierarchy] def rowModify[K](pk : Row => K) : (Row, Row) => Row = { (left, right) =>
    val pathComponent : K = pk(right)
    val path : Seq[Any] = left.getAs[Node](left.length - 1).path ++ List(pathComponent)
    val node : Node = Node(path)
    Row(right.toSeq :+ node : _*)
  }

  private[hierarchy] def rowAppend[K](row : Row, node : Node) : Row = {
    Row(row.toSeq :+ node : _*)
  }

  private[hierarchy] def rowStartWhere[K](pred : Row => K) : Row => Boolean = { row =>
    pred(row) == null
  }

}

package org.apache.spark.sql.hierarchy

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.Node

import scala.reflect.ClassTag


/* TODO (YH, SM) enhance this type, currently it is mutable. */
private[hierarchy] class Tree[T](
                                  val root: T,
                                  var children: Option[Seq[Tree[T]]],
                                  var siblingRank: java.lang.Long = null)
  extends Serializable {

  override def toString: String = {
    children.foldLeft(root.toString)((s, r) => (s + "-> " + r.toString()))
  }

}

case class HierarchyBroadcastBuilder[I: ClassTag, O: ClassTag, C: ClassTag, N: ClassTag]
(pred: I => C,
 succ: I => C,
 startWhere: I => Boolean,
 ord: I => C,
 transformRowFunction: (I, Node) => O) extends HierarchyBuilder[I, O] {

  def buildTree(t: Tree[C], list: Array[((C, C), (C,C))]): Unit = {
    if (t.children.isEmpty) {
      t.children = Some(
        list
          filter (p => p._1._1 == t.root)
          map (i => new Tree(i._1._2, None,
        {i._2._2 match {
          case l: Long => l
          case _ => -1L
        }}
        ))
      sortWith({(lhs,rhs) => lhs.siblingRank < rhs.siblingRank})
      )
    }
    if (t.children.isDefined) {
      t.children.get foreach (buildTree(_, list))
    }
  }

  def getTreePrefix(tree: Tree[C], id: C): Seq[C] =
  /* TODO (YH), enhance by having a hashtable of references and creating references to parents. */
    tree match {
      case t if t.root == id => Seq(t.root)
      case t if t.children.isEmpty => Nil
      case t =>
        t.children.get
          .map(getTreePrefix(_, id))
          .find(_.nonEmpty) match {
          case None => Nil
          case Some(result) => Seq(t.root) ++ result
        }
    }

  def getPrefix(f: Seq[Tree[C]], id: C): Seq[C] =
    f.map(getTreePrefix(_, id)).find(_.nonEmpty) match {
      case None =>
        throw new IllegalStateException("Unexpected, was not able to find prefix!")
      case Some(result) => result
    }

  override def buildFromAdjacencyList(rdd: RDD[I]): RDD[O] = {
    // providing sibling-order is optional: use succ as fallback for now:
    val ord_f = ord match {
      case null => succ
      case _ => ord
    }
    val adjacency = rdd keyBy pred mapValues succ
    val rank_list: RDD[(C, C)] = rdd keyBy pred mapValues ord_f
    val list_with_order = adjacency zip rank_list collect
    val roots = rdd filter startWhere map succ collect
    val forest = {
      val obj = roots map (root => new Tree(root, None))
      obj foreach(buildTree(_, list_with_order))
      obj
    }
    val forestBroadcast = rdd.sparkContext.broadcast(forest)
    /* TODO (YH) define extra attributes for Node */
    rdd map (x => transformRowFunction (x, Node(getPrefix(forestBroadcast.value, succ(x)))))
  }

}

object HierarchyRowBroadcastBuilder {
  def apply(attributes: Seq[Attribute],
            parenthoodExpression: Expression,
            startWhere: Expression,
            searchBy: Seq[SortOrder]): HierarchyBuilder[Row,Row] = {

    val predSuccIndexes: (Int, Int) = parenthoodExpression match {
      case EqualTo(
      left @ AttributeReference(ln, ldt, _, _),
      right @ AttributeReference(rn, rdt, _, _)) if ldt == rdt =>
        val predIndex = attributes.indexWhere(_.name == ln)
        val succIndex = attributes.indexWhere(_.name == rn)
        (predIndex, succIndex)
      case _ =>
        throw new UnsupportedOperationException(
          s"Unsupported parenthood expression: $parenthoodExpression"
        )
    }
    val predIdx = predSuccIndexes._1
    val succIdx = predSuccIndexes._2

    val succ = HierarchyRowFunctions.rowGet[java.lang.Long](succIdx)
    val pred = HierarchyRowFunctions.rowGet[java.lang.Long](predIdx)

    val startsWhere = HierarchyRowFunctions.rowStartWhere(
      HierarchyRowFunctions.bindExpression(startWhere, attributes))

    // Todo(Weidner): currently, only first ordering rule is applied:
    val ord = searchBy.isEmpty match{
      case true => null
      case false =>
        HierarchyRowFunctions.rowGet[java.lang.Long](
          attributes.indexWhere(_.name ==
            searchBy.head.child.asInstanceOf[AttributeReference].name ))
    }

    new HierarchyBroadcastBuilder[Row,Row,Any, Node](
      pred, succ, startsWhere, ord, HierarchyRowFunctions.rowAppend
    ) {
      override def buildFromAdjacencyList(rdd: RDD[Row]): RDD[Row] = {
        /* FIXME: Hack to prevent wrong join results between Long and MutableLong? */
        val cleanRdd = rdd.map(row => Row(row.toSeq: _*))
        super.buildFromAdjacencyList(cleanRdd)
      }
    }
  }
}

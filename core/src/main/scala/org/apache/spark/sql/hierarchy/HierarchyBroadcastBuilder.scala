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
                                  var preRank: java.lang.Integer = null,
                                  var postRank: java.lang.Integer = null,
                                  var siblingRank: java.lang.Long = null)
  extends Serializable {

  override def toString: String = {
    children.foldLeft(root.toString)((s, r) => (s + "-> " + r.toString()))
  }

}

case class PrePostRank(var preRank: java.lang.Integer,
                       var postRank: java.lang.Integer)

case class HierarchyBroadcastBuilder[I: ClassTag, O: ClassTag, C: ClassTag, N: ClassTag]
(pred: I => C,
 succ: I => C,
 startWhere: Option[I => Boolean],
 ord: I => C,
 transformRowFunction: (I, Node) => O) extends HierarchyBuilder[I, O] {

  def buildTree(t: Tree[C], list: Array[((C, C), (C,C))]): Unit = {
    if (t.children.isEmpty) {
      t.children = Some(
        list
          filter (p => p._1._1 == t.root)
          map (i => new Tree(i._1._2, None, siblingRank =
        {i._2._2 match {
          case l: Long => l
          case myi: Int => myi.toLong
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

  /**
   * Sets the pre/post-order DFS ranks of tree-root and works recursively on children
   * @param tree (sub-)tree to iterate
   * @param ranks last assigned-ranks of the caller
   * @return {pre-rank of the most right leaf, self-assigned post-rank}
   */
  private def setPrePostRank(tree: Tree[C], ranks: PrePostRank)
  : PrePostRank =
  /** returns last-assigned prerank */ {
    // we are in java, so let's take a copy - nobody will complain:
    var local_rank = PrePostRank(ranks.preRank + 1, ranks.postRank + 1)
    tree.preRank = local_rank.preRank
    tree.children match {
      case Some(child) => {
        child.foreach(subtree =>
          local_rank = setPrePostRank(subtree, local_rank))
      }
      case None =>
    }
    tree.postRank = local_rank.postRank
    local_rank.postRank += 1
    local_rank
  }

  /**
   * Extract the pre-/post-rank information for a given node from the tree
   * @param tree searched structure
   * @param id searched node
   * @return {pre-rank, post-rank} of searched node
   */
  private def getTreePreRank(tree: Tree[C], id: C): java.lang.Integer = {
    var return_pre_rank: java.lang.Integer = null
    if (tree.root == id) {
      return_pre_rank = tree.preRank
    } else {
      tree.children match {
        case Some(child) => child.foreach(subtree => {
          val loc_pre_rank: java.lang.Integer = getTreePreRank(subtree, id)
          if (loc_pre_rank != null) {
            return_pre_rank = loc_pre_rank
          }
        })
        case None =>
      }
    }
    return_pre_rank
  }

  /**
   * Wrapper to extract the pre/post rank for given node
   * @param f forest
   * @param id tree-node to search for
   * @return
   */
  private def getPreRank(f: Seq[Tree[C]], id: C): java.lang.Integer = {
    var return_pre_rank: java.lang.Integer = null
    f.foreach(t => {
      val local_pre_rank = getTreePreRank(t, id)
      if (local_pre_rank != null) {
        return_pre_rank = local_pre_rank
      }
    })
    return_pre_rank
  }

  override def buildFromAdjacencyList(rdd: RDD[I]): RDD[O] = {
    // providing sibling-order is optional: use succ as fallback for now:
    val ord_f = ord match {
      case null => succ
      case _ => ord
    }
    val adjacency = rdd keyBy pred mapValues succ
    val rank_list: RDD[(C, C)] = rdd keyBy pred mapValues ord_f
    val list_with_order = adjacency zip rank_list collect()

    val roots = rdd filter startWhere.getOrElse({
      val sucessors = rdd.map(succ).collect()
      in => !sucessors.contains(pred(in))
    }) map succ collect()

    if(roots.isEmpty) {
      sys.error("The hierarchy does not have any roots.")
    }

    val forest = {
      val obj = roots map (root => new Tree(root, None))
      obj foreach(buildTree(_, list_with_order))
      obj
    }

    /* Init pre-/post-rank for each tree node */
    var prev_rank = PrePostRank(0,0)
    forest.foreach( f => {
      prev_rank = setPrePostRank(f, prev_rank)
      // prepare for next tree in forest:
      prev_rank.preRank += 1
      prev_rank.postRank += 1
    })

    val forestBroadcast = rdd.sparkContext.broadcast(forest)
    /* TODO (YH) define extra attributes for Node */
    rdd map (x =>
      transformRowFunction (x, Node(getPrefix(forestBroadcast.value, succ(x)),
      getPreRank(forestBroadcast.value, succ(x)))))
  }

}

object HierarchyRowBroadcastBuilder {
  def apply(attributes: Seq[Attribute],
            parenthoodExpression: Expression,
            startWhere: Option[Expression],
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

    val startsWhere = startWhere map {
      case s => HierarchyRowFunctions.rowStartWhere(
        HierarchyRowFunctions.bindExpression(s, attributes))
    }

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

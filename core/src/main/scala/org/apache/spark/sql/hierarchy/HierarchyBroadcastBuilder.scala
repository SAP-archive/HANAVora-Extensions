package org.apache.spark.sql.hierarchy

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.Node

import scala.collection.mutable
import scala.reflect.ClassTag

private[hierarchy] class Forest[T](
                                    val refs: mutable.Map[T,Tree[T]],
                                    val trees: Seq[Tree[T]]
                                    ) extends Serializable {

  def findTree(id: T): Option[Tree[T]] = refs.get(id)

}

private[hierarchy] class Tree[T](
                                  val parent: Option[Tree[T]],
                                  val root: T,
                                  var preRank: Int = 0,
                                  var postRank: Int = 0,
                                  var isLeaf: Boolean = false)
  extends Serializable {

  override def toString: String =
    s"Tree(root=$root)"

  def prefix: Seq[T] =
    parent match {
      case None => root :: Nil
      case Some(p) => p.prefix :+ root
    }

}

private[hierarchy] case class Ranks(var pre: Int, var post: Int)

case class HierarchyBroadcastBuilder[I: ClassTag, O: ClassTag, C: ClassTag, N: ClassTag]
(pred: I => C,
 succ: I => C,
 startWhere: Option[I => Boolean],
 ord: I => C,
 transformRowFunction: (I, Node) => O) extends HierarchyBuilder[I, O] with Logging {

  def buildTree(refs: mutable.Map[C, Tree[C]],
                t: Tree[C],
                candidateMap: Map[C, Array[HRow[C]]],
                prevRanks: Ranks): Ranks = {
    refs.put(t.root, t)

    val children =
      candidateMap.getOrElse(t.root, Array[HRow[C]]())
        .sortWith({ (lhs, rhs) => lhs.ord < rhs.ord })
        .map({ childRow =>
          new Tree(parent = Some(t), root = childRow.succ)
        })

    t.isLeaf = children.isEmpty
    t.preRank = prevRanks.pre + 1
    var ranks = Ranks(t.preRank, prevRanks.post)
    children foreach { child =>
      ranks = buildTree(refs, child, candidateMap - t.root, ranks)
    }
    t.postRank = ranks.post + 1
    ranks.post += 1
    ranks
  }

  /**
   * Providing sibling-order is optional: use succ as fallback for now.
   * @param row
   * @return
   */
  private def orderFunc(row: I): Long = {
    val v = ord match {
      case null => succ(row)
      case _ => ord(row)
    }
    v match {
      case l: Long => l
      case i: Int => i.toLong
      case _ => -1L
    }
  }

  override def buildFromAdjacencyList(rdd: RDD[I]): RDD[O] = {
    logDebug(s"Collecting data to build hierarchy")
    val data = rdd mapPartitions { iter =>
      iter map { row =>
        HRow[C](
          pred = pred(row),
          succ = succ(row),
          ord = orderFunc(row),
          isRoot = startWhere.map({ sw => sw(row) })
        )
      }
    } collect()
    logDebug(s"Finished collecting data to build hierarchy")

    lazy val successors = data.map(_.succ).toSet
    logTrace(s"Partitioning roots / non-roots")
    val (roots, nonRoots) = data partition { hrow =>
      hrow.isRoot match {
        case Some(ir) => ir
        case None => !successors.contains(hrow.pred)
      }
    }
    if (roots.isEmpty) {
      rdd.sparkContext.emptyRDD[O]
    } else {
      logTrace(s"Grouping candidates")
      val candidateMap = nonRoots.groupBy(_.pred)

      logDebug(s"Building hierarchy")
      val refs = mutable.Map[C, Tree[C]]()
      val forest = {
        val trees = roots map { hrow => new Tree[C](None, hrow.succ, preRank = 1) }
        val refs = mutable.Map[C, Tree[C]]()
        trees map (buildTree(refs, _, candidateMap, Ranks(0, 0)))
        new Forest(refs, trees)
      }
      logDebug(s"Broadcasted hierarchy")
      val forestBroadcast = rdd.sparkContext.broadcast(forest)
      logDebug(s"Broadcasted hierarchy as: ${forestBroadcast.id}")

      rdd mapPartitions { iter =>
        iter flatMap { x =>
          val id = succ(x)
          val forest = forestBroadcast.value
          val subtreeOpt = forest.findTree(id)
          subtreeOpt map { subtree =>
            transformRowFunction(x, Node(
              path = subtree.prefix,
              preRank = subtree.preRank,
              postRank = subtree.postRank,
              isLeaf = subtree.isLeaf
            ))
          }
        }
      }
    }
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
            searchBy.head.child.asInstanceOf[AttributeReference].name))
    }

    new HierarchyBroadcastBuilder[Row,Row,Any, Node](
      pred, succ, startsWhere, ord, HierarchyRowFunctions.rowAppend
    )
  }
}

private case class HRow[C](pred: C, succ: C, ord: Long, isRoot: Option[Boolean])

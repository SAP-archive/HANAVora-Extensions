package org.apache.spark.sql.hierarchy

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{DataType, Node, StructType}
import org.apache.spark.sql.util.RddUtils

import scala.collection.{Seq, mutable}
import scala.reflect.ClassTag

// scalastyle:off method.length
case class HierarchyBroadcastBuilder[C: ClassTag, N: ClassTag]
(pred: Row => C,
 key: Row => C,
 startWhere: Option[Row => Boolean],
 attributes: Seq[Attribute],
 sortOrder: Seq[SortOrder],
 transformRowFunction: (Row, Node) => Row) extends HierarchyBuilder[Row, Row] with Logging {

  def buildTree(refs: mutable.Map[C, Tree[C]],
                t: Tree[C],
                candidateMap: Map[C, Array[HRow[C]]],
                prevRanks: Ranks): Ranks = {
    refs.put(t.root, t)

    val children =
      candidateMap.getOrElse(t.root, Array[HRow[C]]())
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

  override def buildHierarchyRdd(rdd: RDD[Row], pathDataType: DataType): RDD[Row] = {
    logDebug("ordering the source rdd")
    val converted = RddUtils.rddToRowRdd(rdd, StructType.fromAttributes(attributes))
    implicit val ordering = new InterpretedOrdering(sortOrder)
    val ordered = converted.sortBy {
      case row => row
    }
    val rowConverter = CatalystTypeConverters.createToScalaConverter(
      StructType.fromAttributes(attributes))

    logDebug(s"Collecting data to build hierarchy")
    val data = ordered mapPartitions { iter =>
      iter map { row =>
        HRow[C](
          pred = pred(rowConverter(row).asInstanceOf[Row]),
          succ = key(rowConverter(row).asInstanceOf[Row]),
          isRoot = startWhere.map({ sw => sw(rowConverter(row).asInstanceOf[Row]) })
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
      rdd.sparkContext.emptyRDD[Row]
    } else {
      logTrace(s"Grouping candidates")
      val candidateMap = nonRoots.groupBy(_.pred)

      logDebug(s"Building hierarchy")
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
          val id = key(x)
          val forest = forestBroadcast.value
          val subtreeOpt = forest.findTree(id)
          subtreeOpt map { subtree =>
            transformRowFunction(x, Node(
              path = subtree.prefix,
              pathDataType = pathDataType,
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
            searchBy: Seq[SortOrder]): HierarchyBuilder[Row, Row] = {

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

    val rowFunctions = HierarchyRowFunctions(attributes.map(_.dataType))
    val succ = rowFunctions.rowGet[java.lang.Long](succIdx)
    val pred = rowFunctions.rowGet[java.lang.Long](predIdx)

    val startsWhere = startWhere map {
      case s => rowFunctions.rowStartWhere(
        rowFunctions.bindExpression(s, attributes))
    }

    val boundOrd = searchBy.map {
      case SortOrder(child, direction) =>
        SortOrder(rowFunctions.bindExpression(child, attributes), direction)
    }

    new HierarchyBroadcastBuilder[Any, Node](
      pred, succ, startsWhere, attributes, boundOrd, rowFunctions.rowAppend
    )
  }
}

private case class HRow[C](pred: C, succ: C, isRoot: Option[Boolean])

private[hierarchy] case class Ranks(var pre: Int, var post: Int)

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

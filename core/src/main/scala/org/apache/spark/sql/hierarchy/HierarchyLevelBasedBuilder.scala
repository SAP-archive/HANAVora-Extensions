package org.apache.spark.sql.hierarchy

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LevelMatcher, MatchPath}
import org.apache.spark.sql.types.{DataType, Node, StructType}
import org.apache.spark.sql.util.RddUtils

import scala.reflect.ClassTag
import scala.collection._

// scalastyle:off
/**
  * Builds a level-based hierarchy.
  *
  * @param startWhere The start-where clause.
  * @param columns The levels.
  * @param internalNodeRowFunction A function that constructs an input row from a node path.
  * @param transformRowFunction A function that converts an input row with node to output row.
  * @tparam M The levels type.
  */
case class HierarchyLevelBasedBuilder[M: ClassTag](
   attributes: Seq[Attribute],
   startWhere: Option[Row => Boolean],
   sortOrder: Seq[SortOrder],
   columns: Row => Seq[M],
   internalNodeRowFunction: Seq[Option[M]] => Row,
   transformRowFunction: (Row, Node) => Row)
  extends HierarchyBuilder[Row, Row]
  with Logging {

  override def buildHierarchyRdd(rdd: RDD[Row], dataType: DataType): RDD[Row] = {
    val converted = RddUtils.rddToRowRdd(rdd, StructType.fromAttributes(attributes))
    implicit val ordering = new InterpretedOrdering(sortOrder)
    val ordered = converted.sortBy {
      case row => row
    }

    // get the columns RDD (collect in one machine)
    val rowConverter = CatalystTypeConverters.createToScalaConverter(
      StructType.fromAttributes(attributes))
    val columnsWithTags = ordered.mapPartitionsWithIndex {
      case (index, it) => it.map {

        case d => (columns(rowConverter(d).asInstanceOf[Row]), index)
      }
    }.collect()

    // build the forest.
    val forest = new LevelHierarchyForest[M]()
    columnsWithTags.foreach {
      case (cols, index) => forest.meld(cols, index)
    }

    forest.fixPrecalculations
    forest.fixTags

    // broadcast the forest to all nodes.
    val broadCastForest = rdd.sparkContext.broadcast(forest)

    // for each partition of the original RDD calculate the transformRowFunction
    // and produce necessary internal node rows.
    rdd flatMap {
      row => generateRows(broadCastForest.value, row, columns, dataType)
    }
  }

  def internalRows(node: HierarchyTree[M], tag: Int, dataType: DataType): Seq[Row] = {
    if (node.tag != tag || !node.isGenerated) {
      Seq.empty
    } else {
      val row = internalNodeRowFunction(node.toPath)
      val result = transformRowFunction(row, Node(columns(row), dataType, node.preRank,
        node.postRank, node.children.isEmpty, null))
      if (node.parent.isEmpty) {
        Seq(result)
      } else {
        internalRows(node.parent.get, tag, dataType) ++ Seq(result)
      }
    }
  }

  def generateRows(forest: LevelHierarchyForest[M],
                   row: Row,
                   columnFunc: Row => Seq[M],
                   dataType: DataType): Seq[Row] = {
    val node = forest.getPath(columnFunc(row))
    val result = Seq(transformRowFunction(row, Node(columns(row), dataType,
      node.preRank, node.postRank, node.children.isEmpty, null)))
    if (node.parent.isEmpty) result else internalRows(node.parent.get, node.tag, dataType) ++ result
  }
}

private[hierarchy] object HierarchyTree {

  /**
    * Converts a path to a [[HierarchyTree]]. The last node of the path is marked as
    * `not generated` and its tag is set to the passed `tag`.
    *
    * @param path The path to convert to a [[HierarchyTree]], must be non-empty effectively.
    * @param tag  The tag.
    * @tparam I The type of the path value.
    * @return The corresponding [[HierarchyTree]].
    */
  def toTree[I: ClassTag](path: Seq[I], tag: Int): HierarchyTree[I] = {
    val ePath = effectivePath(path)
    if (ePath.isEmpty) {
      sys.error(s"not expected, the effective path of this path: $path should not be empty.")
    }

    val result = ePath.map(HierarchyTree[I](_))
    result.sliding(2).foreach {
      case Seq(a, b) =>
        a.children += b
        b.parent = Some(a)
      case Seq(a) => a // single element, it is fine.
    }
    val head = result.reverse.head
    head.tag = tag // set the tag to the last element only (will fix it later).
    head.isGenerated = false // it is enough to pinpoint all nodes that will not generate its own
                             // row in the resulting table.
    result.head
  }

  /**
    * For a give path it returns the ''effective path''. The effective path is calculated as the
    * following:
    * all suffixing nulls are removed.
    * if a null exists to the left of a not-null value then it is substituted with a new
    * ''Ephemeral'' node that is unique.
    *
    * @param path The path.
    * @tparam I The type of the values.
    * @return The corresponding [[HierarchyNode]].
    */
  def effectivePath[I: ClassTag](path: Seq[I]): Seq[HierarchyNode[I]] = {
    val result = path.reverse.dropWhile(_ == null).reverse
    result.map {
      p => new HierarchyNode[I](Option(p))
    }
  }

  def fixTags[I: ClassTag](tree: HierarchyTree[I]): Unit = {
    tree.children.foreach(fixTags)
    if (tree.tag != -1) {
      if (tree.parent.nonEmpty && tree.parent.get.tag == -1) {
        tree.parent.get.tag = tree.tag
      }
    }
  }

  def fixPrecalculatedRanks[I: ClassTag](t: HierarchyTree[I], prevRanks: Ranks): Ranks = {
    t.preRank = prevRanks.pre + 1
    var ranks = Ranks(t.preRank, prevRanks.post)
    t.children.foreach {
      child => ranks = fixPrecalculatedRanks(child, ranks)
    }
    t.postRank = ranks.post + 1
    ranks.post += 1
    ranks
  }
}

private[hierarchy] case class HierarchyTree[I : ClassTag]
  (value: HierarchyNode[I],
   var parent: Option[HierarchyTree[I]] = None,
   children: scala.collection.mutable.Buffer[HierarchyTree[I]]
    = mutable.Buffer.empty[HierarchyTree[I]],
   var isGenerated: Boolean = true,
   var tag: Int = -1,
   var preRank: Int = -1,
   var postRank: Int = -1) {

  def add(path: Seq[I], tag: Int): Unit = {
    if (path.nonEmpty) {
      val potentialChild = HierarchyTree.toTree(path, tag)
      if (!children.exists(_.value.equals(potentialChild.value))) {
        children += potentialChild
        potentialChild.parent = Some(this)
      } else {
        val child = children.find(_.value.equals(potentialChild.value)).get
        if (path.length == 1) {
          child.isGenerated = false
          child.tag = tag
        } else {
          children.find(_.value.equals(potentialChild.value)).get.add(path.drop(1), tag)
        }
       }
    }
  }

  def toPath: Seq[Option[I]] = {
    val res = value.value
    if (parent.isEmpty) Seq(res) else parent.get.toPath ++ Seq(res)
  }

  override def toString: String = {
    toString("%")
  }

  def toString(prefix: String): String = {
    children.map {
      c => s"$prefix-$value (gen=$isGenerated,tag=$tag) -> $prefix-${c.value} " +
        s"(gen=${c.isGenerated},tag=${c.tag}) "
    }.mkString("\n") + "\n" + {
      children.map(_.toString(prefix)).mkString("\n")
    }
  }

  def find(path: Seq[I]): HierarchyTree[I] = {
    val effectivePath = HierarchyTree.toTree(path, -1)
    val matchingChild = children.find(_.value.equals(effectivePath.value))
    require(matchingChild.nonEmpty)
    if (effectivePath.children.isEmpty) {
      matchingChild.get
    } else {
      matchingChild.get.find(path.drop(1))
    }
  }
}

private[hierarchy] object HierarchyNode {
  private val atomicInteger: AtomicInteger = new AtomicInteger()
}

private[hierarchy] case class HierarchyNode[I: ClassTag](value: Option[I]) {

  val ephemeralNodeName =
    if (value.isEmpty)
      sys.error("Currently we can not handle intermediate nulls")
    else
      s"Ephemeral${HierarchyNode.atomicInteger.getAndIncrement()}"

  override def equals(that: Any): Boolean =
    that match {
      case that: HierarchyNode[I] =>
        (value.nonEmpty && that.value.nonEmpty && value.equals(that.value)) ||
          (value.isEmpty && that.value.isEmpty && ephemeralNodeName == that.ephemeralNodeName)
        case _ => false
    }

  override def hashCode: Int = {
    List[Int](value.hashCode, ephemeralNodeName.hashCode)
      .foldLeft(17)((l, r) => 31 * l + r)
  }

  override def toString: String = {
    value.getOrElse(ephemeralNodeName).toString
  }
}

private[hierarchy] case class LevelHierarchyForest[I: ClassTag](
   roots: mutable.Buffer[HierarchyTree[I]] = mutable.Buffer.empty[HierarchyTree[I]]) {

  def meld(path: Seq[I], tag: Int): Unit = {
    if (path.nonEmpty && HierarchyTree.effectivePath(path).nonEmpty) {
      val node = new HierarchyNode(Option(path.head))
      if (!roots.exists(_.value.equals(node))) {
        roots += HierarchyTree.toTree(path, tag)
      } else {
        roots.find(_.value.equals(node)).get.add(path.drop(1), tag)
      }
    }
  }

  def fixPrecalculations: Unit = {
    roots.foreach(HierarchyTree.fixPrecalculatedRanks(_, Ranks(0, 0)))
  }

  def fixTags: Unit = {
    roots.foreach(HierarchyTree.fixTags)
  }

  def getPath(path: Seq[I]): HierarchyTree[I] = {
    val effectivePath = HierarchyTree.toTree(path, -1)
    val matchingChild = roots.find(_.value.equals(effectivePath.value))
    require (matchingChild.nonEmpty)
    if (effectivePath.children.isEmpty) {
      matchingChild.get
    } else {
      matchingChild.get.find(path.drop(1))
    }
  }

  override def toString: String = {
    "digraph G {\n" + roots.zipWithIndex.map {
      case (root, index) =>
        s"root -> tree$index\ntree$index-> $index${root.value} " +
          s"(gen=${root.isGenerated},tag=${root.tag}) \n${root.toString(index.toString)}"
    }.mkString("\n") + "\n}"
  }
}

object HierarchyRowLevelBasedBuilder {
  def apply(attributes: Seq[Attribute],
            levels: Seq[Expression],
            startWhere: Option[Expression],
            searchBy: Seq[SortOrder],
            matcher: LevelMatcher): HierarchyBuilder[Row, Row] = {

    /**
      * Currently we support *only* path matcher. If another matcher is provided then we throw.
      */
    if (matcher != MatchPath) {
      throw new IllegalArgumentException(s"Unexpected level matcher type '$matcher', currently" +
        s" only the ${MatchPath.getClass.getSimpleName} is supported.")
    }

    val rowFunctions = HierarchyRowFunctions(attributes.map(_.dataType))

    val colIndices: Seq[Int] = levels.map {
      case l @ AttributeReference(n, _, _, _) => attributes.indexWhere(_.name == n)
    } // todo check types matching and that all of them are nullables!

    val colsFunc: Row => Seq[Any] = r => colIndices.map(r.get)

    val internalNodeRowFunction: Seq[Option[Any]] => Row =
      s => Row(s.map(_.orNull) ++ Seq.fill(attributes.length - s.length)(null): _*)

    val startsWhere = startWhere map {
      case s => rowFunctions.rowStartWhere(
        rowFunctions.bindExpression(s, attributes))
    }

    val boundOrd = searchBy.map {
      case SortOrder(child, direction) =>
        SortOrder(rowFunctions.bindExpression(child, attributes), direction)
    }

    new HierarchyLevelBasedBuilder[Any](
      attributes,
      startsWhere,
      boundOrd,
      colsFunc, internalNodeRowFunction, rowFunctions.rowAppend
    )
  }
}


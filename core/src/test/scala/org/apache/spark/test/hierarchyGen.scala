package org.apache.spark.test

import org.apache.spark.sql.types.{LongType, Node}
import org.scalacheck.{Arbitrary, Gen}

import scala.util.Random
import scalaz._
import Scalaz._
import scalaz.scalacheck.ScalazArbitrary._

// scalastyle:off file.size.limit

object HierarchyGen {

  val MIN_SIZE_TREE = 6
  val MAX_SIZE_TREE = 100

  /**
   * Generates arbitrary Scalaz Trees
   */
  val tree: Gen[Tree[Long]] = TreeArbitrary[Long](implicitly(Seqs.arb)).arbitrary

  def minSizedTree(minSize: Int): Gen[Tree[Long]] =
    tree suchThat { t => t.flatten.size > minSize }

  def sizedTree(minSize: Int, maxSize: Int): Gen[Tree[Long]] =
    tree suchThat { t =>
      val size = t.flatten.size
      size > minSize && size < maxSize
    }

  /**
   * Generates arbitrary Node. Each node is drawn from a different tree.
   */
  val node: Gen[Node] = tree flatMap { t => Gen.oneOf(treeToNodeSeq(t)) }

  /**
   * Generates arbitrary node sequences of a given size.
   * Each sequence is drawn from the same tree.
   */
  def nodes(n: Int): Gen[Seq[Node]] =
    sizedTree(n, MAX_SIZE_TREE) flatMap { t => Gen.pick(n, treeToNodeSeq(t)) }

  /**
   * Generates pairs of unequal nodes.
   */
  val unequalNodePair: Gen[(Node,Node)] = nodes(2) map (seq =>
    (seq.head, seq(1))) suchThat (p => p._1 != p._2)

  /**
   * Generates pairs of parent-child nodes.
   */
  val parentChildNodePair: Gen[(Node,Node)] = minSizedTree(2) flatMap { tree: Tree[Long] =>
    Gen.oneOf(
      tree.loc
        .coflatMap({ tl => (tl.parent.map(treeLocToNode), treeLocToNode(tl)) })
        .toTree.flatten.toSeq
        .filter(_._1.nonEmpty)
        .map(x => (x._1.get, x._2))
    )
  }

  /**
   * Generates pairs of non-equal sibling nodes.
   */
  val siblingNodePair: Gen[(Node,Node)] = minSizedTree(MIN_SIZE_TREE)
    .flatMap { tree: Tree[Long] =>
    Gen.oneOf(
      tree.loc
        .coflatMap({ tl => (tl.right.map(treeLocToNode), treeLocToNode(tl)) })
        .toTree.flatten.toSeq
        .filter(_._1.nonEmpty)
        .map(x => (x._1.get, x._2))
    )
  }


  private[spark] def treeLocToNode[A](treeLoc: TreeLoc[A]): Node = Node(
    path = treeLoc.path.reverse.toList,
    pathDataType = LongType, // only long types here
    preRank = treeLoc.root.tree.flatten.indexOf(treeLoc.getLabel),
    postRank = treeLoc.root.tree.levels.flatten.indexOf(treeLoc.getLabel),
    isLeaf = treeLoc.isLeaf
  )

  private[spark] def treeToNodeSeq[A](tree: Tree[A]): Seq[Node] =
    tree.loc.coflatMap({ tl => treeLocToNode(tl) }).toTree.flatten.toSeq


  private val r = new Random()

}


/**
 * Generator for sequential longs.
 * Sequence starts at 0, continues to Long.MaxValue and then from Long.MinValue.
 * Seqs.next() is synchronized.
 */
private object Seqs {
  private var currentSeq: Long = 0

  /**
   * Get the next long in the sequence.
   * @return
   */
  def next(): Long = {
    synchronized {
      if (currentSeq == Long.MaxValue) {
        currentSeq = Long.MinValue
      }
      val result = currentSeq
      currentSeq += 1
      result
    }
  }

  def arb: Arbitrary[Long] = Arbitrary {
    gen
  }

  def gen: Gen[Long] = Gen.resultOf[Int,Long] { x => next() }
}

package org.apache.spark.util

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, LeafNode}
import org.scalatest.FunSuite
import org.apache.spark.sql.util.PlanUtils._

class PlanUtilsSuite extends FunSuite {
  trait NoAttributes {
    self: LogicalPlan =>
    override def output: Seq[Attribute] = Seq.empty
  }
  case object Leaf extends LeafNode with NoAttributes
  case class Node(children: LogicalPlan*) extends LogicalPlan with NoAttributes

  val k = Leaf            //        _____a_____
  val j = Leaf            //       /     |     \
  val i = Leaf            //      b      e      k
  val h = Node(i)         //     / \    / \
  val g = Node(h, j)      //    c   d  f   g
  val f = Leaf            //              / \
  val e = Node(f, g)      //             h   j
  val d = Leaf            //             |
  val c = Leaf            //             i
  val b = Node(c, d)      //
  val a = Node(b, e, k)   //

  test("isLeaf") {
    assertResult(expected = false)(Node(Leaf).isLeaf)
    assertResult(expected = true)(Leaf.isLeaf)
  }

  test("find") {
    assertResult(None)(a.find(_ == Node(Leaf, Leaf, Leaf)))
    assertResult(Some(h))(a.find(_ == Node(Leaf)))
  }

  test("filter") {
    assertResult(Seq.empty)(a.filter(_ == Node(Leaf, Leaf, Leaf)))
    assertResult(Seq(c, d, f, i, j, k))(a.filter(_.isLeaf))
  }

  test("contains") {
    assertResult(expected = false)(a.contains(Node(Leaf, Leaf, Leaf)))
    assertResult(expected = true)(a.contains(Node(Leaf)))
  }

  test("exists") {
    assertResult(expected = true)(a.exists(node => node == Node(Leaf)))
    assertResult(expected = false)(a.exists(node => node == Node(Leaf, Leaf, Leaf)))
  }

  test("toPreOrderSeq") {
    assertResult(a.toPreOrderSeq.toList)(List(a, b, c, d, e, f, g, h, i, j, k))
  }

  test("toPostOrderSeq") {
    assertResult(a.toPostOrderSeq.toList)(List(c, d, b, f, i, h, j, g, e, k, a))
  }

  test("toLevelOrderSeq") {
    assertResult(a.toLevelOrderSeq.toList)(List(a, b, e, k, c, d, f, g, h, j, i))
  }

  test("toSeq") {
    assertResult(a.toSeq(PreOrder))(a.toPreOrderSeq)
    assertResult(a.toSeq(PostOrder))(a.toPostOrderSeq)
    assertResult(a.toSeq(LevelOrder))(a.toLevelOrderSeq)
  }
}

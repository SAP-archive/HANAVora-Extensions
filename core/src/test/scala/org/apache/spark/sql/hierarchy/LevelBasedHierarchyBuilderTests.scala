package org.apache.spark.sql.hierarchy

import org.apache.spark.sql.Row
import org.scalatest.FunSuite
import org.apache.spark.sql.types.{Node, StringType}

import scala.reflect.ClassTag

class LevelBasedHierarchyBuilderTests extends FunSuite {

  def levelColumnsFuncGen[I: ClassTag](indices: Seq[Int]): Row => Seq[I] = r =>
    indices.map(i => r.get(i).asInstanceOf[I])

  def columnFunc: Row => Seq[String] = levelColumnsFuncGen[String](Seq(0, 1, 2))

  val inputSize = 5 // columns for input table

  def internalNodeRowFunc: Seq[Option[String]] =>
    Row = s => Row(s.map(_.orNull) ++ Seq.fill(inputSize - s.length)(null): _*)

  def rowTransformFunc: (Row, Node) => Row = (r, n) => Row (r.toSeq :+ n: _*)

  val levelBasedBuilder = new HierarchyLevelBasedBuilder[String](
    null, null, null, columnFunc, internalNodeRowFunc, rowTransformFunc)

  test("Proper levels are handeled correctly") {
    // create some input
    // scalastyle:off magic.number (stupid rule anyway)
    val input = Seq(
      Seq("a", "b", "c", "value1", 10),
      Seq("a", "b", "d", "value2", 20),
      Seq("a", "b", "e", "value3", 30),
      Seq("c", "d", "q", "value4", 40)
    )
    val inputRows = input.map(line => Row(line: _*))
    // scalastyle:on magic.number

    // build forest, and fix its properties.
    val forest = LevelHierarchyForest[String]()
    inputRows.foreach {
      row => forest.meld(columnFunc(row), 1)
    }
    forest.fixTags
    forest.fixPrecalculations

    // the forest should look like this now (note that order is NOT calculated for now).
    //       a      c
    //      /       |
    //     b        d
    //     |        |
    //   / | \      |
    //  c  d  e     q

    // if we try to calculate the output of the first row (a,b,v,value1,10)we will get in return
    // three rows:
    // - row for node c.
    // - row for node b. (because it is generated, a parent of c, and shares the same tag (1).
    // - row for node a for the same reasons as above (recursively).

    val result = levelBasedBuilder.generateRows(forest, inputRows.head, columnFunc, StringType)

    // scalastyle:off magic.number (stupid rule anyway)
    assertResult(
      Seq(
        Row("a", null, null, null, null,
          Node(List("a", null, null), StringType, 1, 5, false, null)),
        Row("a", "b", null, null, null, Node(List("a", "b", null), StringType, 2, 4, false, null)),
        Row("a", "b", "c", "value1", 10,
          Node(List("a", "b", "c"), StringType, 3, 1, true, null))))(result)
    // scalastyle:on magic.number
  }

  test("Exception is thrown if a intermediate NULL is encountered") {
    // build forest.
    val forest = LevelHierarchyForest[String]()

    intercept[RuntimeException] {
      forest.meld(columnFunc(Row("a", null, "c", "value1", 10)), 1) // scalastyle:off
    }
  }

  // TODO More tests for edge cases, non-trailing nulls, ... etc.
}

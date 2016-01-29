package org.apache.spark.sql.hierarchy

import org.apache.spark.SparkConf
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.Node
import org.scalatest.FunSuite

class HierarchyBuilderSuite extends FunSuite {

  val N = 5
  val rowFunctions = HierarchyRowFunctions(Seq.fill(N)(StringType))

  test("HierarchyRowFunctions.rowGet") {
    for (i <- 0 to 5) {
      val row = Row((0 to 5).map(_.toString): _*)
      assertResult(i.toString)(rowFunctions.rowGet(i)(row))
    }
  }

  test("HierarchyRowFunctions.rowInit") {
    for (i <- 0 to 5) {
      val row = Row((0 to 5).map(_.toString): _*)

      val result = rowFunctions.rowInit(rowFunctions.rowGet(i), StringType)(row, None)
      val expected = Row(row.toSeq :+ Node(List(i.toString), StringType): _*)
      assertResult(expected)(result)
    }
  }

  // scalastyle:off magic.number
  test("HierarchyRowFunctions.rowInitWithOrder") {
    for (i <- 0 to 5) {
      val row = Row((0 to 5).map(_.toString): _*)
      val result = rowFunctions.rowInit(rowFunctions.rowGet(i), StringType)(row, Some(42L))
      val expected = Row(row.toSeq :+ Node(List(i.toString),StringType, ordPath = List(42L)): _*)
      assertResult(expected)(result)
    }
  }
  // scalastyle:on magic.number

  test("HierarchyRowFunctions.rowModify") {
    for (i <- 0 to 5) {
      val rightRow = Row(0 to 5: _*)
      val leftRow = Row("foo", 0, "bar", Node(List(0),StringType))
      val result = rowFunctions.rowModify(
        rowFunctions.rowGet(i),StringType
      )(leftRow, rightRow)
      val expected = Row((0 to 5) :+ Node(List(0, i), StringType): _*)
      assertResult(expected)(result)
    }
  }

  // scalastyle:off magic.number
  test("HierarchyRowFunctions.rowModifyAndOrder") {
    for (i <- 0 to 5) {
      val rightRow = Row(0 to 5: _*)
      val leftRow = Row("foo", 0, "bar", Node(List(0),StringType))
      val result = rowFunctions.rowModifyAndOrder(
        rowFunctions.rowGet(i), StringType
      )(leftRow, rightRow, Some(42L))
      val expected = Row((0 to 5) :+ Node(List(0, i), StringType, ordPath = List(42L)): _*)
      assertResult(expected)(result)
    }
  }
  // scalastyle:on magic.number

  test("HierarchyBuilder closure is serializable") {
    val closureSerializer = new JavaSerializer(new SparkConf(loadDefaults = false)).newInstance()
    val serialized = closureSerializer.serialize(() =>
      HierarchyJoinBuilder(null, null, null, null, null, null))
  }

  test("HierarchyRowFunctions closure is serializable") {
    val closureSerializer = new JavaSerializer(new SparkConf(loadDefaults = false)).newInstance()
    val serialized = closureSerializer.serialize(() =>
      HierarchyRowJoinBuilder(null, null, null, null))
  }

}

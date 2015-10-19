package org.apache.spark.sql.hierarchy

import org.apache.spark.SparkConf
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.Node
import org.scalatest.FunSuite

class HierarchyBuilderSuite extends FunSuite {

  test("HierarchyRowFunctions.rowGet") {
    for (i <- 0 to 5) {
      val row = Row((0 to 5).map(_.toString): _*)
      assertResult(i.toString)(HierarchyRowFunctions.rowGet(i)(row))
    }
  }

  test("HierarchyRowFunctions.rowInit") {
    for (i <- 0 to 5) {
      val row = Row((0 to 5).map(_.toString): _*)
      val result = HierarchyRowFunctions.rowInit(HierarchyRowFunctions.rowGet(i))(row, None)
      val expected = Row(row.toSeq :+ Node(List(i.toString)): _*)
      assertResult(expected)(result)
    }
  }

  // scalastyle:off magic.number
  test("HierarchyRowFunctions.rowInitWithOrder") {
    for (i <- 0 to 5) {
      val row = Row((0 to 5).map(_.toString): _*)
      val result = HierarchyRowFunctions.rowInit(HierarchyRowFunctions.rowGet(i))(row, Some(42L))
      val expected = Row(row.toSeq :+ Node(List(i.toString), ordPath = List(42L)): _*)
      assertResult(expected)(result)
    }
  }
  // scalastyle:on magic.number

  test("HierarchyRowFunctions.rowModify") {
    for (i <- 0 to 5) {
      val rightRow = Row(0 to 5: _*)
      val leftRow = Row("foo", 0, "bar", Node(List(0)))
      val result = HierarchyRowFunctions.rowModify(
        HierarchyRowFunctions.rowGet(i)
      )(leftRow, rightRow)
      val expected = Row((0 to 5) :+ Node(List(0, i)): _*)
      assertResult(expected)(result)
    }
  }

  // scalastyle:off magic.number
  test("HierarchyRowFunctions.rowModifyAndOrder") {
    for (i <- 0 to 5) {
      val rightRow = Row(0 to 5: _*)
      val leftRow = Row("foo", 0, "bar", Node(List(0)))
      val result = HierarchyRowFunctions.rowModifyAndOrder(
        HierarchyRowFunctions.rowGet(i)
      )(leftRow, rightRow, Some(42L))
      val expected = Row((0 to 5) :+ Node(List(0, i), ordPath = List(42L)): _*)
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

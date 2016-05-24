package org.apache.spark.sql.currency.basic

import org.scalatest.FunSuite

class DualKeyPartialSortedMapSuite extends FunSuite {
  type UnorderedKey = (String, String)
  type OrderedKey = Int

  test("usage with un/existing unordered and ordered keys") {
    val dkMap = new DualKeyPartialSortedMap[UnorderedKey, OrderedKey, Int]()
    val N = 300
    /*
     * Adds even numbers as sorted keys and values.
     * Checks that even numbered keys (they exist in map) have the right value,
     * and that odd numbered keys K (they do not exist in map) K have the value K-1
     */
    (0 to N).foreach { i =>
      dkMap.put(("A", "B"), i*2, i*2)
      dkMap.put(("A", "C"), i*2, i*2)
      dkMap.put(("B", "C"), i*2, i*2)
    }
    (0 to N*2).foreach { i =>
      val checker = i % 2 == 0 match {
        case true => i
        case false => i - 1
      }
      assert(dkMap.getSortedKeyFloorValue(("A", "B"), i).get == checker)
      assert(dkMap.getSortedKeyFloorValue(("A", "C"), i).get == checker)
      assert(dkMap.getSortedKeyFloorValue(("B", "C"), i).get == checker)
      assert(dkMap.getSortedKeyFloorValue(("A", "Y"), i).isEmpty)
      assert(dkMap.getSortedKeyFloorValue(("X", "Y"), i).isEmpty)
    }
    assert(dkMap.getSortedKeyFloorValue(("A", "B"), -1).isEmpty)
  }

  test("floor") {
    val dkMap = new DualKeyPartialSortedMap[UnorderedKey, OrderedKey, Int]()
    val unorderedKey = ("A", "B")
    val orderedKey = 50
    val lowerOrderedKey = 1
    val higherOrderedKey = 10000
    val value = 111
    dkMap.put(unorderedKey, orderedKey, value)
    assert(dkMap.getSortedKeyFloorValue(unorderedKey, lowerOrderedKey).isEmpty)
    assert(dkMap.getSortedKeyFloorValue(unorderedKey, orderedKey).get == value)
    assert(dkMap.getSortedKeyFloorValue(unorderedKey, higherOrderedKey).get == value)
  }

  test("empty map") {
    val dkMap = new DualKeyPartialSortedMap[UnorderedKey, OrderedKey, Int]()
    assert(dkMap.getSortedKeyFloorValue(("A", "A"), 0).isEmpty)
  }

  test("clear") {
    val dkMap = new DualKeyPartialSortedMap[UnorderedKey, OrderedKey, Int]()
    val unorderedKey = ("A", "B")
    val orderedKey = 50
    val value = 111
    dkMap.put(unorderedKey, orderedKey, value)
    assert(dkMap.getSortedKeyFloorValue(unorderedKey, orderedKey).get == value)
    dkMap.clear()
    assert(dkMap.getSortedKeyFloorValue(unorderedKey, orderedKey).isEmpty)
  }
}

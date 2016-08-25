package org.apache.spark.util

import org.scalatest.FunSuite
import org.apache.spark.sql.util.CollectionUtils._

class CollectionUtilsSuite extends FunSuite {
  val testMap = Map("a" -> 1, "b" -> 2)

  test("putIfAbsent does not override present values") {
    assertResult(testMap)(testMap.putIfAbsent("a", 0))
  }

  test("putIfAbsent returns a map with the value added if it was not present") {
    assertResult(testMap.updated("c", 3))(testMap.putIfAbsent("c", 3))
  }

  test("CaseInsensitiveMap stores the keys case insensitive") {
    val caseInsensitive = CaseInsensitiveMap(testMap)
    assertResult(1)(caseInsensitive("A"))
    assertResult(1)(caseInsensitive("a"))
  }

  test("CaseInsensitiveMap treats equality and hashcode correctly") {
    val c1 = CaseInsensitiveMap(testMap)
    val c2 = CaseInsensitiveMap(testMap)
    val c3 = CaseInsensitiveMap(testMap.updated("c", 3))

    assert(c1 == testMap)
    assert(testMap == c1)
    assert(c1 == c2)
    assert(c2 == c1)
    assert(c3 != c1)
    assert(c1.hashCode() == c2.hashCode())
    assert(c3.hashCode() != c1.hashCode())
  }

  test("Adding a value to a case insensitive map works") {
    val caseInsensitive = CaseInsensitiveMap(testMap)
    val withAddition = caseInsensitive + ("c" -> 3)
    assertResult(3)(withAddition("c"))
  }

  test("Removing a value from a case insensitive map works") {
    val caseInsensitive = CaseInsensitiveMap(testMap)
    val withRemoval = caseInsensitive - "a"
    assertResult(None)(withRemoval.get("a"))
  }

  test("Adding and removing values from a case insensitive map return a case insensitive map") {
    val caseInsensitive = CaseInsensitiveMap(testMap)
    val withRemoval = caseInsensitive - "a"
    val withAddition = caseInsensitive + ("c" -> 3)

    assert(withRemoval.isInstanceOf[CaseInsensitiveMap[_]])
    assert(withAddition.isInstanceOf[CaseInsensitiveMap[_]])
  }

  test("Duplicates returns all duplicate values in an iterable") {
    val withDuplicates = Seq("a", "b", "c", "a", "d", "b")
    assertResult(Set("a", "b"))(withDuplicates.duplicates)
  }

  test("Duplicates returns an empty set in case there are no duplicates in an iterable") {
    val withoutDuplicates = Seq("a", "b", "c")
    assertResult(Set.empty)(withoutDuplicates.duplicates)
  }
}

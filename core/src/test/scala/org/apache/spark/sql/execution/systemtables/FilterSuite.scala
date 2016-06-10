package org.apache.spark.sql.execution.systemtables

import org.apache.spark.sql.catalyst.analysis.systables.filterToFunction
import org.apache.spark.sql.sources._
import org.scalatest.FunSuite

class FilterSuite extends FunSuite {
  def conversionWithoutRemains(filter: Filter, attributes: Seq[String]): (Seq[Any] => Boolean) = {
    val (opt, fn) = filterToFunction(filter, attributes)
    assert(opt.isEmpty)
    fn
  }

  def assertSimpleFilter(filter: Filter, filterAttribute: String, attributeArgument: Any): Unit = {
    test(s"Filter $filter should yield true for $filterAttribute = $attributeArgument") {
      val fn = conversionWithoutRemains(filter, Seq(filterAttribute))
      assert(fn(Seq(attributeArgument)))
    }
  }

  def assertNotSimpleFilter(filter: Filter,
                            filterAttribute: String,
                            attributeArgument: Any): Unit = {
    test(s"Filter $filter should yield false for $filterAttribute = $attributeArgument") {
      val fn = conversionWithoutRemains(filter, Seq(filterAttribute))
      assert(!fn(Seq(attributeArgument)))
    }
  }

  assertSimpleFilter(EqualTo("foo", 1), "foo", 1)
  assertNotSimpleFilter(EqualTo("foo", 1), "foo", 2)
  assertSimpleFilter(EqualNullSafe("foo", 1), "foo", 1)
  assertNotSimpleFilter(EqualNullSafe("foo", 1), "foo", 2)
  assertSimpleFilter(GreaterThan("foo", 1), "foo", 2)
  assertSimpleFilter(GreaterThan("foo", "a"), "foo", "b")
  assertSimpleFilter(GreaterThan("foo", 0L), "foo", 1L)
  assertNotSimpleFilter(GreaterThan("foo", 1), "foo", 1)
  assertNotSimpleFilter(GreaterThan("foo", "a"), "foo", "a")
  assertNotSimpleFilter(GreaterThan("foo", 0L), "foo", 0L)
  assertSimpleFilter(GreaterThanOrEqual("foo", 2), "foo", 2)
  assertSimpleFilter(GreaterThanOrEqual("foo", "a"), "foo", "a")
  assertSimpleFilter(GreaterThanOrEqual("foo", 1L), "foo", 1L)
  assertSimpleFilter(GreaterThanOrEqual("foo", 1), "foo", 2)
  assertSimpleFilter(GreaterThanOrEqual("foo", "a"), "foo", "b")
  assertSimpleFilter(GreaterThanOrEqual("foo", 0L), "foo", 1L)
  assertNotSimpleFilter(GreaterThanOrEqual("foo", 2), "foo", 1)
  assertNotSimpleFilter(GreaterThanOrEqual("foo", "b"), "foo", "a")
  assertNotSimpleFilter(GreaterThanOrEqual("foo", 1L), "foo", 0L)
  assertSimpleFilter(LessThan("foo", 2), "foo", 1)
  assertSimpleFilter(LessThan("foo", "b"), "foo", "a")
  assertSimpleFilter(LessThan("foo", 1L), "foo", 0L)
  assertNotSimpleFilter(LessThan("foo", 2), "foo", 2)
  assertNotSimpleFilter(LessThan("foo", "a"), "foo", "b")
  assertNotSimpleFilter(LessThan("foo", 1L), "foo", 1L)
  assertSimpleFilter(LessThanOrEqual("foo", 1), "foo", 1)
  assertSimpleFilter(LessThanOrEqual("foo", "a"), "foo", "a")
  assertSimpleFilter(LessThanOrEqual("foo", 0L), "foo", 0L)
  assertSimpleFilter(LessThanOrEqual("foo", 2), "foo", 1)
  assertSimpleFilter(LessThanOrEqual("foo", "b"), "foo", "a")
  assertSimpleFilter(LessThanOrEqual("foo", 1L), "foo", 0L)
  assertNotSimpleFilter(LessThanOrEqual("foo", 1), "foo", 2)
  assertNotSimpleFilter(LessThanOrEqual("foo", "a"), "foo", "b")
  assertNotSimpleFilter(LessThanOrEqual("foo", 0L), "foo", 1L)
  assertSimpleFilter(In("foo", Array(0, 1, 2)), "foo", 1)
  assertNotSimpleFilter(In("foo", Array(1, 2)), "foo", 0)
  assertSimpleFilter(IsNull("foo"), "foo", null)
  assertNotSimpleFilter(IsNull("foo"), "foo", 1)
  assertSimpleFilter(IsNotNull("foo"), "foo", 1)
  assertNotSimpleFilter(IsNotNull("foo"), "foo", null)
  assertSimpleFilter(StringStartsWith("foo", "foo"), "foo", "foobar")
  assertNotSimpleFilter(StringStartsWith("foo", "foo"), "foo", "barfoo")
  assertSimpleFilter(StringEndsWith("foo", "bar"), "foo", "foobar")
  assertNotSimpleFilter(StringEndsWith("foo", "bar"), "foo", "barfoo")
  assertSimpleFilter(StringContains("foo", "oba"), "foo", "foobar")
  assertNotSimpleFilter(StringContains("foo", "abo"), "foo", "foobar")
  assertSimpleFilter(Not(StringContains("foo", "oba")), "foo", "not")
  assertNotSimpleFilter(Not(StringContains("foo", "oba")), "foo", "foobar")
  assertSimpleFilter(And(GreaterThan("foo", 0), LessThan("foo", 2)), "foo", 1)
  assertNotSimpleFilter(And(GreaterThan("foo", 0), LessThan("foo", 2)), "foo", 0)
  assertNotSimpleFilter(And(GreaterThan("foo", 0), LessThan("foo", 2)), "foo", 2)
  assertSimpleFilter(Or(GreaterThan("foo", 1), LessThan("foo", 1)), "foo", 0)
  assertSimpleFilter(Or(GreaterThan("foo", 1), LessThan("foo", 1)), "foo", 2)
  assertNotSimpleFilter(Or(GreaterThan("foo", 1), LessThan("foo", 1)), "foo", 1)

  test("EqualTo returns false on null values") {
    val filter1 = EqualTo("foo", null)
    val filter2 = EqualTo("foo", 1)
    val fn1 = conversionWithoutRemains(filter1, Seq("foo"))
    val fn2 = conversionWithoutRemains(filter2, Seq("foo"))

    assert(!fn1(Seq(1)))
    assert(!fn1(Seq(null)))
    assert(!fn2(Seq(null)))
    assert(fn2(Seq(1)))
    assert(!fn2(Seq(0)))
  }

  test("Partial AND resolution") {
    val filter = And(EqualTo("foo", 1), EqualTo("bar", 2))
    val (remains, fn) = filterToFunction(filter, Seq("foo"))

    assertResult(Some(EqualTo("bar", 2)))(remains)
    assert(fn(Seq(1)))
    assert(!fn(Seq(2)))
  }

  test("Partial OR resolution") {
    val filter = Or(EqualTo("foo", 1), EqualTo("bar", 2))
    val (remains, fn) = filterToFunction(filter, Seq("foo"))
    assertResult(Some(filter))(remains)
    assert(fn(Seq(2)))
    assert(fn(Seq(1)))
    assert(fn(Seq(0)))
  }

  test("Partial NOT with OR resolution") {
    val filter = Not(Or(EqualTo("foo", 1), EqualTo("bar", 2)))
    val (remains, fn) = filterToFunction(filter, Seq("foo"))
    assertResult(Some(filter))(remains)
    assert(fn(Seq(0)))
    assert(fn(Seq(1)))
  }

  test("Partial NOT with AND resolution") {
    val filter = Not(And(EqualTo("foo", 1), EqualTo("bar", 2)))
    val (remains, fn) = filterToFunction(filter, Seq("foo"))
    assertResult(Some(filter))(remains)
    assert(fn(Seq(0)))
    assert(fn(Seq(1)))
  }

  test("multi value and validation") {
    val filter = And(EqualTo("foo", 1), EqualTo("bar", 2))
    val fn = conversionWithoutRemains(filter, Seq("foo", "bar"))
    assert(!fn(Seq(1, 0)))
    assert(!fn(Seq(0, 2)))
    assert(!fn(Seq(0, 0)))
    assert(fn(Seq(1, 2)))
  }

  test("multi value or validation") {
    val filter = Or(EqualTo("foo", 1), EqualTo("bar", 2))
    val fn = conversionWithoutRemains(filter, Seq("foo", "bar"))
    assert(fn(Seq(1, 0)))
    assert(fn(Seq(0, 2)))
    assert(fn(Seq(1, 2)))
    assert(!fn(Seq(0, 0)))
  }
}

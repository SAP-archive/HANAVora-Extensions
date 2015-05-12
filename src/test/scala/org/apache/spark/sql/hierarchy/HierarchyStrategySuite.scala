package org.apache.spark.sql.hierarchy

import org.apache.spark.{MockitoSparkContext, SparkContext}
import org.scalatest.PrivateMethodTester._
import org.scalatest.{BeforeAndAfter, FunSuite, ParallelTestExecution}

// scalastyle:off magic.number

class HierarchyStrategySuite extends FunSuite
  with MockitoSparkContext {

  test("unit: test hierarchy strategy with default context configuration") {
    val strategy = new HierarchyStrategy(Nil, null, null, Nil)
    val parseConfigMethod = PrivateMethod[SparkContext]('parseConfig)
    val expected = ("undefined", HierarchyStrategy.THRESHOLD)
    assertResult(expected)(strategy invokePrivate parseConfigMethod(sc))
  }

  test("unit: test hierarchy strategy with valid context configuration") {
    val strategy = new HierarchyStrategy(Nil, null, null, Nil)
    val parseConfigMethod = PrivateMethod[SparkContext]('parseConfig)
    sc.conf.set("hierarchy.always", "broadcast")
    var expected = ("broadcast", HierarchyStrategy.THRESHOLD)
    assertResult(expected)(strategy invokePrivate parseConfigMethod(sc))
    sc.conf.set("hierarchy.always", "join")
    expected = ("join", HierarchyStrategy.THRESHOLD)
    assertResult(expected)(strategy invokePrivate parseConfigMethod(sc))
    sc.conf.set("hierarchy.always", "undefined")
    sc.conf.set("hierarchy.threshold", "10000")
    expected = ("undefined", 10000L)
    assertResult(expected)(strategy invokePrivate parseConfigMethod(sc))
  }

  test("unit: setting invalid context configuration for hierarchy strategy throws") {
    val strategy = new HierarchyStrategy(Nil, null, null, Nil)
    val parseConfigMethod = PrivateMethod[SparkContext]('parseConfig)
    sc.conf.set("hierarchy.threshold", "-100")
    intercept[IllegalArgumentException]{
      strategy invokePrivate parseConfigMethod(sc)
    }
    sc.conf.set("hierarchy.threshold", "1000")
    sc.conf.set("hierarchy.always", "invalid")
    intercept[IllegalArgumentException]{
      strategy invokePrivate parseConfigMethod(sc)
    }
  }

  test("unit: below threshold show use broadcast hierarchy") {
    val strategy = new HierarchyStrategy(Nil, null, null, Nil)
    val getBuilderMethod = PrivateMethod[Boolean]('useBroadcastHierarchy)
    val actual = strategy invokePrivate getBuilderMethod(("undefined", 10L),  () => 9L)
    assertResult(true)(actual)
  }

  test("unit: above or equal threshold show use join hierarchy") {
    val strategy = new HierarchyStrategy(Nil, null, null, Nil)
    val getBuilderMethod = PrivateMethod[Boolean]('useBroadcastHierarchy)
    var actual = strategy invokePrivate getBuilderMethod(("undefined", 10L),  () => 11L)
    assertResult(false)(actual)
    actual = strategy invokePrivate getBuilderMethod(("undefined", 10L),  () => 10L)
    assertResult(false)(actual)
  }

  test("unit: explicit implementation in config should override threshold") {
    val strategy = new HierarchyStrategy(Nil, null, null, Nil)
    val getBuilderMethod = PrivateMethod[Boolean]('useBroadcastHierarchy)
    var actual = strategy invokePrivate getBuilderMethod(("broadcast", 10L),  () => 11L)
    assertResult(true)(actual)
    actual = strategy invokePrivate getBuilderMethod(("broadcast", 10L),  () => 10L)
    assertResult(true)(actual)
    actual = strategy invokePrivate getBuilderMethod(("join", 10L),  () => 9L)
    assertResult(false)(actual)
  }
}

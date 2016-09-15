package com.sap.commons

import org.scalatest.FunSuite

// scalastyle:off magic.number
class TimingTestUtilsSuite extends FunSuite {
  val random = new scala.util.Random(123)

  test("no correlation for random exec times with default params") {
    val (testResult, _, _) = TimingTestUtils.executionTimeNotCorrelatedWithRuns() {
      Thread.sleep(random.nextInt(10))
    }
    assertResult(true)(testResult)
  }

  test("correlation for strictly increasing times") {
    var ms = 1
    val (testResult, _, _) = TimingTestUtils.executionTimeNotCorrelatedWithRuns() {
      Thread.sleep(ms)
      ms += 1
    }
    assertResult(false)(testResult)
  }

  test("correlation for noisy increasing times") {
    var ms = 1
    val (testResult, _, _) = TimingTestUtils.executionTimeNotCorrelatedWithRuns() {
      Thread.sleep(random.nextInt(10) + ms)
      ms += 5
    }
    assertResult(false)(testResult)
  }
}

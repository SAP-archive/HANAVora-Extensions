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

  test("correlation for noisy increasing times with default params") {
    var ms = 1
    val (testResult, _, _) = TimingTestUtils.executionTimeNotCorrelatedWithRuns() {
      Thread.sleep(random.nextInt(10) + ms)
      ms += 5
    }
    assertResult(false)(testResult)
  }

  test("correlation for strictly increasing times") {
    var ms = 1
    val (testResult, _, _) = TimingTestUtils.executionTimeNotCorrelatedWithRuns(
      warmupRuns = 0,
      runs = 10
    ) {
      Thread.sleep(ms)
      ms += 1
    }
    assertResult(false)(testResult)
  }

  test("correlation for strictly decreasing times") {
    var ms = 200
    val (testResult, _, _) = TimingTestUtils.executionTimeNotCorrelatedWithRuns(
      warmupRuns = 0,
      runs = 10,
      testNegativeCorr = true
    ) {
      Thread.sleep(ms)
      ms -= 5
    }
    assertResult(false)(testResult)
  }

  test("consider only positive correlation") {
    var ms = 200
    val (testResult, _, _) = TimingTestUtils.executionTimeNotCorrelatedWithRuns(
      warmupRuns = 0,
      runs = 10,
      testPositiveCorr = true,
      testNegativeCorr = false) {
      Thread.sleep(ms)
      ms -= 5
    }
    assertResult(true)(testResult)
  }

  test("consider only negative correlation") {
    var ms = 5
    val (testResult, _, _) = TimingTestUtils.executionTimeNotCorrelatedWithRuns(
      warmupRuns = 0,
      runs = 10,
      testPositiveCorr = false,
      testNegativeCorr = true) {
      Thread.sleep(ms)
      ms += 5
    }
    assertResult(true)(testResult)
  }
}

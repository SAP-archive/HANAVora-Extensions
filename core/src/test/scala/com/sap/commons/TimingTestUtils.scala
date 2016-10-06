package com.sap.commons

/**
  * Helpers to measure runtime behaviour of given actions.
  */
object TimingTestUtils {

  // Important: Do not change defaults if you are not absolutely sure
  //            what you are doing, since they specify the probability
  //            that a test will fail by random chance!
  val WarmupRuns = 2
  val Runs = 40
  val SignificantCorrelation = 0.9

  /**
    * Determines if the execution time of the given action is correlated with the number of
    * its executions. E.g., an action having the following execution times:
    *
    *   samples = (340, 180, 410, 310, .., 311, 199, 204, 466)
    *
    * does not obviously depend on it's execution numbers. However
    *
    *   samples = (340, 400, 399, 450, 499, .., 850, 910)
    *
    * obviously has increasing execution times. This indicates a bug in a routine that
    * is assumed to have constant runtime.
    *
    * This method allows to easily test such methods for correct runtime behaviour
    * by doing a Spearman's rank correlation test in the (idx, execution_time) samples,
    * where idx and execution_time are treated as random variables of a bi-variate distribution.
    *
    * NOTE: Be careful with using this method on low-level methods which are candidates to be
    * tuned by the JVM over time (e.g., by JIT compilation or caching). Use a large enough
    * warmup period in this case.
    *
    * @param rho The accepted correlation coefficient. The determined correlation `corr`
    *            needs to be `-rho < corr < rho`
    * @param warmupRuns The number of executions of the action execution before collecting samples
    * @param runs The number of runs to gather samples
    * @param testPositiveCorr Consider positive relationship in test result (default `true`)
    * @param testNegativeCorr Consider negative relationship in test result (default `false`)
    * @param action The action to execute
    * @return A tuple containing the test result (`true` if not correlated), the determined
    *         correlation, and the measured samples
    */
  def executionTimeNotCorrelatedWithRuns(rho: Double = SignificantCorrelation,
                                         warmupRuns: Int = WarmupRuns,
                                         runs: Int = Runs,
                                         testPositiveCorr: Boolean = true,
                                         testNegativeCorr: Boolean = false)
                                         (action: => Any): (Boolean, Double, Seq[(Int, Long)]) = {
    (1 to warmupRuns).foreach { i =>
      action
    }
    val samples = (1 to runs).map { i =>
      val dur = timedExecution(action)
      (i, dur)
    }
    val corr = DescriptiveStats.spearman(samples)
    val posTest = if (testPositiveCorr) corr < rho else true
    val negTest = if (testNegativeCorr) corr > -rho else true
    (posTest && negTest, corr, samples)
  }

  private[this] def timedExecution(action: => Any): Long = {
    val t0 = System.nanoTime()
    action
    System.nanoTime() - t0
  }
}

package com.sap.commons

/**
  * Utility module to compute simple descriptive statistics.
  */
object DescriptiveStats {

  /**
    * Computes the sample standard deviation:
    * {{{
    * Stdev(x) := 1/(n-1) sqrt(sum_i { pow(x_i - mean(x), 2) })
    * }}}
    *
    * @param samples Sample of numeric values
    * @tparam B Sample type (must support `Numeric`)
    * @return Sample standard deviation as [[Double]]
    */
  def stdev[B](samples: Seq[B])(implicit num: Numeric[B]): Double = {
    internalStdev(toDouble(samples))
  }

  /**
    * Computes the sample mean:
    * {{{
    * Mean(x) := 1/n sum_i { x_i }
    * }}}
    *
    * @param samples Sample of numeric values
    * @tparam B Sample type (must support `Numeric`)
    * @return Sample mean as [[Double]]
    */
  def mean[B](samples: Seq[B])(implicit num: Numeric[B]): Double =
    internalMean(toDouble(samples))

  /**
    * Computes the Pearson correlation coefficient between the random vectors `x` and `y`:
    * {{{
    * Pearson(x,y) := (1/(n-1) sum_i { (x_i - mean(x)) * (y_i - mean(y)) }) /
    *                 (stdev(x) * stdev(y))
    * }}}
    *
    * @param samples Sample of numeric value pairs
    * @tparam A Sample type of first attribute (must support `Numeric`)
    * @tparam B Sample type of second attribute (must support `Numeric`)
    * @return The Pearson correlation coefficient as [[Double]]
    */
  def pearson[A, B](samples: Seq[(A,B)])
                   (implicit num1: Numeric[A], num2: Numeric[B]): Double = {
    val doubleSamples = toDouble2(samples)
    val n = doubleSamples.size
    val a1 = doubleSamples.map(_._1)
    val a2 = doubleSamples.map(_._2)
    val a1mean = internalMean(a1)
    val a2mean = internalMean(a2)
    val a1stdev = internalStdev(a1)
    val a2stdev = internalStdev(a2)
    val nom = doubleSamples.map { case (_a1, _a2) => (_a1 - a1mean) * (_a2 - a2mean)}.sum / (n-1)
    val denom = a1stdev * a2stdev
    nom / denom
  }

  /**
    * Computes the Spearman's rank correlation coefficient between the random vectors `x` and `y`:
    *
    * {{{
    * rg[x] := index of the rank sorted random vector; e.g., rg[(0.5, 0.1, 1.2)] = (1,0,2)
    * Spearman(x, y) := Pearson(rg[x], rg[y])
    * }}}
    *
    * @param samples Sample of numeric value pairs
    * @tparam A Sample type of first attribute (must support `Numeric`)
    * @tparam B Sample type of second attribute (must support `Numeric`)
    * @return The Spearman correlation coefficient as [[Double]]
    */
  def spearman[A, B](samples: Seq[(A,B)])
                (implicit num1: Numeric[A], num2: Numeric[B]): Double = {
    val (x1, x2) = samples.unzip
    val x1Ranks = x1.zipWithIndex.sortBy(_._1).map(_._2)
    val x2Ranks = x2.zipWithIndex.sortBy(_._1).map(_._2)
    pearson(x1Ranks.zip(x2Ranks))
  }

  private def toDouble[B](samples: Seq[B])(implicit num: Numeric[B]): Seq[Double] =
    samples.map { r => num.toDouble(r) }

  private def toDouble2[A, B](samples: Seq[(A,B)])
                             (implicit num1: Numeric[A], num2: Numeric[B]): Seq[(Double, Double)] =
    samples.map { case(a1, a2) => (num1.toDouble(a1), num2.toDouble(a2)) }

  private def internalMean(samples: Seq[Double]): Double = samples.sum / samples.size

  private def internalStdev(samples: Seq[Double]): Double =
    samples.size match {
      case 0 => Double.NaN
      case n =>
        val mean = internalMean(samples)
        math.sqrt(samples.map{ r => math.pow(r - mean, 2) }.sum / (n-1))
    }
}

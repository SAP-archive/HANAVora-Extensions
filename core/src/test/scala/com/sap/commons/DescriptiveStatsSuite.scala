package com.sap.commons

import java.util.Random

import org.scalatest.FunSuite

// scalastyle:off magic.number
class DescriptiveStatsSuite extends FunSuite {
  val SignificantPosCorrelation = 0.9
  test("mean") {
    val samples0 = Seq(1, 1)
    val samples1 = Seq(1, 2, 3, 4)
    val samples2 = Seq(1.1, 0.9, 1.0)
    val samples3 = Seq.empty[Int]
    assertResult(1.0)(DescriptiveStats.mean(samples0))
    assertResult(2.5)(DescriptiveStats.mean(samples1))
    assertResult(1.0)(DescriptiveStats.mean(samples2))
    val samples3mean = DescriptiveStats.mean(samples3)
    assert(samples3mean.isNaN)
  }

  test("stdev") {
    val samples0 = Seq(0, 2)
    val samples1 = Seq.empty[Double]
    val samples2 = Seq.fill(1000)(0) ++ Seq.fill(1000)(2)
    assertResult(math.sqrt(2))(DescriptiveStats.stdev(samples0))
    assert(DescriptiveStats.stdev(samples1).isNaN)
    val samples2stdev = DescriptiveStats.stdev(samples2)
    assert(samples2stdev > 1.0 && samples2stdev < 1.001)
  }

  test("pearson") {
    val rand = new Random()
    val samples1 = Seq((1, 1), (2, 2), (3, 3))
    val samples2 = Seq((3.0, 1), (2.0, 2), (1.0, 3))
    val samples3 = (1 to 100000).map { i => (rand.nextDouble(), rand.nextDouble()) }
    val samples4 = Seq.empty[(Double, Double)]
    assertResult(1.0)(DescriptiveStats.pearson(samples1))
    assertResult(-1.0)(DescriptiveStats.pearson(samples2))
    assert(math.abs(DescriptiveStats.pearson(samples3)) < 0.01)
    assert(DescriptiveStats.pearson(samples4).isNaN)
  }

  test("spearman") {
    val rand = new Random()
    val samples1 = Seq((1, 1), (2, 2), (3, 3))
    val samples2 = Seq((3.0, 1), (2.0, 2), (1.0, 3))
    val samples3 = (1 to 100000).map { i => (rand.nextDouble(), rand.nextDouble()) }
    val samples4 = Seq.empty[(Double, Double)]
    assertResult(1.0)(DescriptiveStats.spearman(samples1))
    assertResult(-1.0)(DescriptiveStats.spearman(samples2))
    assert(math.abs(DescriptiveStats.spearman(samples3)) < 0.01)
    assert(DescriptiveStats.spearman(samples4).isNaN)
  }

  test("spearman & pearson w/ noise & outliers") {
    val samples1 = Seq((1, 300.0), (2, 250.0), (3, 400.0), (4, 350.0), (5, 500.0),
                       (6, 450.0), (7, 600.0), (8, 550.0), (9, 700.0), (10, 650.0))
    val samples2 = Seq((1, 300.0), (2, 350.0), (3, 400.0), (4, 450.0), (5, 500.0),
                       (6, 550.0), (7, 2000.0), (8, 700.0), (9, 750.0), (10, 800.0))
    assert(DescriptiveStats.pearson(samples1) > SignificantPosCorrelation)
    assert(DescriptiveStats.spearman(samples1) > SignificantPosCorrelation)
    // pearson is less robust, does not detect dependency
    assert(DescriptiveStats.pearson(samples2) < SignificantPosCorrelation)
    // spearman detects dependency
    assert(DescriptiveStats.spearman(samples2) > SignificantPosCorrelation)
  }

  test("spearman & pearson w/ real data") {

    val measure1 = Seq(379, 379, 382, 360, 378, 374, 364, 371, 360, 365, 364, 363, 369, 375, 365,
                       369, 358, 372, 370, 363, 363, 369, 361, 362, 367, 357, 365, 364, 363, 368,
                       360, 361, 360, 363, 359, 357, 365, 367, 364, 363)
    val measure2 = Seq(411, 379, 380, 382, 387, 404, 410, 431, 430, 444, 468, 489, 519, 573, 571,
                       620, 643, 657, 694, 711, 752, 783, 807, 841, 856, 891, 912, 962,1042, 982,
                       1076,1056,1092,1145,1128,1186,1221,1245,1284,1307)
    val samples1 = measure1.zipWithIndex
    val samples2 = measure2.zipWithIndex

    assert(DescriptiveStats.pearson(samples1) < SignificantPosCorrelation)
    assert(DescriptiveStats.spearman(samples1) < SignificantPosCorrelation)
    assert(DescriptiveStats.pearson(samples2) > SignificantPosCorrelation)
    assert(DescriptiveStats.spearman(samples2) > SignificantPosCorrelation)

  }
}

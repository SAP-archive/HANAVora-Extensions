package org.apache.spark.sql.catalyst.expressions

import org.apache.commons.math3.util.FastMath
import org.apache.spark.Logging
import org.apache.spark.sql.{GlobalSapSQLContext, GlobalSapSQLContext$}
import org.scalatest.FunSuite

class MathsSuite
  extends FunSuite
  with GlobalSapSQLContext
  with Logging {

  // scalastyle:off magic.number

  val rowA = DoubleRow("AAA", 1.0)
  val rowB = DoubleRow("BBB", 2.0)
  val rowC = DoubleRow("CCC", 0.6)
  val rowD = DoubleRow("DDD", -1.1)
  val rowE = DoubleRow("DDD", -1.1)

  val data = Seq(rowA, rowB)

  test("ln, log, pow") {
    val rdd = sc.parallelize(data)
    val dSrc = sqlContext.createDataFrame(rdd).cache()
    dSrc.registerTempTable("src")

    val result1 = sqlContext.sql("SELECT name,d,LN(d) FROM src").collect()

    assertResult(Row(rowA.name, rowA.d, 0.0) ::
      Row(rowB.name, rowB.d, FastMath.log(2.0)) :: Nil)(result1)

    val result2 = sqlContext.sql("SELECT name,d,LOG(d) FROM src").collect()

    assertResult(Row(rowA.name, rowA.d, 0.0) ::
      Row(rowB.name, rowB.d, FastMath.log10(2.0)) :: Nil)(result2)

    val result3 = sqlContext.sql("SELECT name,d,POWER(d,2) FROM src").collect()

    assertResult(Row(rowA.name, rowA.d, 1.0) ::
      Row(rowB.name, rowB.d, 4.0) :: Nil)(result3)
  }

  val data2 = Seq(rowC, rowD)

  test("ceil, floor, round, sign, mod") {
    val rdd = sc.parallelize(data2)
    val dSrc = sqlContext.createDataFrame(rdd).cache()
    dSrc.registerTempTable("src")

    val result1 = sqlContext.sql("SELECT name, d, CEIL(d), FLOOR(d)," +
      "ROUND(d,0), SIGN(d), MOD(d,3) FROM src").collect()

    assertResult(Row(rowC.name, rowC.d, 1.0, 0.0, 1.0, 1.0, 0.6) ::
      Row(rowD.name, rowD.d, -1.0, -2.0, -1.0, -1.0, -1.1) :: Nil)(result1)
  }

  test("cos, SIN, TAN, ACOS, ASIN, ATAN") {
    val rdd = sc.parallelize(data2)
    val dSrc = sqlContext.createDataFrame(rdd).cache()
    dSrc.registerTempTable("src")

    val result1 = sqlContext.sql("SELECT name, d, COS(d), SIN(d), TAN(d)," +
      " ACOS(COS(d)), ASIN(SIN(d)), ATAN(TAN(d)) FROM src").collect()

    assertResult(Row(rowC.name, rowC.d, FastMath.cos(rowC.d),
      FastMath.sin(rowC.d), FastMath.tan(rowC.d), 0.6, 0.6, 0.6) ::
      Row(rowD.name, rowD.d, FastMath.cos(rowD.d), FastMath.sin(rowD.d),
        FastMath.tan(rowD.d), 1.1, -1.1, -1.1) :: Nil)(result1)
  }
}

case class DoubleRow(name: String, d: Double)


package org.apache.spark.sql.function

import java.sql
import java.sql.Timestamp
import java.util.Calendar

import corp.sap.spark.SharedSparkContext
import org.apache.spark.Logging
import org.apache.spark.sql.{Row, VelocitySQLContext}
import org.scalatest.FunSuite

class ExtractSuite extends FunSuite with SharedSparkContext with Logging {

  // scalastyle:off magic.number

  val rowA = DateRow("AAA", getDate(21, 4, 1987))
  val rowB = DateRow("BBB", getDate(1, 9, 1987))
  val rowC = DateRow("CCC", getDate(5, 4, 2000))

  /* 24/6/1987 8:03:01 */
  val dtRowA = DateTimeRow("DDD", new Timestamp(551512981000L))
  /* 30/1/2000 2:01:00 */
  val dtRowB = DateTimeRow("EEE", new Timestamp(949194060000L))
  /* 31/12/2015 0:03:00 */
  val dtRowC = DateTimeRow("FFF", new Timestamp(1451516580000L))

  val dataWithDates = Seq(rowA, rowB, rowC)
  val dataWithDateTimes = Seq(dtRowA, dtRowB, dtRowC)


  test("DateTime extract in project") {
    val sqlContext = new VelocitySQLContext(sc)
    val rdd = sc.parallelize(dataWithDateTimes)
    val dSrc = sqlContext.createDataFrame(rdd).cache()
    dSrc.registerTempTable("src")

    val result1 = sqlContext.sql("SELECT name, d, EXTRACT(DAY FROM d) FROM src").collect

    assertResult(Row.apply(dtRowA.name, dtRowA.d, 24) ::
      Row.apply(dtRowB.name, dtRowB.d, 30) ::
      Row.apply(dtRowC.name, dtRowC.d, 31) :: Nil)(result1)

    val result2 = sqlContext.sql("SELECT name, d, EXTRACT(MONTH FROM d) FROM src").collect

    assertResult(Row.apply(dtRowA.name, dtRowA.d, 6) ::
      Row.apply(dtRowB.name, dtRowB.d, 1) ::
      Row.apply(dtRowC.name, dtRowC.d, 12) :: Nil)(result2)

    val result3 = sqlContext.sql("SELECT name, d, EXTRACT(YEAR FROM d) FROM src").collect

    assertResult(Row.apply(dtRowA.name, dtRowA.d, 1987) ::
      Row.apply(dtRowB.name, dtRowB.d, 2000) ::
      Row.apply(dtRowC.name, dtRowC.d, 2015) :: Nil)(result3)

    val result4 = sqlContext.sql("SELECT name, d, EXTRACT(HOUR FROM d) FROM src").collect

    assertResult(Row.apply(dtRowA.name, dtRowA.d, 8) ::
      Row.apply(dtRowB.name, dtRowB.d, 2) ::
      Row.apply(dtRowC.name, dtRowC.d, 0) :: Nil)(result4)

    val result5 = sqlContext.sql("SELECT name, d, EXTRACT(MINUTE FROM d) FROM src").collect

    assertResult(Row.apply(dtRowA.name, dtRowA.d, 3) ::
      Row.apply(dtRowB.name, dtRowB.d, 1) ::
      Row.apply(dtRowC.name, dtRowC.d, 3) :: Nil)(result5)

    val result6 = sqlContext.sql("SELECT name, d, EXTRACT(SECOND FROM d) FROM src").collect

    assertResult(Row.apply(dtRowA.name, dtRowA.d, 1) ::
      Row.apply(dtRowB.name, dtRowB.d, 0) ::
      Row.apply(dtRowC.name, dtRowC.d, 0) :: Nil)(result6)
  }

  test("Date extract in project") {
    val sqlContext = new VelocitySQLContext(sc)
    val rdd = sc.parallelize(dataWithDates)
    val dSrc = sqlContext.createDataFrame(rdd).cache()
    dSrc.registerTempTable("src")

    val result1 = sqlContext.sql("SELECT name, d, EXTRACT(DAY FROM d) FROM src").collect

    assertResult(Row.apply(rowA.name, rowA.d, 21) ::
      Row.apply(rowB.name, rowB.d, 1) ::
      Row.apply(rowC.name, rowC.d, 5) :: Nil)(result1)

    val result2 = sqlContext.sql("SELECT name, d, EXTRACT(MONTH FROM d) FROM src").collect

    assertResult(Row.apply(rowA.name, rowA.d, 4) ::
      Row.apply(rowB.name, rowB.d, 9) ::
      Row.apply(rowC.name, rowC.d, 4) :: Nil)(result2)

    val result3 = sqlContext.sql("SELECT name, d, EXTRACT(YEAR FROM d) FROM src").collect

    assertResult(Row.apply(rowA.name, rowA.d, 1987) ::
      Row.apply(rowB.name, rowB.d, 1987) ::
      Row.apply(rowC.name, rowC.d, 2000) :: Nil)(result3)
  }

  private def getDate(day: Int, month: Int, year: Int): sql.Date = {
    val calendar = Calendar.getInstance()
    calendar.clear()
    calendar.set(Calendar.DAY_OF_MONTH, day)
    calendar.set(Calendar.MONTH, month - 1)
    calendar.set(Calendar.YEAR, year)
    new sql.Date(calendar.getTimeInMillis)
  }
}

case class DateRow(name: String, d: sql.Date)

case class DateTimeRow(name: String, d: Timestamp)

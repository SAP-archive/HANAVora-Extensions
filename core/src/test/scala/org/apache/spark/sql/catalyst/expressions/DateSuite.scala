package org.apache.spark.sql.catalyst.expressions

import java.sql
import java.util.{Calendar, Locale, TimeZone}
import org.apache.spark.Logging
import org.apache.spark.sql.{Row, GlobalSapSQLContext}
import org.scalatest.FunSuite

class DateSuite
  extends FunSuite
  with GlobalSapSQLContext
  with Logging {

  // scalastyle:off magic.number

  val rowA = DateRow("AAA", getDate(21, 4, 1987))
  val rowB = DateRow("BBB", getDate(1, 9, 1987))
  val rowC = DateRow("CCC", getDate(5, 4, 2000))

  val dataWithDates = Seq(rowA, rowB, rowC)

  test("Date parts in project") {
    val rdd = sc.parallelize(dataWithDates)
    val dSrc = sqlContext.createDataFrame(rdd).cache()
    dSrc.registerTempTable("src")

    val result1 =
      sqlContext.sql("SELECT name, DAYOFMONTH(d) FROM src").collect()

    assertResult(Row(rowA.name, 21) ::
      Row(rowB.name, 1) ::
      Row(rowC.name, 5) :: Nil)(result1)

    val result2 =
      sqlContext.sql("SELECT name, MONTH(d) FROM src").collect()

    assertResult(Row.apply(rowA.name, 4) ::
      Row(rowB.name, 9) ::
      Row(rowC.name, 4) :: Nil)(result2)

    val result3 =
      sqlContext.sql("SELECT name, YEAR(d) FROM src").collect()

    assertResult(Row.apply(rowA.name, 1987) ::
      Row(rowB.name, 1987) ::
      Row(rowC.name, 2000) :: Nil)(result3)
  }

  test("Date add in project") {
    val rdd = sc.parallelize(dataWithDates)
    val dSrc = sqlContext.createDataFrame(rdd).cache()
    dSrc.registerTempTable("src")

    val result1 =
      sqlContext.sql("SELECT name, ADD_DAYS(d,1) FROM src").collect()

    assertResult(Row(rowA.name, getDate(22, 4, 1987)) ::
      Row(rowB.name, getDate(2, 9, 1987)) ::
      Row(rowC.name, getDate(6, 4, 2000)) :: Nil)(result1)

    val result2 =
      sqlContext.sql("SELECT name, ADD_MONTHS(d,-2) FROM src").collect()

    assertResult(Row(rowA.name, getDate(21, 2, 1987)) ::
      Row(rowB.name, getDate(1, 7, 1987)) ::
      Row(rowC.name, getDate(5, 2, 2000)) :: Nil)(result2)

    val result3 =
      sqlContext.sql("SELECT name, ADD_YEARS(d,3) FROM src").collect()

    assertResult(Row(rowA.name, getDate(21, 4, 1990)) ::
      Row(rowB.name, getDate(1, 9, 1990)) ::
      Row(rowC.name, getDate(5, 4, 2003)) :: Nil)(result3)

    val result4 =
      sqlContext.sql("SELECT name, DAYS_BETWEEN(d, ADD_MONTHS(d,1)) FROM src").collect()

    assertResult(Row(rowA.name, 30) ::
      Row(rowB.name, 30) ::
      Row(rowC.name, 30) :: Nil)(result4)

    val result5 =
      sqlContext.sql("SELECT name, DAYS_BETWEEN(CURRENT_DATE(), " +
        "ADD_DAYS(CURRENT_DATE(),1)) FROM src").collect()

    assertResult(Row(rowA.name, 1) ::
      Row(rowB.name, 1) ::
      Row(rowC.name, 1) :: Nil)(result5)

  }

  private def getDate(day: Int, month: Int, year: Int): sql.Date = {
    val cal =
      Calendar.getInstance(Locale.ENGLISH)
    cal.clear()
    cal.set(Calendar.DAY_OF_MONTH, day)
    cal.set(Calendar.MONTH, month - 1)
    cal.set(Calendar.YEAR, year)
    cal.set(Calendar.HOUR, 0)
    new sql.Date(cal.getTimeInMillis)
  }
}

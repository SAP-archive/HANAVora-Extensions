package org.apache.spark.sql.catalyst.expressions

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.catalyst.compat.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.compat._
import org.scalatest.FunSuite

//
// Backported from Spark 1.5.2.
//

// scalastyle:off

class BackportedDateExpressionsSuite extends FunSuite with ExpressionEvalHelper {


  val positiveIntLit = Literal(Short.MaxValue + 1)
  val negativeIntLit = Literal(Short.MinValue - 1)

  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val sdfDate = new SimpleDateFormat("yyyy-MM-dd")
  val d = new Date(sdf.parse("2015-04-08 13:10:15").getTime)
  val ts = new Timestamp(sdf.parse("2013-11-08 13:10:15").getTime)
  val emptyRow: InternalRow = EmptyRow /* XXX: Spark 1.4/1.5 compat quirk */

  test("datetime function current_date") {
    val d0 = DateTimeUtils.millisToDays(System.currentTimeMillis())
    val cd = CurrentDate().eval(emptyRow).asInstanceOf[Int]
    val d1 = DateTimeUtils.millisToDays(System.currentTimeMillis())
    assert(d0 <= cd && cd <= d1 && d1 - d0 <= 1)
  }

  test("datetime function current_timestamp") {
    val ct = DateTimeUtils.toJavaTimestamp(CurrentTimestamp().eval(emptyRow).asInstanceOf[Long])
    val t1 = System.currentTimeMillis()
    assert(math.abs(t1 - ct.getTime) < 5000)
  }

  test("DayOfYear") {
    val sdfDay = new SimpleDateFormat("D")
    (0 to 3).foreach { m =>
      (0 to 5).foreach { i =>
        val c = Calendar.getInstance()
        c.set(2000, m, 28, 0, 0, 0)
        c.add(Calendar.DATE, i)
        checkEvaluation(DayOfYear(Literal(new Date(c.getTimeInMillis))),
          sdfDay.format(c.getTime).toInt)
      }
    }
    checkEvaluation(DayOfYear(Literal.create(null, DateType)), null)
  }

  test("Year") {
    checkEvaluation(Year(Literal.create(null, DateType)), null)
    checkEvaluation(Year(Literal(d)), 2015)
    checkEvaluation(Year(Cast(Literal(sdfDate.format(d)), DateType)), 2015)
    checkEvaluation(Year(Cast(Literal(ts), DateType)), 2013)

    val c = Calendar.getInstance()
    (2000 to 2002).foreach { y =>
      (0 to 11 by 11).foreach { m =>
        c.set(y, m, 28)
        (0 to 5 * 24).foreach { i =>
          c.add(Calendar.HOUR_OF_DAY, 1)
          checkEvaluation(Year(Literal(new Date(c.getTimeInMillis))),
            c.get(Calendar.YEAR))
        }
      }
    }
  }

  test("Quarter") {
    checkEvaluation(Quarter(Literal.create(null, DateType)), null)
    checkEvaluation(Quarter(Literal(d)), 2)
    checkEvaluation(Quarter(Cast(Literal(sdfDate.format(d)), DateType)), 2)
    checkEvaluation(Quarter(Cast(Literal(ts), DateType)), 4)

    val c = Calendar.getInstance()
    (2003 to 2004).foreach { y =>
      (0 to 11 by 3).foreach { m =>
        c.set(y, m, 28, 0, 0, 0)
        (0 to 5 * 24).foreach { i =>
          c.add(Calendar.HOUR_OF_DAY, 1)
          checkEvaluation(Quarter(Literal(new Date(c.getTimeInMillis))),
            c.get(Calendar.MONTH) / 3 + 1)
        }
      }
    }
  }

  test("Month") {
    checkEvaluation(Month(Literal.create(null, DateType)), null)
    checkEvaluation(Month(Literal(d)), 4)
    checkEvaluation(Month(Cast(Literal(sdfDate.format(d)), DateType)), 4)
    checkEvaluation(Month(Cast(Literal(ts), DateType)), 11)

    (2003 to 2004).foreach { y =>
      (0 to 3).foreach { m =>
        (0 to 2 * 24).foreach { i =>
          val c = Calendar.getInstance()
          c.set(y, m, 28, 0, 0, 0)
          c.add(Calendar.HOUR_OF_DAY, i)
          checkEvaluation(Month(Literal(new Date(c.getTimeInMillis))),
            c.get(Calendar.MONTH) + 1)
        }
      }
    }
  }

  test("Day / DayOfMonth") {
    checkEvaluation(DayOfMonth(Cast(Literal("2000-02-29"), DateType)), 29)
    checkEvaluation(DayOfMonth(Literal.create(null, DateType)), null)
    checkEvaluation(DayOfMonth(Literal(d)), 8)
    checkEvaluation(DayOfMonth(Cast(Literal(sdfDate.format(d)), DateType)), 8)
    checkEvaluation(DayOfMonth(Cast(Literal(ts), DateType)), 8)

    (1999 to 2000).foreach { y =>
      val c = Calendar.getInstance()
      c.set(y, 0, 1, 0, 0, 0)
      (0 to 365).foreach { d =>
        c.add(Calendar.DATE, 1)
        checkEvaluation(DayOfMonth(Literal(new Date(c.getTimeInMillis))),
          c.get(Calendar.DAY_OF_MONTH))
      }
    }
  }

  test("Seconds") {
    checkEvaluation(Second(Literal.create(null, DateType)), null)
    checkEvaluation(Second(Cast(Literal(d), TimestampType)), 0)
    checkEvaluation(Second(Cast(Literal(sdf.format(d)), TimestampType)), 15)
    checkEvaluation(Second(Literal(ts)), 15)

    val c = Calendar.getInstance()
    (0 to 60 by 5).foreach { s =>
      c.set(2015, 18, 3, 3, 5, s)
      checkEvaluation(Second(Literal(new Timestamp(c.getTimeInMillis))),
        c.get(Calendar.SECOND))
    }
  }

  test("WeekOfYear") {
    checkEvaluation(WeekOfYear(Literal.create(null, DateType)), null)
    checkEvaluation(WeekOfYear(Literal(d)), 15)
    checkEvaluation(WeekOfYear(Cast(Literal(sdfDate.format(d)), DateType)), 15)
    checkEvaluation(WeekOfYear(Cast(Literal(ts), DateType)), 45)
    checkEvaluation(WeekOfYear(Cast(Literal("2011-05-06"), DateType)), 18)
  }

  /* XXX: REMOVED test("DateFormat") { */

  test("Hour") {
    checkEvaluation(Hour(Literal.create(null, DateType)), null)
    checkEvaluation(Hour(Cast(Literal(d), TimestampType)), 0)
    checkEvaluation(Hour(Cast(Literal(sdf.format(d)), TimestampType)), 13)
    checkEvaluation(Hour(Literal(ts)), 13)

    val c = Calendar.getInstance()
    (0 to 24).foreach { h =>
      (0 to 60 by 15).foreach { m =>
        (0 to 60 by 15).foreach { s =>
          c.set(2015, 18, 3, h, m, s)
          checkEvaluation(Hour(Literal(new Timestamp(c.getTimeInMillis))),
            c.get(Calendar.HOUR_OF_DAY))
        }
      }
    }
  }

  test("Minute") {
    checkEvaluation(Minute(Literal.create(null, DateType)), null)
    checkEvaluation(Minute(Cast(Literal(d), TimestampType)), 0)
    checkEvaluation(Minute(Cast(Literal(sdf.format(d)), TimestampType)), 10)
    checkEvaluation(Minute(Literal(ts)), 10)

    val c = Calendar.getInstance()
    (0 to 60 by 5).foreach { m =>
      (0 to 60 by 15).foreach { s =>
        c.set(2015, 18, 3, 3, m, s)
        checkEvaluation(Minute(Literal(new Timestamp(c.getTimeInMillis))),
          c.get(Calendar.MINUTE))
      }
    }
  }

  test("date_add") {
    checkEvaluation(
      DateAdd(Literal(Date.valueOf("2016-02-28")), Literal(1)),
      DateTimeUtils.fromJavaDate(Date.valueOf("2016-02-29")))
    checkEvaluation(
      DateAdd(Literal(Date.valueOf("2016-02-28")), Literal(-365)),
      DateTimeUtils.fromJavaDate(Date.valueOf("2015-02-28")))
    checkEvaluation(DateAdd(Literal.create(null, DateType), Literal(1)), null)
    checkEvaluation(DateAdd(Literal(Date.valueOf("2016-02-28")), Literal.create(null, IntegerType)),
      null)
    checkEvaluation(DateAdd(Literal.create(null, DateType), Literal.create(null, IntegerType)),
      null)
    checkEvaluation(
      DateAdd(Literal(Date.valueOf("2016-02-28")), positiveIntLit), 49627)
    checkEvaluation(
      DateAdd(Literal(Date.valueOf("2016-02-28")), negativeIntLit), -15910)
  }

  test("date_sub") {
    checkEvaluation(
      DateSub(Literal(Date.valueOf("2015-01-01")), Literal(1)),
      DateTimeUtils.fromJavaDate(Date.valueOf("2014-12-31")))
    checkEvaluation(
      DateSub(Literal(Date.valueOf("2015-01-01")), Literal(-1)),
      DateTimeUtils.fromJavaDate(Date.valueOf("2015-01-02")))
    checkEvaluation(DateSub(Literal.create(null, DateType), Literal(1)), null)
    checkEvaluation(DateSub(Literal(Date.valueOf("2016-02-28")), Literal.create(null, IntegerType)),
      null)
    checkEvaluation(DateSub(Literal.create(null, DateType), Literal.create(null, IntegerType)),
      null)
    checkEvaluation(
      DateSub(Literal(Date.valueOf("2016-02-28")), positiveIntLit), -15909)
    checkEvaluation(
      DateSub(Literal(Date.valueOf("2016-02-28")), negativeIntLit), 49628)
  }

  /* XXX: REMOVED test("time_add") { */

  /* XXX: REMOVED test("time_sub") { */

  test("add_months") {
    checkEvaluation(AddMonths(Literal(Date.valueOf("2015-01-30")), Literal(1)),
      DateTimeUtils.fromJavaDate(Date.valueOf("2015-02-28")))
    checkEvaluation(AddMonths(Literal(Date.valueOf("2016-03-30")), Literal(-1)),
      DateTimeUtils.fromJavaDate(Date.valueOf("2016-02-29")))
    checkEvaluation(
      AddMonths(Literal(Date.valueOf("2015-01-30")), Literal.create(null, IntegerType)),
      null)
    checkEvaluation(AddMonths(Literal.create(null, DateType), Literal(1)), null)
    checkEvaluation(AddMonths(Literal.create(null, DateType), Literal.create(null, IntegerType)),
      null)
    checkEvaluation(
      AddMonths(Literal(Date.valueOf("2015-01-30")), Literal(Int.MinValue)), -7293498)
    checkEvaluation(
      AddMonths(Literal(Date.valueOf("2016-02-28")), positiveIntLit), 1014213)
    checkEvaluation(
      AddMonths(Literal(Date.valueOf("2016-02-28")), negativeIntLit), -980528)
  }

  /* XXX: test("months_between") { */

  /* XXX: test("last_day") { */

  /* XXX: test("next_day") { */

  /* XXX: test("function to_date") { */

  test("function trunc") {
   def testTrunc(input: Date, fmt: String, expected: Date): Unit = {
     checkEvaluation(TruncDate(Literal.create(input, DateType), Literal.create(fmt, StringType)),
       expected)
   }
   val date = Date.valueOf("2015-07-22")
   Seq("yyyy", "YYYY", "year", "YEAR", "yy", "YY").foreach{ fmt =>
     testTrunc(date, fmt, Date.valueOf("2015-01-01"))
   }
   Seq("month", "MONTH", "mon", "MON", "mm", "MM").foreach { fmt =>
     testTrunc(date, fmt, Date.valueOf("2015-07-01"))
   }
   testTrunc(date, "DD", null)
   testTrunc(date, null, null)
   testTrunc(null, "MON", null)
   testTrunc(null, null, null)
  }

  /* XXX: test("from_unixtime") { */

  /* XXX: test("unix_timestamp") { */

  test("datediff") {
    checkEvaluation(
      DateDiff(Literal(Date.valueOf("2015-07-24")), Literal(Date.valueOf("2015-07-21"))), 3)
    checkEvaluation(
      DateDiff(Literal(Date.valueOf("2015-07-21")), Literal(Date.valueOf("2015-07-24"))), -3)
    checkEvaluation(DateDiff(Literal.create(null, DateType), Literal(Date.valueOf("2015-07-24"))),
      null)
    checkEvaluation(DateDiff(Literal(Date.valueOf("2015-07-24")), Literal.create(null, DateType)),
      null)
    checkEvaluation(
      DateDiff(Literal.create(null, DateType), Literal.create(null, DateType)),
      null)
  }

  /* XXX: test("to_utc_timestamp") { */

  /* XXX: test("from_utc_timestamp") { */

}

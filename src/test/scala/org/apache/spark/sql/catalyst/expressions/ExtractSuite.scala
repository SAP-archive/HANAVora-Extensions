package org.apache.spark.sql.catalyst.expressions

import java.sql
import java.sql.Timestamp
import java.util.{Calendar, Locale, TimeZone}

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.types.DateUtils
import org.scalatest.FunSuite

class ExtractSuite extends FunSuite with Logging {

  // scalastyle:off magic.number

  test("EXTRACT date") {
    val extract = Extract('a.string.at(0), 'a.date.at(1))
    val d =  getDate(21, 4, 1987)

    assertResult(21)(extract.eval(Row("DAY", d)))
    assertResult(4)(extract.eval(Row("MONTH",  d)))
    assertResult(1987)(extract.eval(Row("YEAR",  d)))
    assertResult(1987)(extract.eval(Row("YEAR",  DateUtils.fromJavaDate(d))))
    assertResult(null)(extract.eval(Row("YEAR",  null)))
    assertResult(null)(extract.eval(Row(null,  d)))
    assertResult(null)(extract.eval(Row(null,  null)))
    assertResult(0)(extract.eval(Row("HOUR", d)))
    assertResult(0)(extract.eval(Row("MINUTE", d)))
    assertResult(0)(extract.eval(Row("SECOND", d)))
  }

  test("EXTRACT timestamp") {
    val extract = Extract('a.string.at(0), 'a.timestamp.at(1))

    /* 24/6/1987 6:03:01 GMT */
    val t = new Timestamp(551512981000L)

    assertResult(24)(extract.eval(Row("DAY", t)))
    assertResult(6)(extract.eval(Row("MONTH",  t)))
    assertResult(1987)(extract.eval(Row("YEAR",  t)))
    assertResult(null)(extract.eval(Row("YEAR",  null)))
    assertResult(null)(extract.eval(Row(null,  t)))
    assertResult(null)(extract.eval(Row(null,  null)))
    assertResult(6)(extract.eval(Row("HOUR", t)))
    assertResult(3)(extract.eval(Row("MINUTE", t)))
    assertResult(1)(extract.eval(Row("SECOND", t)))
  }

  test("Prevent bad data types in EXTRACT") {
    intercept[RuntimeException](Extract('a.int.at(0), 'a.timestamp.at(1)))
    intercept[RuntimeException](Extract('a.string.at(0), 'a.float.at(1)))
    intercept[RuntimeException](Extract('a.int.at(0), 'a.float.at(1)))
  }

  private def getDate(day: Int, month: Int, year: Int): sql.Date = {
    val calendar = Calendar.getInstance(TimeZone.getTimeZone("Etc/UTC"), Locale.ENGLISH)
    calendar.clear()
    calendar.set(Calendar.DAY_OF_MONTH, day)
    calendar.set(Calendar.MONTH, month - 1)
    calendar.set(Calendar.YEAR, year)
    new sql.Date(calendar.getTimeInMillis)
  }
}

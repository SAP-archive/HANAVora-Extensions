package org.apache.spark.sql.sources.sql

import java.sql.Timestamp
import java.util.TimeZone

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.scalatest.FunSuite

class SqlBuilderTimestampSuite extends FunSuite {
  // scalastyle:off magic.number
  test("Timestamp correctly casted for 1.5") {
    val defaultTZ = TimeZone.getDefault
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    val timestampAsLong = 1420156800000L
    val tmpst = new Timestamp(timestampAsLong)
    val expectedResult = "TO_TIMESTAMP('2015-01-02 00:00:00.0000000')"

    val sqlBuilder = new SqlBuilder()

    val result = sqlBuilder.expressionToSql(Literal(tmpst))

    assert(expectedResult.equals(result))
    // reset the TZ
    TimeZone.setDefault(defaultTZ)
  }
  // scalastyle:on

}

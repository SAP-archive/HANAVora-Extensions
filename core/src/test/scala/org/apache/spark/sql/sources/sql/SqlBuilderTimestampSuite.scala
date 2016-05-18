package org.apache.spark.sql.sources.sql

import java.sql.Timestamp
import java.util.TimeZone

import org.apache.spark.sql.catalyst.expressions.{AddSeconds, Literal}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class SqlBuilderTimestampSuite extends FunSuite with BeforeAndAfterAll {

  val defaultTZ = TimeZone.getDefault

  override def beforeAll() {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
  }

  override def afterAll() {
    // reset the TZ
    TimeZone.setDefault(defaultTZ)
  }

  // scalastyle:off magic.number
  test("Timestamp correctly casted for 1.5") {
    val timestampAsLong = 1420156800000L
    val tmpst = new Timestamp(timestampAsLong)
    val expectedResult = "TO_TIMESTAMP('2015-01-02 00:00:00.0000000')"

    val sqlBuilder = new SqlBuilder()

    val result = sqlBuilder.expressionToSql(Literal(tmpst))

    assert(expectedResult.equals(result))
  }

  test("correctly build add_seconds SQL string") {
    val tmpst = new Timestamp(1420156800000L)
    val expectedResult = "ADD_SECONDS(TO_TIMESTAMP('2015-01-02 00:00:00.0000000'), 1)"

    val sqlBuilder = new SqlBuilder()

    val result = sqlBuilder.expressionToSql(AddSeconds(Literal(tmpst), Literal(1)))

    assert(expectedResult.equals(result))
  }

}

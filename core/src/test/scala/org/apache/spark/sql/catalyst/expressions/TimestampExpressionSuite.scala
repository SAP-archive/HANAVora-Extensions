package org.apache.spark.sql.catalyst.expressions

import java.sql.Timestamp

import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.{DateType, IntegerType}
import org.scalatest.FunSuite

class TimestampExpressionSuite extends FunSuite with ExpressionEvalHelper {

  test("add_seconds") {
    // scalastyle:off magic.number
    checkEvaluation(AddSeconds(Literal(Timestamp.valueOf("2015-01-01 00:11:33")), Literal(28)),
      DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf("2015-01-01 00:12:01")))
    checkEvaluation(AddSeconds(Literal(Timestamp.valueOf("2015-01-02 00:00:00")), Literal(-1)),
      DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf("2015-01-01 23:59:59")))
    checkEvaluation(AddSeconds(Literal(Timestamp.valueOf("2015-01-01 00:00:00")), Literal(-1)),
      DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf("2014-12-31 23:59:59")))
    checkEvaluation(AddSeconds(Literal(Timestamp.valueOf("2015-01-02 00:00:00")),
      Literal.create(null, IntegerType)), null)
    checkEvaluation(AddSeconds(Literal.create(null, DateType), Literal(1)), null)
    checkEvaluation(AddSeconds(Literal.create(null, DateType), Literal.create(null, IntegerType)),
      null)
  }
}

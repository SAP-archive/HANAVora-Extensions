package org.apache.spark.sql.catalyst.expressions

import java.sql.Timestamp

import org.apache.spark.sql.{GlobalSapSQLContext, Row}
import org.scalatest.FunSuite

class AddSecondsSuite
  extends FunSuite
  with GlobalSapSQLContext {

  val rowA = TimestampRow("AAA", Timestamp.valueOf("2015-01-01 12:12:04"))
  val rowB = TimestampRow("BBB", Timestamp.valueOf("2015-01-01 00:00:00"))
  val rowC = TimestampRow("CCC", Timestamp.valueOf("2015-12-31 23:59:58"))
  val rowD = TimestampRow("DDD", Timestamp.valueOf("2012-01-01 23:30:45"))

  val dataWithTimestamps = Seq(rowA, rowB, rowC, rowD)

  test("add_seconds") {
    val rdd = sc.parallelize(dataWithTimestamps)
    val dSrc = sqlContext.createDataFrame(rdd).cache()
    dSrc.registerTempTable("src")

    val result1 = sqlContext.sql("SELECT name, ADD_SECONDS(t, 5) FROM src").collect()
    assertResult(
      Row(rowA.name, Timestamp.valueOf("2015-01-01 12:12:09")) ::
      Row(rowB.name, Timestamp.valueOf("2015-01-01 00:00:05")) ::
      Row(rowC.name, Timestamp.valueOf("2016-01-01 00:00:03")) ::
      Row(rowD.name, Timestamp.valueOf("2012-01-01 23:30:50")) :: Nil)(result1)

    val result2 = sqlContext.sql("SELECT name, ADD_SECONDS(t, -5) FROM src").collect()
    assertResult(
      Row(rowA.name, Timestamp.valueOf("2015-01-01 12:11:59")) ::
      Row(rowB.name, Timestamp.valueOf("2014-12-31 23:59:55")) ::
      Row(rowC.name, Timestamp.valueOf("2015-12-31 23:59:53")) ::
      Row(rowD.name, Timestamp.valueOf("2012-01-01 23:30:40")) :: Nil)(result2)

    // example from SAP HANA documentation at
    // http://help.sap.com/hana/SAP_HANA_SQL_and_System_Views_Reference_en.pdf
    val result3 = sqlContext.sql("SELECT name, ADD_SECONDS(t, 60*30) FROM src").collect()
    assertResult(
      Row(rowA.name, Timestamp.valueOf("2015-01-01 12:42:04")) ::
      Row(rowB.name, Timestamp.valueOf("2015-01-01 00:30:00")) ::
      Row(rowC.name, Timestamp.valueOf("2016-01-01 00:29:58")) ::
      Row(rowD.name, Timestamp.valueOf("2012-01-02 00:00:45")) :: Nil)(result3)
  }

}

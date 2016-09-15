package org.apache.spark.sql

import com.sap.commons.TimingTestUtils
import com.sap.spark.util.TestUtils._
import org.apache.spark.util.DummyRelationUtils._
import org.apache.spark.sql.types.StructType
import org.scalatest.FunSuite


/**
  * This test case checks that operations with assumed runtime behaviour
  * are working as expected. Do not add test that check for absolute times, but
  * check the runtime complexity instead.
  *
  * These tests are a barrier for "cummulation bugs", e.g., in case that our parser or catalyst
  * collects objects created for each run.
  *
  */
class KPISmokeSuite
  extends FunSuite
  with GlobalSapSQLContext {

  val tableName = "tableTest"
  val sparkSchema = "name string, age int"
  val acceptedCorrelation = 0.9

  def createTestTable(): Unit = {
    sqlc.sql(
      s"""CREATE TEMPORARY TABLE $tableName ($sparkSchema)
          |USING com.sap.spark.dstest
          |OPTIONS ()""".stripMargin)
  }

  def dropTestTable(): Unit = sqlc.sql(s"""DROP TABLE $tableName""")

  /**
    * This test checks that queries of the same result are not getting slower over time.
    */
  test("Query times do not change with number of runs") {
    val sampleSize = 100
    val warmup = 10
    val numOfRows = 10
    val rows = (1 to numOfRows).map { i =>
      Row("a", i)
    }
    val rdd = sc.parallelize(rows)
    registerMockCatalystRelation(
      tableName = tableName,
      schema = StructType('a1.string :: 'a2.int :: Nil),
      data = rdd)

    def action(): Unit = {
      val res = sqlc.sql(s"SELECT * FROM $tableName").collect()
      assertResult(numOfRows)(res.size)

    }
    val (res, corr, samples) =
      TimingTestUtils.executionTimeNotCorrelatedWithRuns(
        acceptedCorrelation, warmup, sampleSize)(action)
    sqlc.dropTempTable(tableName)
    assert(res.booleanValue(), s"Correlation check failed. Correlation is $corr. " +
      s"Accepted correlation is $acceptedCorrelation. Determined samples: $samples")
  }

  /**
    * This test check that create/drop operations are not getting slower over time.
    */
  test("Create/drop does not change with number of runs") {
    val sampleSize = 40
    val warmup = 10
    def action(): Unit = {
      createTestTable()
      dropTestTable()
    }
    val (res, corr, samples) =
      TimingTestUtils.executionTimeNotCorrelatedWithRuns(
        acceptedCorrelation, warmup, sampleSize)(action)
    assert(res.booleanValue(), s"Correlation check failed. Correlation is $corr. " +
      s"Accepted correlation is $acceptedCorrelation. Determined samples: $samples")
  }
}

package org.apache.spark

import com.sap.spark.{GlobalSparkContext, WithSapHiveContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.SapHiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.FunSuite

/**
  * Suite to verify hive specific behaviour.
  */
class HiveSuite
  extends FunSuite
  with GlobalSparkContext
  with WithSapHiveContext {

  val schema = StructType(
    StructField("foo", StringType) ::
    StructField("bar", StringType) :: Nil)

  test("NewSession returns a new SapHiveContext") {
    val hiveContext = sqlc.asInstanceOf[SapHiveContext]
    val newHiveContext = hiveContext.newSession()

    assert(newHiveContext.isInstanceOf[SapHiveContext])
    assert(newHiveContext != hiveContext)
  }

  test("NewSession returns a hive context whose catalog is separated to the current one") {
    val newContext = sqlc.newSession()
    val emptyRdd = newContext.createDataFrame(sc.emptyRDD[Row], schema)
    emptyRdd.registerTempTable("foo")

    assert(!sqlc.tableNames().contains("foo"))
    assert(newContext.tableNames().contains("foo"))
  }
}
